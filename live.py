"""Vapor live loop: discovery -> warmup -> monitor -> paper trades, with
restart recovery from SQLite. Run: python live.py [--dry-run] [--once]
"""
import argparse
import sys
import time

import config
import db as dbmod
import engine
import gmgn

if hasattr(sys.stdout, "reconfigure"):  # Windows cp1252 can't print some symbol chars
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def rebuild_state(db, ts, gm):
    """Reconstruct TokenState objects from DB after a restart, preserving
    open trades, evaluations, and observed bars so nothing is double-traded."""
    states = {}
    for cand in db.get_candidates():
        addr = cand[0]
        st = engine.TokenState(addr, db, gm, watch_start_ms=(cand[4] or 0) * 1000.0)
        st.state = cand[8]
        st.bars = [b for b in db.observations(addr)]
        # rebuild open trades
        for t in db.open_trades(addr):
            cfg_id = t[2]
            st.open_trade_ids[cfg_id] = t[0]
            # last exit index = index of the OPENING bar of that trade
            ents = [b[0] for b in st.bars]
            try:
                st.last_exit_idx[cfg_id] = ents.index(t[3])  # entry_ts
            except ValueError:
                pass
        st.last_eval_idx = len(st.bars) - 1  # never re-evaluate old bars
        # mark expiry if stale and no open trades
        if st.state not in ("EXPIRED", "ERROR") and st.bars:
            last_ts = st.bars[-1][0] / 1000.0 if st.bars[-1][0] > 1e12 else st.bars[-1][0]
            if not st.open_trade_ids and time.time() - last_ts > config.STALE_EXPIRY_SECONDS:
                st.state = "EXPIRED"
        states[addr] = st
    return states


def status_line(db):
    n = db.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
    n_open = db.conn.execute("SELECT COUNT(*) FROM paper_trades WHERE state='OPEN'").fetchone()[0]
    n_closed = db.conn.execute("SELECT COUNT(*) FROM paper_trades WHERE state='CLOSED'").fetchone()[0]
    ne = db.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    return f"candidates={n} open={n_open} closed={n_closed} events={ne}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="discover + record candidates, do NOT create paper trades")
    ap.add_argument("--once", action="store_true", help="single discovery cycle, then exit")
    ap.add_argument("--cycles", type=int, default=0, help="run N monitor cycles then exit")
    args = ap.parse_args()

    db = dbmod.DB()
    gm = gmgn.GMGN()
    states = rebuild_state(db, None, gm)
    print("Vapor live |", status_line(db), "| restored", len(states), "token states")

    next_discovery = 0.0  # always refresh once at boot, then every DISCOVERY_SECONDS

    try:
        while True:
            t0 = time.time()
            # 1. resolve open trades + evaluate new bars for all monitored tokens
            for addr, st in list(states.items()):
                if st.state in ("EXPIRED", "ERROR"):
                    continue
                decision_ts = time.time()
                try:
                    new = st.refresh_history()
                    db.add_observations(addr, bars_since(st))
                    if not args.dry_run:
                        st.process_new_bars(decision_ts)
                    else:
                        st.last_eval_idx = len(st.bars) - 1
                except Exception as e:
                    db.event("error", addr, payload={"msg": str(e)})
                if st.state in ("DISCOVERED", "MONITORING", "POTENTIAL_SETUP",
                                "EVALUATED", "PAPER_TRADE", "EXITED"):
                    # keep current lifecycle state; only promote out of DISCOVERED once fed
                    if st.state == "DISCOVERED" and len(st.bars) >= 2:
                        st.mark("MONITORING")
            db.commit()

            # 2. periodic universe refresh on wall-clock cadence
            if time.time() >= next_discovery:
                states = refresh_universe(db, gm, states)
                db.commit()
                next_discovery = time.time() + config.DISCOVERY_SECONDS

            print("vapor |", status_line(db))
            db.commit()

            if args.once:
                break
            if args.cycles:
                args.cycles -= 1
                if args.cycles <= 0:
                    break
            # sleep until next poll (keep pacing loose; refresh_history paces request rate)
            time.sleep(max(1, config.POLL_SECONDS - (time.time() - t0)))
    except KeyboardInterrupt:
        print("\nstopped. final:", status_line(db))
    finally:
        db.commit()
        db.close()


def bars_since(st):
    """Return bars appended but not yet persisted (rows with market_ts > last saved)."""
    last = st.db.conn.execute(
        "SELECT MAX(market_ts) FROM observations WHERE token_address=?", (st.addr,)).fetchone()[0]
    last = last or 0
    return [b for b in st.bars if b[0] > last]


def refresh_universe(db, gm, states):
    """Fetch trending list; add new candidates (DISCOVERED); mark misses for
    watched tokens absent from the list. Returns updated states dict."""
    try:
        found = gm.trending()
        db.set_meta("trending_filter_id", str(gm.last_filter_id))
    except Exception as e:
        db.event("error", payload={"msg": f"trending: {e}"})
        return states
    seen = set()
    for t in found:
        addr = t["address"]
        seen.add(addr)
        db.upsert_candidate(addr, t["symbol"], t["name"], t["price"], t["liquidity"], t["meta"])
        if addr not in states:
            st = engine.TokenState(addr, db, gm, watch_start_ms=time.time() * 1000.0)
            st.mark("DISCOVERED")
            try:
                ok = st.refresh_history()
            except Exception:
                ok = False
            if ok and len(st.bars) >= 2:
                st.mark("MONITORING" if not st.open_trade_ids else "PAPER_TRADE")
            else:
                st.mark("ERROR")
            states[addr] = st
            db.add_observations(addr, bars_since(st))
            db.event("candidate_seen", addr, payload={"symbol": t["symbol"]})
            print(f"  + new candidate {t['symbol']} {addr[:8]}")
    # miss tracking for existing watches
    now = time.time()
    to_expire = []
    for addr, st in states.items():
        if addr not in seen and st.state not in ("EXPIRED",):
            db.bump_miss(addr)
            miss = db.conn.execute("SELECT misses FROM candidates WHERE token_address=?", (addr,)).fetchone()
            if miss and (miss[0] or 0) >= config.MAX_CONSECUTIVE_DISCOVERY_MISSES:
                if not st.bars or (time.time() - (st.bars[-1][0] if st.bars[-1][0] < 1e12 else st.bars[-1][0]/1000.0) > config.STALE_EXPIRY_SECONDS):
                    st.expire()
                    to_expire.append(addr)
                else:
                    db.conn.execute("UPDATE candidates SET misses=0 WHERE token_address=?", (addr,))
    db.set_meta("last_universe", str(len(found)))
    db.event("candidate_updated", payload={"n": len(found)})
    return states


if __name__ == "__main__":
    main()