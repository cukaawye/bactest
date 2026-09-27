"""CERTIFIED engine-parity replay: run the REAL engine.TokenState over the
recorded GMGN 24h pull. No mirrored strategy code here - every gate, barrier,
cooldown and fill comes from engine.py / config.py / strategy.py, the same
objects live.py drives.

Method (mirrors live.py's loop exactly):
  per token -> upsert_candidate (metadata snapshot) -> engine.TokenState ->
  for each bar in market order: ingest(bar) then process_new_bars(bar close),
  so the engine only ever sees CLOSED bars and never a future bar. At the end
  of the window st.expire() closes anything still open, exactly as live.py
  does when a candidate goes stale.

watch_start_ms = 0 ("watch-whole-24h"): every bar in the pull is eligible for
entries, i.e. we assume Vapor was watching from the first bar. The first
config.MIN_BARS_ENTER bars are still warm-up (engine enforces it).

Metadata modes (the pcp1h / mc entry gates read the candidate row, which live
snapshots ONCE at discovery - they do NOT refresh per bar). A replay has to
decide WHEN discovery happened, and getting it wrong injects lookahead:
  --snap disc  (default) : discovery at the first entry-eligible bar. No
                    lookahead - this is what the running bot can see. A 24h
                    watch-whole replay = "Vapor discovered all 35 tokens at
                    the first bar it had enough history on".
  --snap pull  : discovery at the time of the GMGN pull (end of window).
                    LOOKAHEAD: end-of-window metadata gating mid-window
                    entries. Shown only to quantify the bias.
  --recon           : per-bar reconstruction (pcp1h = close[i]/close[i-61],
                    mc scaled by close[i]/close[-1]) - the input the old
                    replay_gmgn.py mirror used. No lookahead, but gives the
                    gate a freshness the live bot never gets (it only
                    backfills meta keys that are missing). Re-running this
                    proves the engine reproduces the mirror trade-for-trade.

Usage: python parity_gmgn.py [--snap disc|pull] [--recon] [--db PATH]
"""
import argparse
import json
import os
import sqlite3
import sys
try:   # token symbols are arbitrary unicode; the windows console is cp1252
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    pass
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import db as dbmod
import engine

ALLOC_DOLLARS = config.ALLOC * config.CAP  # $25 per trade
HERE = os.path.dirname(os.path.abspath(__file__))

# mc / liquidity snapshot from the live trending pull (all 35 tokens).
META = {
    "6uPEdkU2xPg1iyfLzV1ssUwvve8nndA2sd6nBFrb1n58": (1017980.0, 297252.0),
    "CbcyNo7m1amFWqEQm2m4PLv1UNvpcL3C1Ujm6AkzpKoU": (3864070.0, 294111.0),
    "J1yxV53EmVRmt9RUj6PPufMfdAYUbtgwXCuNYTBTsYzJ": (395585.0, 74033.0),
    "FGZScHYURa8qMezk5gpZj5C2ExazybAwBp5TbNE4M3A8": (108028.0, 28474.6),
    "GW5xNXBtW9usH62Uimis4L5T1Fi5ceeMa9dupjTVpxaD": (22376.1, 10906.2),
    "FGoHcBuWfy6XGGkXwTLe9AuXAQEUmDR5g1LkxK98d1vJ": (38015.5, 19583.3),
    "8p6LETQaWnFGTeXmox66JHhemoj6Gvd5rjZjTXVBpump": (3857260.0, 179834.0),
    "45Bzv83gfhP6RPxX4AFNeB65NxKYba8vvzyb5Bqwpump": (1565190.0, 113399.0),
    "D9QYds4z8vSyxM4t6GvjeKFkkLLLWS5m3LdJEThxKyzB": (51166.4, 18476.0),
    "AuqagzhTqC6uBLraRb3rx2fwCCBAXCmfRm4btemyPXJb": (63031.0, 25283.5),
    "8HnLRKJ6XkeGq1tpmKPqn3yB5ruqbSR5LR5nVCNAshop": (665347.0, 76493.4),
    "DAemPFNc3RtibBDKkUA4eL2Ns11q9VRdbpptmqm8DGqN": (379875.0, 68990.4),
    "6UTgBWACzB1zX2GA2uEvVn6325XE99wNvkVTXv5qpump": (74683.1, 23501.4),
    "3jg5Zvqbg37afKLPGui2YevW1scuf5ra1Y8f3ejPDHiM": (12759.8, 11284.1),
    "39N6nzt3m1wcAGnSPXbmFadhbsLNrxbNqmZWNRTkEshm": (120556.0, 29302.2),
    "3w67HTHBF7Q92F9rCikgNsvYS4X7AyVUSGDpsLcMpump": (2357040.0, 142684.0),
    "9LbKkVVhrFRqAKCRDnSVAeWHgoBgNgej25J1ZHEPeURe": (14491.1, 11605.3),
    "4kNaq23LoL62kGiPZ2PSkNrpVWhvwAkfowpxyCNq9aBD": (45260.1, 20741.6),
    "3iUTyNYW6xKv5kZUjtbrEDTsuJTrSVwvB3bQtxkLpump": (223100.0, 46854.3),
    "HgoScXAo4zFv5kDb6axGrwuz95FaVEcLskyDVZfo297z": (228094.0, 43851.1),
    "F1z5FQdToNDLQYJ72grqePaWJrcrf39HR4gaKA57ySUQ": (83372.5, 23066.9),
    "7ukDd3E3PSupATuK4yvJ49jyZr8nNrkXKcq8pioiq98F": (31477.0, 16523.8),
    "9fno96crbJq887w3DuDFFaurYE7sHA8GrtZutzSa72j2": (39357.3, 14905.1),
    "HNazMWySREpLBoyEdsXPPzvvc4eZ6avQDwqBko3vpump": (32599.5, 13086.0),
    "6Rhs8iuUiepGYpiP6ZEhNF9jRemVV8xYdD3hVuqtKTAR": (26655.4, 20555.0),
    "jLz71QZfjnZZMLjUaCBw7KmUyftkkNq8NkWi2u9dyap": (1464500.0, 212392.0),
    "AuHt7PrmWVS9hsgpy967rKXWiMvc8ZFMKvRgm5W5BMRp": (45221.0, 16088.2),
    "39JRjoQBnxzMqGmieYf3Y22Hk58pUiTijmfyqt4Cpump": (111843.0, 27667.6),
    "7BN68PVzqWV6EjK8J8Y6Q1h5C8FFD5ZRZVscXyJDpump": (62669.8, 20316.5),
    "7nG5dJxuuUFXqxE4J8LecUZwP2m2HD6m83Xhae6jpzU1": (89596.5, 30094.2),
    "2G4k1G77cRAxogZk9AyjmufPqA4KDeCdtUQAXef3A5pT": (11927.7, 13743.9),
    "5wW9mhbwq1HTFh341iimpmrqBB4mfxdXiYhdYBL7hUnp": (141800.0, 39955.3),
    "F5zMFhfDtvRPhj71xPgnCWWResmWWuNSmS2AKgyz9KDk": (18995.5, 12741.0),
    "6hzyHMwR1S2wAbPfDjTcQtM5fTq4XZBWFTRSdTsvZ2DH": (124608.0, 30488.6),
    "64tUZQBCDKhkxPYNey9PXdV8Hz36ra28FuDhMMULpump": (18395.8, 17053.6),
}

TS = lambda ms: time.strftime("%m-%d %H:%M", time.gmtime(ms / 1000))


def recon_meta(bars, i, mc_now):
    """Per-bar reconstruction (the old mirror's input) for signal bar i."""
    close = [b[4] for b in bars]
    pcp1h = (close[i] / close[i - 60] - 1.0) * 100.0 if i >= 60 else None
    return {"pcp1h": pcp1h, "mc": mc_now * close[i] / close[-1]}


def snapshot_meta(bars, mc_now, when):
    """Static discovery snapshot taken at bar index `when` (None = last bar).
    pcp1h = that bar's own trailing-1h change, mc = the pulled mc carried
    forward/back on price (constant-supply approximation). Fails closed
    (pcp1h=None) when there is no 1h window at that bar."""
    close = [b[4] for b in bars]
    i = len(close) - 1 if when is None else when
    if i < 60:
        return {"pcp1h": None, "mc": mc_now * close[i] / close[-1]}
    return {"pcp1h": (close[i] / close[i - 60] - 1.0) * 100.0,
            "mc": mc_now * close[i] / close[-1]}


def run_token(db, addr, sym, bars, mc_now, liq, mode, meta=None, watch_start_ms=0, src=""):
    """Drive the real engine over one token's bars, one bar at a time.

    meta=None -> the GMGN-pull path (reconstructed / snapshot metadata, watch-whole).
    meta=dict -> a historical run's REAL discovery snapshot, with its true
    watch_start_ms. Nothing is reconstructed: ct, pcp1h, mc, hd, liquidity and
    first_seen_at are exactly what the live bot recorded.
    """
    if meta is None:
        disc = min(config.MIN_BARS_ENTER, len(bars)) - 1   # first entry-eligible bar
        meta = (recon_meta(bars, disc, mc_now) if mode == "recon"
                else snapshot_meta(bars, mc_now, None if mode == "pull" else disc))
    db.upsert_candidate(addr, sym, sym, bars[-1][4], liq, meta)
    st = engine.TokenState(addr, db, None, watch_start_ms=watch_start_ms)
    for i, b in enumerate(bars):
        st.ingest([b])
        if mode == "recon" and mc_now is not None:
            db.set_candidate_meta(addr, recon_meta(bars, i, mc_now))
        # decision time = the moment the bar closes (live never trades in-progress)
        st.process_new_bars((b[0] + 60000) / 1000.0)
    st.expire()   # window over: close anything still open, like live's stale expiry
    rows = db.conn.execute(
        "SELECT config_id, entry_ts, entry_price, ref_peak, drawdown, liquidity,"
        " lb, hold, state, exit_ts, exit_price, exit_reason, gross_ret, net_ret"
        " FROM paper_trades WHERE token_address=? ORDER BY entry_ts", (addr,)).fetchall()
    ents = [b[0] for b in bars]
    highs = [b[2] for b in bars]
    vols = [b[5] for b in bars]
    out = []
    for r in rows:
        (cid, ets, epx, ref, dd, liq_r, lb, hold, state, xts, _xpx, why, g, n) = r
        i_in = ents.index(ets)
        span = [b for b in bars if ets < b[0] <= xts] if xts else []
        # peak confirm = most recent bar whose high == ref (engine's own scan)
        age = None
        for j in range(i_in - 1, max(0, i_in - lb) - 1, -1):
            if highs[j] == ref:
                age = i_in - j
                break
        i = i_in
        # features: the recorded discovery snapshot when we have it (real runs),
        # else the price-derived reconstruction.
        snap = meta if mc_now is None else snapshot_meta(bars, mc_now, i_in)
        out.append({
            "src": src, "addr": addr, "in_ms": ets, "sym": sym, "cfg": cid, "lb": lb, "hold": hold,
            "in_t": TS(ets), "out_t": TS(xts) if xts else None,
            "entry": epx, "ref_peak": ref, "dd": dd, "why": why, "state": state,
            "sl_lvl": epx * (1 - 0.30), "rec_lvl": ref * (1 - 0.05),
            "gross": g, "net": n, "dollars": n * ALLOC_DOLLARS if n is not None else None,
            "mfe": max([b[2] / epx - 1.0 for b in span] or [0.0]),
            "mae": min([b[3] / epx - 1.0 for b in span] or [0.0]),
            "bars": (xts - ets) // 60000 if xts else None,
            "mc": snap.get("mc"), "pcp1h": snap.get("pcp1h"),
            "hd": snap.get("hd"), "peak_age": age,
            "age_h": snap.get("age_h"),
            "liq": liq_r, "vol": vols[i],
            "vol_ratio": vols[i] / max(1.0, sum(vols[max(0, i - 30):i]) / max(1, min(30, i))),
        })
    return out


def load_db_universe(paths, min_age_h=0.0, max_age_h=0.0):
    """Read a historical run: candidates + the bars the live bot actually polled.
    watch_start_ms and the metadata snapshot are the RECORDED ones, so this
    universe needs no reconstruction and no watch-whole assumption."""
    out = []
    for p in paths:
        conn = sqlite3.connect(p)
        for addr, sym, liq, first_seen, meta_json in conn.execute(
                "SELECT token_address, symbol, liquidity, first_seen_at, meta_json"
                " FROM candidates"):
            bars = [tuple(r) for r in conn.execute(
                "SELECT market_ts, open, high, low, close, volume FROM observations"
                " WHERE token_address=? ORDER BY market_ts", (addr,))]
            if not bars:
                continue
            try:
                meta = json.loads(meta_json or "{}")
            except (TypeError, ValueError):
                meta = {}
            age_h = None
            if meta.get("ct") and first_seen:
                age_h = (first_seen - float(meta["ct"])) / 3600.0
                meta["age_h"] = age_h
            if min_age_h and (age_h is None or age_h < min_age_h):
                continue
            if max_age_h and (age_h is None or age_h > max_age_h):
                continue
            out.append({"src": os.path.basename(p), "addr": addr,
                        "sym": sym or addr[:8], "bars": bars,
                        "liq": liq or 1000.0, "meta": meta,
                        "watch_start_ms": (first_seen or 0) * 1000.0,
                        "age_h": age_h})
        conn.close()
    return out



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snap", choices=("disc", "pull"), default="disc",
                    help="when the discovery metadata snapshot was taken "
                         "(disc=first eligible bar, no lookahead; pull=end of window)")
    ap.add_argument("--recon", action="store_true",
                    help="per-bar reconstructed pcp1h/mc (old mirror input) instead of "
                         "a static discovery snapshot")
    ap.add_argument("--db", default=None, help="keep the replay DB at this path")
    ap.add_argument("--configs", default=None,
                    help="comma-separated config ids, overrides config.ACTIVE_CONFIG_IDS")
    ap.add_argument("--gates", choices=("all", "none", "fresh", "pcp", "mc"), default="all",
                    help="which Vapor entry gates the real engine applies")
    ap.add_argument("--profile", action="store_true",
                    help="print the median feature profile split by winner vs loser")
    ap.add_argument("--dbs", default="",
                    help="comma-separated historical vapor.db paths to replay "
                         "instead of _gmgn_24h.json (uses each run's recorded "
                         "watch start + discovery metadata, nothing reconstructed)")
    ap.add_argument("--min-age-h", type=float, default=0.0,
                    help="skip candidates whose token age at discovery is under this")
    ap.add_argument("--max-age-h", type=float, default=0.0,
                    help="skip candidates whose token age at discovery is over this")
    args = ap.parse_args()
    if args.configs:
        config.ACTIVE_CONFIG_IDS = [int(x) for x in args.configs.split(",")]
    # gate ablation = flip the same config knobs the live engine reads
    config.FRESH_DIP_BARS = 30 if args.gates in ("all", "fresh") else 0
    config.MIN_PCP1H_PCT = 10 if args.gates in ("all", "pcp") else 0
    config.MIN_META_MC = 300000 if args.gates in ("all", "mc") else 0
    mode = "recon" if args.recon else args.snap

    if args.dbs:
        paths = [p.strip() for p in args.dbs.split(",") if p.strip()]
        missing = [p for p in paths if not os.path.exists(p)]
        if missing:
            sys.exit("missing database(s): " + "; ".join(missing))
        universe = load_db_universe(paths, args.min_age_h, args.max_age_h)
        bar_total = sum(len(u["bars"]) for u in universe)
        span = (TS(min(u["bars"][0][0] for u in universe)),
                TS(max(u["bars"][-1][0] for u in universe)))
    else:
        with open(os.path.join(HERE, "_gmgn_24h.json"), encoding="utf-8") as f:
            raw = json.load(f)["raw"]
        universe = None
        span = (TS(min(o["bars"][0][0] for o in raw.values() if o.get("bars"))),
                TS(max(o["bars"][-1][0] for o in raw.values() if o.get("bars"))))

    tmp = args.db or os.path.join(tempfile.gettempdir(), "vapor_parity.db")
    trades, skipped = [], []
    if universe is None:
        if os.path.exists(tmp):
            os.remove(tmp)
        db = dbmod.DB(tmp, run_id="parity")
        for addr, o in raw.items():
            if "err" in o or not o.get("bars"):
                continue
            if addr not in META:
                skipped.append(o["s"])
                continue
            mc_now, liq = META[addr]
            trades += run_token(db, addr, o["s"], o["bars"], mc_now, liq, mode)
        db.commit()
        dbs = {"_gmgn_24h.json": db}
    else:
        # One replay DB per source run. 65 token addresses appear in more than one
        # historical DB, and TokenState restores an open paper trade from the DB it
        # is given - a shared replay DB would carry a trade from run 1 into run 2 and
        # close it on bars that run never saw.
        dbs = {}
        by_src = {}
        for u in universe:
            by_src.setdefault(u["src"], []).append(u)
        for src, us in by_src.items():
            safe = "".join(ch if ch.isalnum() else "_" for ch in src)
            p = (tmp if len(by_src) == 1
                 else os.path.splitext(tmp)[0] + "_" + safe + ".db")
            if os.path.exists(p):
                os.remove(p)
            d = dbmod.DB(p, run_id="parity")
            for u in us:
                trades += run_token(d, u["addr"], u["sym"], u["bars"], None, u["liq"],
                                    mode, meta=u["meta"], watch_start_ms=u["watch_start_ms"],
                                    src=src)
            d.commit()
            dbs[src] = d
        db = list(dbs.values())[0]

    reasons = {}
    for d in dbs.values():
        for k, v in d.conn.execute(
                "SELECT reason, COUNT(*) FROM evaluations GROUP BY reason").fetchall():
            reasons[k] = reasons.get(k, 0) + v
    n_tok = sum(d.conn.execute(
        "SELECT COUNT(*) FROM candidates WHERE state IS NOT NULL").fetchone()[0]
        for d in dbs.values())

    label = {"recon": "recon (per-bar meta, old mirror input)",
             "disc": "live-faithful (snapshot at discovery bar)",
             "pull": "LOOKAHEAD (snapshot at pull time, do not trust)"}[mode]
    if universe is not None:
        label = ("historical runs: RECORDED watch start + discovery metadata "
                 "(no reconstruction)")
    cfgs = config.ACTIVE_CONFIG_IDS
    gname = {"all": "fresh30 + pcp10% + mc300k (LIVE)", "none": "no gates",
             "fresh": "fresh30 ONLY", "pcp": "pcp10% only", "mc": "mc300k only"}[args.gates]
    if args.min_age_h or args.max_age_h:
        gname += (f"  + token age {args.min_age_h:g}h..{args.max_age_h:g}h"
                  f" (applied to the candidate universe)")
    print("=" * 78)
    print(f"CERTIFIED REPLAY  engine.TokenState (real code) | configs {cfgs}")
    print(f"gates: {gname}")
    if args.gates in ("fresh", "none"):
        print(f"metadata: {label}  (gates off -> pcp/mc are FEATURES here, not filters)")
    else:
        print(f"metadata: {label}")
    print(f"universe={n_tok} tokens  bars={bar_total}  window={span[0]} -> {span[1]} UTC")
    print("live engine config in use: " + ", ".join(
        f"cfg{c['id']}(lb{c['lb']},dip{c['dip']},sl{c['sl']},hold{effective_hold(c)})"
        for c in config.FROZEN_CONFIGS if c["id"] in cfgs))
    print()

    # de-dupe: the same address in two source runs is two independent trade sets,
    # but within one run a (token, config, entry) can only have happened once.
    _seen = set()
    _uniq = []
    for t in trades:
        k = (t["src"], t["addr"], t["cfg"], t["in_ms"])
        if k not in _seen:
            _seen.add(k)
            _uniq.append(t)
    trades = _uniq

    # ---- totals straight off the replay DB(s) ----
    def tally(d):
        a = {r[0]: r[1] for r in d.conn.execute(
            "SELECT exit_reason, COUNT(*) FROM paper_trades GROUP BY exit_reason").fetchall()}
        nn = sum(a.values())
        s = d.conn.execute("SELECT COALESCE(SUM(net_ret),0) FROM paper_trades").fetchone()[0]
        o = d.conn.execute("SELECT COUNT(*) FROM paper_trades WHERE state='OPEN'").fetchone()[0]
        w = a.get("rec", 0) + a.get("tp", 0)
        return a, nn, s, o, w

    agg, n, net, st_open, wins = tally(db)
    if len(dbs) > 1:
        # a token address present in two historical runs produces two independent
        # trade sets; each replay DB is isolated, so the combined row is their sum
        agg, n, net, st_open, wins = {}, 0, 0.0, 0, 0
        for d in dbs.values():
            a, nn, s, o, w = tally(d)
            n += nn
            net += s
            st_open += o
            wins += w
            for k, v in a.items():
                agg[k] = agg.get(k, 0) + v
        print(f"{'source run':<22}{'tok':>5}{'trades':>8}{'rec':>5}{'sl':>5}{'tp':>4}"
              f"{'end':>5}{'WR':>8}{'net $':>10}")
        for src, d in dbs.items():
            a, nn, s, o, w = tally(d)
            tok = d.conn.execute(
                "SELECT COUNT(*) FROM candidates WHERE state IS NOT NULL").fetchone()[0]
            print(f"{src:<22}{tok:>5}{nn:>8}{a.get('rec',0):>5}{a.get('sl',0):>5}"
                  f"{a.get('tp',0):>4}{a.get('end',0):>5}"
                  f"{(w / nn * 100 if nn else 0):>7.1f}%{s * ALLOC_DOLLARS:>10.2f}")
        print(f"{'ALL 3 RUNS':<22}{n_tok:>5}{n:>8}{agg.get('rec',0):>5}{agg.get('sl',0):>5}"
              f"{agg.get('tp',0):>4}{agg.get('end',0):>5}{wins / n * 100:>7.1f}%"
              f"{net * ALLOC_DOLLARS:>10.2f}")
        print()
    print(f"TOTAL  trades={n}  rec={agg.get('rec',0)}  sl={agg.get('sl',0)}  "
          f"tp={agg.get('tp',0)}  end={agg.get('end',0)}  still_open={st_open}")
    print(f"       WR={wins/n if n else 0:.1%}   net=${net*ALLOC_DOLLARS:+.2f} on "
          f"${n*ALLOC_DOLLARS:.0f} deployed   avg=${(net*ALLOC_DOLLARS/n if n else 0):+.2f}/trade")
    print()
    print("gate decisions recorded by the engine (evaluations table):")
    for k, v in reasons.items():
        print(f"  {k:14} {v}")
    print()

    print()
    if len(cfgs) > 1:
        print("--- per config (fresh30 does NOT make lb180 == lb240; the younger "
              "lb180 peak can pass where the older lb240 peak is stale) ---")
        print(f"  {'cfg':6}{'n':>4}{'WR':>7}{'rec':>5}{'sl':>4}{'tp':>4}{'end':>5}{'net$':>9}")
        for cf in cfgs:
            g = [t for t in trades if t["cfg"] == cf]
            if not g:
                continue
            c = {k: sum(1 for t in g if t["why"] == k) for k in ("rec", "sl", "tp", "end")}
            wr = (c["rec"] + c["tp"]) / len(g)
            print(f"  cfg{cf:<3}{len(g):>4}{wr:>7.1%}{c['rec']:>5}{c['sl']:>4}{c['tp']:>4}"
                  f"{c['end']:>5}{sum(t['dollars'] or 0 for t in g):>9.2f}")
        print()

    if args.profile:
        from statistics import median
        win = [t for t in trades if t["why"] in ("rec", "tp")]
        los = [t for t in trades if t["why"] == "sl"]
        end = [t for t in trades if t["why"] == "end"]
        feats = [
            ("drawdown %", "dd", 100, 1),
            ("peak age (bars)", "peak_age", 1, 0),
            ("pcp1h %", "pcp1h", 1, 0),
            ("mc $k", "mc", 1e-3, 0),
            ("holders", "hd", 1, 0),
            ("token age h", "age_h", 1, 0),
            ("liquidity $k", "liq", 1e-3, 0),
            ("entry vol $", "vol", 1, 0),
            ("vol x 30bar avg", "vol_ratio", 1, 2),
            ("MFE %", "mfe", 100, 1),
            ("MAE %", "mae", 100, 1),
            ("bars held", "bars", 1, 0),
        ]

        def med(group, key, scale):
            vals = [t[key] * scale for t in group if t.get(key) is not None]
            return median(vals) if vals else None

        print("--- MEDIAN feature profile (entry-bar values, causal) ---")
        hdr = f"  {'':20}{'all':>11}{'rec+tp':>11}{'sl':>11}{'end':>11}"
        print(hdr)
        print(f"  {'':20}{len(trades):>11}{len(win):>11}{len(los):>11}{len(end):>11}   n")
        for name, key, scale, dp in feats:
            cells = [med(g, key, scale) for g in (trades, win, los, end)]
            print(f"  {name:20}" + "".join(
                "           -" if c is None else f"{c:>11,.{dp}f}" for c in cells))
        print(f"  {'':20}" + "".join(
            f"{sum(t['dollars'] or 0 for t in g):>11.2f}" for g in (trades, win, los, end))
            + "   net$")
        print()

    # ---- per token ----
    print("--- per token ---")
    bysym = {}
    for t in trades:
        bysym.setdefault(t["sym"], []).append(t)
    print(f"{'sym':12}{'n':>3}{'net$':>9}   detail")
    for sym in sorted(bysym, key=lambda s: -sum(t["dollars"] or 0 for t in bysym[s])):
        ts_ = bysym[sym]
        tn = sum(t["dollars"] or 0 for t in ts_)
        detail = "; ".join(
            f"{t['in_t'][6:]}->{t['out_t'][6:] if t['out_t'] else 'OPEN'} {t['why']} "
            f"g{t['gross']*100:+.1f}% n{t['net']*100:+.1f}%" for t in ts_)
        print(f"{sym:12}{len(ts_):>3}{tn:>+9.2f}   {detail}")
    print()

    # ---- anatomy ----
    print("--- anatomy by outcome (means) ---")
    groups = {"rec": [t for t in trades if t["why"] == "rec"],
              "sl": [t for t in trades if t["why"] == "sl"],
              "end": [t for t in trades if t["why"] == "end"]}
    print(f"  {'':6}{'n':>3}{'dd%':>7}{'MFE%':>8}{'MAE%':>8}{'bars':>6}{'net$':>9}")
    for name, g in groups.items():
        if not g:
            continue
        m = lambda k: sum(t[k] for t in g) / len(g)
        print(f"  {name:6}{len(g):>3}{m('dd')*100:>7.0f}{m('mfe')*100:>8.1f}"
              f"{m('mae')*100:>8.1f}{m('bars') or 0:>6.0f}"
              f"{sum(t['dollars'] or 0 for t in g):>9.2f}")
    print()

    # ---- every trade, full path ----
    print("--- every certified trade ---")
    print(f"{'sym':10}{'c':>3}{'in':>12}{'out':>12} {'why':4}{'dd%':>4}{'pcp%':>6}"
          f"{'mc_k':>7}{'entry$':>11}{'MFE%':>7}{'MAE%':>7}{'gross%':>8}{'net%':>7}{'$':>7}")
    for t in sorted(trades, key=lambda x: (x["in_t"], x["sym"])):
        print(f"{t['sym']:10}{t['cfg']:>3}{t['in_t']:>12}{(t['out_t'] or '-'):>12} "
              f"{t['why']:4}{t['dd']*100:>4.0f}{(t['pcp1h'] or 0):>+6.0f}{t['mc']/1000:>7.0f}"
              f"{t['entry']:>11.4g}{t['mfe']*100:>7.1f}{t['mae']*100:>7.1f}"
              f"{t['gross']*100:>+8.1f}{t['net']*100:>+7.1f}{t['dollars']:>+7.2f}")
    if skipped:
        print(f"\n(no metadata snapshot, skipped: {', '.join(skipped)})")
    print(f"\nreplay DB: {tmp}")


def effective_hold(c):
    return config.COMBO_HOLD_MIN or c["hold"]


if __name__ == "__main__":
    main()
