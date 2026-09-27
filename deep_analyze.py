"""Deep analysis of a vapor run DB: independent dip-episodes, churn, exit
anatomy, drawdown/entry-depth vs outcome, hold-time, and metadata at entry.

Usage: python deep_analyze.py [path-to-vapor.db]
Read-only. Prints a structured report.
"""
import json
import sqlite3
import sys
from datetime import datetime, timezone

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DB = sys.argv[1] if len(sys.argv) > 1 else "vapor.db"


def ts_min(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M")


def main():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    cfg = {
        r["id"]: r
        for r in c.execute("SELECT id, lb, dip, tp, sl, hold FROM configs")
    } if c.execute(
        "SELECT 1 FROM sqlite_master WHERE name='configs'"
    ).fetchone() else None

    sym = {r["token_address"]: r["symbol"] for r in c.execute("SELECT token_address, symbol FROM candidates")}
    meta = {r["token_address"]: json.loads(r["meta_json"] or "{}")
            for r in c.execute("SELECT token_address, meta_json FROM candidates")}
    trades = [dict(r) for r in c.execute(
        "SELECT * FROM paper_trades ORDER BY entry_ts")]

    n_closed = sum(1 for t in trades if t["state"] == "CLOSED")
    print(f"db={DB}")
    print(f"trades={len(trades)} closed={n_closed} "
          f"open={sum(1 for t in trades if t['state'] != 'CLOSED')}")
    t0 = min(t["entry_ts"] for t in trades)
    t1 = max((t["exit_ts"] for t in trades if t["exit_ts"]), default=t0)
    print(f"window: {ts_min(t0)}UTC -> {ts_min(t1)}UTC "
          f"({(t1 - t0) / 60000:.1f} min)")
    print()

    # ---- 1. group into independent dip-episodes ---------------------------
    by_token = {}
    for t in trades:
        by_token.setdefault(t["token_address"], []).append(t)
    eps = []
    for addr, ts_ in by_token.items():
        ts_.sort(key=lambda t: t["entry_ts"])
        cur = [ts_[0]]
        for t in ts_[1:]:
            if t["entry_ts"] - cur[0]["entry_ts"] < 10 * 60 * 1000:
                cur.append(t)
            else:
                eps.append(cur)
                cur = [t]
        eps.append(cur)
    eps = [e for e in eps if any(t["state"] == "CLOSED" for t in e)]

    print("== 1. INDEPENDENT DIP-EPISODES (same token, entry within 10 min) ==")
    print(f"episodes={len(eps)}  avg configs/episode={sum(len(e) for e in eps)/len(eps):.1f}")
    rows = []
    for e in eps:
        closed = [t for t in e if t["state"] == "CLOSED"]
        net = sum(t["net_ret"] for t in closed)
        wins = sum(1 for t in closed if t["net_ret"] > 0)
        why = {}
        for t in closed:
            why[t["exit_reason"]] = why.get(t["exit_reason"], 0) + 1
        m = meta.get(e[0]["token_address"], {})
        rows.append({
            "sym": sym.get(e[0]["token_address"], "?"),
            "dip_ts": e[0]["entry_ts"],
            "n": len(closed),
            "net": net,
            "w": wins,
            "why": why,
            "mc": m.get("mc", 0),
            "pcp1h": m.get("pcp1h"),
            "mclk": m.get("mc"),
        })
    for r in sorted(rows, key=lambda r: (r["sym"], r["dip_ts"])):
        print(f"  {r['sym']:>10} {ts_min(r['dip_ts'])}UTC n={r['n']:>1} "
              f"net={r['net']:+.2f} w={r['w']}/{r['n']} {r['why']} "
              f"mc={r['mclk']:>10,.0f} pcp1h={r['pcp1h'] if r['pcp1h'] is not None else 0:>6.1f}")
    print(f"  -> episodes WINNING (net>0): {sum(1 for r in rows if r['net']>0)} / {len(rows)}")
    print(f"  -> sum net by episode (losers+winners): {sum(r['net'] for r in rows):+.2f}")
    print()

    # ---- 2. re-entry churn: does the same token farm more losses? ---------
    print("== 2. RE-ENTRY CHURN (per-token episode sequence) ==")
    by_sym = {}
    for e in eps:
        by_sym.setdefault((sym.get(e[0]['token_address'], '?'), e[0]['token_address']), []).append(e)
    for (sym_, addr), ts_ in sorted(by_sym.items()):
        _ = addr
        nets = [sum(t["net_ret"] for t in e if t["state"] == "CLOSED") for e in ts_]
        if len(ts_) > 1:
            print(f"  {sym_:>10} episodes={len(ts_)} nets={[f'{n:+.2f}' for n in nets]} "
                  f"cum={sum(nets):+.2f}")

    # ---- 3. exit anatomy: hold time + drawdown vs exit ---------------------
    print()
    print("== 3. EXIT ANATOMY ==")
    for why in ("sl", "rec", "tp", "end"):
        sel = [t for t in trades if t["state"] == "CLOSED" and t["exit_reason"] == why]
        if not sel:
            continue
        holds = [(t["exit_ts"] - t["entry_ts"]) / 60000 for t in sel]
        dds = [t["drawdown"] for t in sel]
        print(f"  {why:>4}: n={len(sel):>3} net={sum(t['net_ret'] for t in sel):+.2f} "
              f"hold median={sorted(holds)[len(holds)//2]:.0f}m "
              f"(avg {sum(holds)/len(holds):.0f}m) "
              f"entry_dd avg={sum(dds)/len(dds):.3f} "
              f"hold<10m={sum(1 for h in holds if h<10)}")

    # ---- 4. entry drawdown depth vs outcome --------------------------------
    print()
    print("== 4. ENTRY DEPTH (drawdown of entry close vs ref peak) vs OUTCOME ==")
    for lo, hi in [(0, 0.27), (0.27, 0.31), (0.31, 0.36), (0.36, 0.55), (0.55, 1.01)]:
        sel = [t for t in trades if t["state"] == "CLOSED" and lo <= t["drawdown"] < hi]
        if not sel:
            continue
        w = sum(1 for t in sel if t["net_ret"] > 0)
        l = sum(1 for t in sel if t["net_ret"] <= 0)
        print(f"  dd {lo:.2f}-{hi:.2f}: n={len(sel):>3} wr={w/(w+l):.2f} "
              f"net={sum(t['net_ret'] for t in sel):+.2f} "
              f"avg={sum(t['net_ret'] for t in sel)/len(sel):+.3f}")

    # ---- 5. hold-time bucket vs outcome ------------------------------------
    print()
    print("== 5. HOLD TIME vs OUTCOME ==")
    for lo, hi in [(0, 10), (10, 40), (40, 120), (120, 240), (240, 1000)]:
        sel = [t for t in trades if t["state"] == "CLOSED"]
        sel = [t for t in sel if lo <= (t["exit_ts"] - t["entry_ts"]) / 60000 < hi]
        if not sel:
            continue
        w = sum(1 for t in sel if t["net_ret"] > 0)
        print(f"  hold {lo:>3}-{hi:>3}m: n={len(sel):>3} net={sum(t['net_ret'] for t in sel):+.2f} "
              f"w={w}/{len(sel)}")

    # ---- 6. metadata at entry ----------------------------------------------
    print()
    print("== 6. METADATA (FINAL snapshot - NOTE: post-hoc for early trades) ==")
    def seat(tok):
        return c.execute("SELECT current_price, liquidity FROM candidates WHERE token_address=?", (tok,)).fetchone()
    grouped = {}
    for t in trades:
        if t["state"] != "CLOSED":
            continue
        grouped.setdefault(t["token_address"], []).append(t)
    for tok, ts_ in sorted(grouped.items(), key=lambda kv: -abs(sum(x["net_ret"] for x in kv[1]))):
        m = meta.get(tok, {})
        net = sum(t["net_ret"] for t in ts_)
        print(f"  {sym.get(tok,'?'):>10} net={net:+.2f} n={len(ts_):>2} "
              f"mc={m.get('mc',0):>10,.0f} hhmc={m.get('hhmc',0):>10,.0f} "
              f"lq={m.get('lq',0):>10,.0f} pcp1h={m.get('pcp1h',0):>7.1f} "
              f"pcp5m={m.get('pcp5m',0):>7.1f} holders={m.get('hd',0):>6} "
              f"rug={m.get('rug',0)}")

    c.close()


if __name__ == "__main__":
    main()