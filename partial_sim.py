"""partial_sim.py — experiment: what-if exit policies on RECORDED trades.

For every CLOSED paper trade in a DB, re-simulate the post-entry OHLC series
under alternative exit policies and compare net P&L:
  - bank100: sell everything at the REC level (0.95*ref_peak) as soon as hit.
  - bankF+runner (F=0.85/0.90): bank F at REC, let the rest run with SL moved to
    entry (breakeven) until TP (+100%) or the bar series ends.
  - baseline: the engine's own SL->rec->TP scan (gap-aware fills, GAP_FILL=True).

Precisely reproduces engine._my_exit ordering and fill model so the policies
differ ONLY in the REC-fraction behavior. net = gross - cost (same strategy
cost model, per-trade independent as the engine does).
"""
import sqlite3
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import strategy

DBS = [
    r"C:\Users\hp\Downloads\Telegram Desktop\vapor.db (2).new",
    r"C:\Users\hp\Downloads\Telegram Desktop\vapor.db.new",
    r"C:\Users\hp\Downloads\Telegram Desktop\vapor.db",
]
REC = strategy.REC  # 0.05 -> rec level 0.95 * ref_peak


def load(db_path):
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    trades = [dict(t) for t in c.execute(
        "SELECT * FROM paper_trades WHERE state='CLOSED'")]
    obs = {}
    for addr in set(t["token_address"] for t in trades):
        obs[addr] = [tuple(r) for r in c.execute(
            "SELECT market_ts,open,high,low,close,volume FROM observations "
            "WHERE token_address=? ORDER BY market_ts", (addr,))]
    c.close()
    return trades, obs


def banked_scan(idx, series, entry_price, ref_peak, sl, tp, hold, bank_frac):
    """bank_frac in (0,1): bank that fraction at REC, runner (remainder) continues
    with SL=->breakeven until TP / series end. Returns (blended_gross, why)."""
    sl_lvl = entry_price * (1 - sl)
    tp_lvl = entry_price * (1 + tp)
    rec_lvl = ref_peak * (1 - REC)
    gap_fill = True
    n = len(series)
    we = min(n, idx + 1 + hold) if hold else n
    j = idx + 1
    while j < we:
        o, h, l = series[j][1], series[j][2], series[j][3]
        if l <= sl_lvl:
            px = o if (gap_fill and o < sl_lvl) else sl_lvl
            return px / entry_price - 1.0, "sl"
        if h >= rec_lvl and rec_lvl <= tp_lvl:
            bank_ret = rec_lvl / entry_price - 1.0
            # runner: breakeven stop, keep tp; scan bars after this one
            we2 = min(n, idx + 1 + hold) if hold else n
            k = j + 1
            run_px = None
            while k < we2:
                o2, h2, l2 = series[k][1], series[k][2], series[k][3]
                if l2 <= entry_price:
                    run_px = entry_price
                    break
                if h2 >= tp_lvl:
                    run_px = tp_lvl
                    break
                k += 1
            if run_px is None:
                run_px = series[we2 - 1][4]
            run_ret = run_px / entry_price - 1.0
            blended = bank_frac * bank_ret + (1 - bank_frac) * run_ret
            return blended, f"recpart{bank_frac:g}"
        if h >= tp_lvl:
            return tp_lvl / entry_price - 1.0, "tp"
        j += 1
    return series[n - 1][4] / entry_price - 1.0, "end"


def simulate(trade, series, policy):
    entry_price = trade["entry_price"]
    entry_ts, ref_peak = trade["entry_ts"], trade["ref_peak"]
    k = [i for i, b in enumerate(series) if b[0] >= entry_ts]
    if not k:
        return None
    idx = k[0]
    sl = trade["sl"] or 0.3
    tp = trade["tp"] or 1.0
    hold = trade["hold"] or 0
    if policy == "baseline":
        _, px, method = scan_first(idx, series, entry_price, ref_peak, sl, tp, hold)
        gross = px / entry_price - 1.0
    elif policy == "bank100":
        _, gross, method = _bank100(idx, series, entry_price, ref_peak, sl, tp, hold)
    else:
        frac = float(policy.replace("bank", "")) / 100.0
        gross, method = banked_scan(idx, series, entry_price, ref_peak, sl, tp, hold, frac)
    if gross is None:
        return None
    liquidity = trade.get("liquidity") or 10000.0
    net = strategy.net_return(gross, liquidity)
    return net, method


def scan_first(idx, series, entry_price, ref_peak, sl, tp, hold):
    sl_lvl = entry_price * (1 - sl)
    tp_lvl = entry_price * (1 + tp)
    rec_lvl = ref_peak * (1 - REC)
    gap_fill = True
    n = len(series)
    we = min(n, idx + 1 + hold) if hold else n
    j = idx + 1
    while j < we:
        o, h, l = series[j][1], series[j][2], series[j][3]
        if l <= sl_lvl:
            return j, (o if (gap_fill and o < sl_lvl) else sl_lvl), "sl"
        if h >= rec_lvl and rec_lvl <= tp_lvl:
            return j, rec_lvl, "rec"
        if h >= tp_lvl:
            return j, tp_lvl, "tp"
        j += 1
    return n - 1, series[n - 1][4], "end"


def _bank100(idx, series, entry_price, ref_peak, sl, tp, hold):
    lvl = ref_peak * (1 - REC)
    sl_lvl = entry_price * (1 - sl)
    tp_lvl = entry_price * (1 + tp)
    gap_fill = True
    n = len(series)
    we = min(n, idx + 1 + hold) if hold else n
    j = idx + 1
    while j < we:
        o, h, l = series[j][1], series[j][2], series[j][3]
        if l <= sl_lvl:
            px = o if (gap_fill and o < sl_lvl) else sl_lvl
            return j, px / entry_price - 1.0, "sl"
        if h >= lvl:
            return j, lvl / entry_price - 1.0, "rec100"
        if h >= tp_lvl:
            return j, tp_lvl / entry_price - 1.0, "tp"
        j += 1
    return n - 1, series[n - 1][4] / entry_price - 1.0, "end"


def main():
    policy_names = ["baseline", "bank100", "bank85", "bank90"]
    totals = {p: 0.0 for p in policy_names}
    counts = {p: 0 for p in policy_names}
    wins = {p: 0 for p in policy_names}
    for dbp in DBS:
        trades, obs = load(dbp)
        row = {p: 0.0 for p in policy_names}
        for t in trades:
            series = obs.get(t["token_address"], [])
            for p in policy_names:
                r = simulate(t, series, p)
                if r:
                    net, _ = r
                    row[p] += net
                    totals[p] += net
                    counts[p] += 1
                    wins[p] += (net > 0)
        print(f"== {dbp.split('vapor.db')[-1][:12] or '(main)'} trades={len(trades)}")
        for p in policy_names:
            print(f"   {p:8s} net={row[p]:+10.2f}")
    print(f"\n== ALL 3 DBs (n={counts['baseline']}) ==")
    for p in policy_names:
        wr = wins[p] / counts[p] * 100 if counts[p] else 0
        print(f"   {p:8s} net={totals[p]:+10.2f}  WR={wr:5.1f}%")


if __name__ == "__main__":
    main()