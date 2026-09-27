"""partial_sim.py — experiment: what-if exit policies on RECORDED trades.

For every CLOSED paper trade in a DB, re-simulate the post-entry OHLC series
under alternative exit policies and compare net P&L:
  - bank100: sell everything at the REC level (0.95*ref_peak) as soon as hit.
  - bankF+runner (F=0.85/0.90): bank F at REC, let the rest run with SL moved to
    entry (breakeven) until TP (+100%) or the bar series ends.
  - baseline: the engine's own SL->rec->TP scan (gap-aware fills, GAP_FILL=True).
  - combo: the user-requested combination — 30% SL, sell-all at REC, 60-minute
    time-stop (hold=60), gap-aware fills, PLUS a 60-minute re-entry cooldown per
    token after any stop-loss (churn killer). Entries are the recorded ones
    (i.e. the live bot's actual "current entry filters" execution); the cooldown
    drops recorded entries that would have been blocked.

Precisely reproduces engine._my_exit ordering and fill model so the policies
differ ONLY in exit/cooldown behavior. net = gross - cost (same strategy cost
model, per-trade independent as the engine does).
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


def combo_sim(trades, obs):
    """trades ordered by entry_ts for ONE token; 60-min cooldown after an SL
    skip any entry within COOLDOWN_MS of that SL's exit. Returns (nets, blocked)."""
    COOLDOWN_MS = 60 * 60000
    blocked = 0
    out = []
    last_sl_ts = None
    for t in trades:
        series = obs.get(t["token_address"], [])
        if not series:
            continue
        entry_ts = t["entry_ts"]
        if last_sl_ts is not None and entry_ts < last_sl_ts + COOLDOWN_MS:
            blocked += 1
            continue
        k = [i for i, b in enumerate(series) if b[0] >= entry_ts]
        if not k:
            continue
        gross, method, exit_ts = combo_exit(
            k[0], series, t["entry_price"], t["ref_peak"], 0.30, 1.0, 60)
        liquidity = t.get("liquidity") or 10000.0
        out.append(strategy.net_return(gross, liquidity))
        if method == "sl":
            last_sl_ts = exit_ts
    return out, blocked


def combo_exit(idx, series, entry_price, ref_peak, sl, tp, hold):
    """sl(30%) first, then sell-all at REC, then TP, then hold=60 time-stop."""
    sl_lvl = entry_price * (1 - sl)
    tp_lvl = entry_price * (1 + tp)
    rec_lvl = ref_peak * (1 - REC)
    n = len(series)
    we = min(n, idx + 1 + hold) if hold else n
    j = idx + 1
    while j < we:
        o, h, l = series[j][1], series[j][2], series[j][3]
        if l <= sl_lvl:
            px = o if (o < sl_lvl) else sl_lvl  # gap-aware fill
            return px / entry_price - 1.0, "sl", series[j][0]
        if h >= rec_lvl:
            return rec_lvl / entry_price - 1.0, "rec100", series[j][0]
        if h >= tp_lvl:
            return tp_lvl / entry_price - 1.0, "tp", series[j][0]
        j += 1
    if we > idx + 1 and we < n:
        return series[we][4] / entry_price - 1.0, "end", series[we][0]
    return series[n - 1][4] / entry_price - 1.0, "end", series[n - 1][0]


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
    policy_names = ["baseline", "bank100", "bank85", "bank90", "combo"]
    totals = {p: 0.0 for p in policy_names}
    counts = {p: 0 for p in policy_names}
    wins = {p: 0 for p in policy_names}
    # recorded totals: what the live bot actually logged (optimistic fills)
    print(f"  {'policy':8s} {'n':>5s}  totals (recorded vs sim)")
    for dbp in DBS:
        trades, obs = load(dbp)
        recorded = sum(t["net_ret"] or 0 for t in trades)
        row = {p: 0.0 for p in policy_names}
        nrow = {p: 0 for p in policy_names}
        by_token = {}
        for t in trades:
            by_token.setdefault(t["token_address"], []).append(t)
        for addr, tl in by_token.items():
            tl.sort(key=lambda x: x["entry_ts"])
            for p in policy_names:
                if p == "combo":
                    nets, blocked = combo_sim(tl, obs)
                    for net in nets:
                        row[p] += net
                        nrow[p] += 1
                        totals[p] += net
                        counts[p] += 1
                        wins[p] += (net > 0)
                else:
                    for t in tl:
                        r = simulate(t, obs.get(t["token_address"], []), p)
                        if r:
                            net, _ = r
                            row[p] += net
                            nrow[p] += 1
                            totals[p] += net
                            counts[p] += 1
                            wins[p] += (net > 0)
        print(f"== {dbp.split('vapor.db')[-1][:12] or '(main)'} recorded_net={recorded:+.2f} trades={len(trades)}")
        for p in policy_names:
            print(f"   {p:8s} n={nrow[p]:5d} net={row[p]:+10.2f}")
    print(f"\n== ALL 3 DBs (combo blocks churn re-entries within 60min of an SL) ==")
    for p in policy_names:
        wr = wins[p] / counts[p] * 100 if counts[p] else 0
        print(f"   {p:8s} n={counts[p]:5d} net={totals[p]:+10.2f}  WR={wr:5.1f}%")

if __name__ == "__main__":
    main()