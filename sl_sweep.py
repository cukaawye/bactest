"""Bar-level exit sweep: for each CLOSED trade in the db, re-walk its recorded
post-entry bars with modified SL/tp levels (same rec rule, hold=0) and report
total EV under each config. Confirms whether a tighter SL (or an exit-ladder)
flips the run positive — the honest EV question.
Usage: python sl_sweep.py [db]
"""
import sqlite3
import sys

import strategy

DB = sys.argv[1] if len(sys.argv) > 1 else "vapor.db"
REC = 0.05
net_return = strategy.net_return


def main():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    trades = [dict(r) for r in c.execute(
        "SELECT * FROM paper_trades WHERE state='CLOSED' AND "
        "(exit_reason='sl' OR exit_reason='rec' OR exit_reason='tp')")]
    bars_by_token = {}
    for t in trades:
        addr = t["token_address"]
        if addr not in bars_by_token:
            bars_by_token[addr] = c.execute(
                "SELECT market_ts, open, high, low, close FROM observations "
                "WHERE token_address=? ORDER BY market_ts", (addr,)).fetchall()

    # record original params
    for t in trades:
        t["bars"] = bars_by_token[t["token_address"]]

    def replay(t, sl_frac, tp_frac):
        bars = [b for b in t["bars"] if b[0] >= t["entry_ts"]]
        entry = t["entry_price"]
        ref = t["ref_peak"]
        sl_lvl = entry * (1 - sl_frac)
        tp_lvl = entry * (1 + tp_frac)
        rec_lvl = ref * (1 - REC)
        rec_active = rec_lvl <= tp_lvl
        for b in bars[1:]:
            o, h, l, cl = b[1], b[2], b[3], b[4]
            if l <= sl_lvl:
                return "sl", sl_lvl / entry - 1.0
            if rec_active and h >= rec_lvl:
                return "rec", rec_lvl / entry - 1.0
            if h >= tp_lvl:
                return "tp", tp_lvl / entry - 1.0
        return "end", cl / entry - 1.0

    scenarios = [(0.50, 1.00, "current sl50/tp100"),
                 (0.40, 1.00, "sl40/tp100"),
                 (0.30, 1.00, "sl30/tp100"),
                 (0.25, 1.00, "sl25/tp100"),
                 (0.20, 1.00, "sl20/tp100"),
                 (0.50, 0.75, "sl50/tp75"),
                 (0.50, 0.50, "sl50/tp50"),
                 (0.30, 0.30, "sl30/tp30"),
                 ]
    print(f"{'scenario':<18}{'sl':>5}{'rec':>5}{'tp':>5}{'end':>5}{'n_win':>7}"
          f"{'net$':>8}{'avg$':>7}{'trades':>7}")
    for sl, tp, label in scenarios:
        tot = 0.0
        win = 0
        cnt = {"sl": 0, "rec": 0, "tp": 0, "end": 0}
        for t in trades:
            why, gross = replay(t, sl, tp)
            net = net_return(gross, t["liquidity"])
            cnt[why] += 1
            tot += net
            if net > 0:
                win += 1
        print(f"{label:<18}{cnt['sl']:>5}{cnt['rec']:>5}{cnt['tp']:>5}{cnt['end']:>5}"
              f"{win:>7}/{len(trades)}{tot:>8.2f}{tot/len(trades):>7.3f}{len(trades):>7}")


if __name__ == "__main__":
    main()