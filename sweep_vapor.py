"""Standalone config sweep over the bot-run data in vapor.db (read-only,
reuses replay_gates.walk_trades for identical math + gates). Sweeps an
extended grid (bigger TPs, wider SLs/HOLDS) and ranks by modeled net return.

Usage:
  python sweep_vapor.py                    # gates off, full grid
  python sweep_vapor.py --gates            # with fresh-dip + pcp1h gates
  python sweep_vapor.py --tp 1 2 3         # custom tp values
"""
import argparse
import time

import config
import strategy
import replay_gates


def sweep(tokens, grid, gates):
    fresh = config.FRESH_DIP_BARS if gates else 0
    pcp = config.MIN_PCP1H_PCT if gates else 0
    rows = []
    for lb, dip, tp, sl, hold in grid:
        agg = {"n": 0, "sl": 0, "rec": 0, "tp": 0, "end": 0, "net": 0.0}
        for t in tokens.values():
            cfg = {"id": -1, "lb": lb, "dip": dip, "tp": tp, "sl": sl, "hold": hold}
            for _i, _ex, gross, why in replay_gates.walk_trades(
                    t["bars"], cfg, t["watch_start_ms"], fresh, pcp, t["meta"]):
                net = strategy.net_return(gross, t["liquidity"])
                agg["n"] += 1
                agg[why] += 1
                agg["net"] += net
        if agg["n"] >= 3:  # require some trades to mean anything
            rows.append(dict(lb=lb, dip=dip, tp=tp, sl=sl, hold=hold, n=agg["n"],
                             n_sl=agg["sl"], n_rec=agg["rec"], n_tp=agg["tp"], n_end=agg["end"],
                             net=agg["net"], avg=agg["net"] / agg["n"],
                             wr=(agg["rec"] + agg["tp"]) / agg["n"]))
    return sorted(rows, key=lambda r: r["net"], reverse=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gates", action="store_true")
    ap.add_argument("--lb", nargs="+", type=int, default=[60, 120, 180, 240, 360])
    ap.add_argument("--dip", nargs="+", type=float, default=[0.20, 0.25, 0.30])
    ap.add_argument("--tp", nargs="+", type=float, default=[0.50, 0.75, 1.00, 1.50, 2.00, 3.00])
    ap.add_argument("--sl", nargs="+", type=float, default=[0.30, 0.40, 0.50, 0.60, 0.75])
    ap.add_argument("--hold", nargs="+", type=int, default=[0, 120, 360])
    ap.add_argument("--top", type=int, default=25)
    args = ap.parse_args()

    tokens = replay_gates.load()
    print(f"tokens={len(tokens)} gates={'on' if args.gates else 'off'}")
    grid = [(l, d, t, s, h) for l in args.lb for d in args.dip
            for t in args.tp for s in args.sl for h in args.hold]
    print(f"{len(grid)} configs ...", flush=True)
    t0 = time.time()
    rows = sweep(tokens, grid, args.gates)
    print(f"done in {time.time()-t0:.0f}s | {len(rows)} configs with >=3 trades\n")

    print(f"{'lb':>4}{'dip':>5}{'tp':>6}{'sl':>5}{'hold':>5}{'n':>5}{'wr':>6}{'avg$':>7}{'net$':>9}  sl/rec/tp/end")
    for r in rows[:args.top]:
        print(f"{r['lb']:>4}{r['dip']:>5.2f}{r['tp']:>6.2f}{r['sl']:>5.2f}{r['hold']:>5}{r['n']:>5}"
              f"{r['wr']:>6.2f}{r['avg']:>7.3f}{r['net']:>9.2f}  {r['n_sl']}/{r['n_rec']}/{r['n_tp']}/{r['n_end']}")


if __name__ == "__main__":
    main()