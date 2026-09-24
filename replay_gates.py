"""Standalone replay of stored vapor.db data through the entry gates.

Reads candidates + observations from vapor.db (read-only), walks each config
over every token's recorded bars EXACTLY like the live engine (walk() parity:
ref rolling-max shifted, dip signal, SL->rec->TP->hold exit, cooldown),
optionally applying the Vapor-only gates:
  fresh  : skip signals whose reference peak formed > N bars ago  (stale-peak)
  pcp1h  : skip tokens with 1h % price change < threshold          (cold-pcp1h)

Usage:
  python replay_gates.py                     # gates off vs fresh=30 vs pcp=10 vs both
  python replay_gates.py 30 10               # custom fresh-bars / min-pcp1h (0=off)
"""
import json
import sqlite3
import sys

import strategy
import config


def walk_trades(bars, cfg, watch_start_ms, fresh_bars, min_pcp1h, min_mc, meta):
    """Mirror of engine._entry_signal + _my_exit over a recorded series.
    Returns list of (entry_idx, exit_idx, gross, why). Gate-rejected signals
    are skipped (pos advances) without a trade."""
    n = len(bars)
    close = [b[4] for b in bars]
    high = [b[2] for b in bars]
    low = [b[3] for b in bars]
    ref = strategy.ref_for(high, cfg["lb"])
    sigs = strategy.sigs_for(close, ref, cfg["dip"])
    trades = []
    pos = 0
    while pos < len(sigs):
        i = sigs[pos]
        if ref[i] != ref[i] or i < config.MIN_BARS_ENTER - 1:   # NaN ref / warmup
            pos += 1
            continue
        if bars[i][0] < watch_start_ms:                          # pre-watch
            pos += 1
            continue
        # --- Vapor-only gates (skip signal, no trade) ---
        if fresh_bars and peak_is_stale(bars, i, cfg["lb"], ref[i], fresh_bars):
            pos += 1
            continue
        pcp = meta.get("pcp1h") if meta else None
        if min_pcp1h and not (isinstance(pcp, (int, float)) and pcp >= min_pcp1h):
            pos += 1
            continue
        mc = meta.get("mc") if meta else None
        if min_mc and not (isinstance(mc, (int, float)) and mc >= min_mc):
            pos += 1
            continue
        # --- open trade (engine ordering identical) ---
        entry = close[i]
        sl_lvl = entry * (1 - cfg["sl"])
        tp_lvl = entry * (1 + cfg["tp"])
        rec_lvl = ref[i] * (1 - strategy.REC)
        we = min(n, i + 1 + cfg["hold"]) if cfg["hold"] else n
        ex = px = None
        why = "end"
        for j in range(i + 1, we):
            if low[j] <= sl_lvl:
                ex, px, why = j, sl_lvl, "sl"
                break
            if high[j] >= rec_lvl and rec_lvl <= tp_lvl:
                ex, px, why = j, rec_lvl, "rec"
                break
            if high[j] >= tp_lvl:
                ex, px, why = j, tp_lvl, "tp"
                break
        if ex is None:
            ex = we - 1 if we > i + 1 else i
            px = close[ex]
        trades.append((i, ex, px / entry - 1.0, why))
        pos += 1
        while pos < len(sigs) and sigs[pos] <= ex:
            pos += 1
    return trades


def peak_is_stale(bars, i, lb, peak, limit):
    hi = [b[2] for b in bars]
    lo = max(0, i - lb)
    for j in range(i - 1, lo - 1, -1):
        if hi[j] == peak:
            return (i - j) > limit
    return True


def load():
    conn = sqlite3.connect("vapor.db")
    conn.row_factory = sqlite3.Row
    tokens = {}
    for c in conn.execute("SELECT * FROM candidates"):
        bars = [tuple(b) for b in conn.execute(
            "SELECT market_ts, open, high, low, close, volume FROM observations "
            "WHERE token_address=? ORDER BY market_ts", (c["token_address"],))]
        if not bars:
            continue
        meta = {}
        try:
            meta = json.loads(c["meta_json"] or "{}")
        except Exception:
            pass
        tokens[c["token_address"]] = {
            "sym": c["symbol"], "bars": bars,
            "watch_start_ms": (c["first_seen_at"] or 0) * 1000.0,
            "liquidity": c["liquidity"] or 1000.0,
            "meta": meta,
        }
    conn.close()
    return tokens


def run(tokens, fresh_bars, min_pcp1h, min_mc=0):
    per_token = {}
    agg = {"n": 0, "sl": 0, "rec": 0, "tp": 0, "end": 0, "net": 0.0}
    for addr, t in tokens.items():
        token_net = 0.0
        for cfg in config.FROZEN_CONFIGS:
            for _i, _ex, gross, why in walk_trades(
                    t["bars"], cfg, t["watch_start_ms"], fresh_bars, min_pcp1h, min_mc, t["meta"]):
                net = strategy.net_return(gross, t["liquidity"])
                agg["n"] += 1
                agg[why] += 1
                agg["net"] += net
                token_net += net
        per_token[t["sym"]] = token_net
    return agg, per_token


def main():
    fresh = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    pcp = float(sys.argv[2]) if len(sys.argv) > 2 else 10.0
    mc = float(sys.argv[3]) if len(sys.argv) > 3 else 300000
    tokens = load()
    print(f"tokens={len(tokens)}")

    label = [("baseline(fresh=0,pcp=0,mc=0)", 0, 0.0, 0),
             (f"fresh(peak<={fresh}b)", fresh, 0.0, 0),
             (f"pcp1h(>={pcp:g}%)", 0, pcp, 0),
             (f"mc(>={mc:g}k)", 0, 0.0, mc),
             (f"all(fresh={fresh},pcp={pcp:g},mc={mc:g}k)", fresh, pcp, mc)]
    results = {nm: run(tokens, fb, pp, mm) for nm, fb, pp, mm in label}
    print(f"{'':24}{'trades':>6}{'WR':>6}{'sl':>5}{'rec':>5}{'tp':>5}{'end':>5}{'net$':>8}{'avg$':>7}  tokens: +net / -net")
    for name, _fb, _pp, _mm in label:
        a, pt = results[name]
        wr = a["rec"] + a["tp"]
        wrr = wr / a["n"] if a["n"] else 0
        good = sum(1 for v in pt.values() if v > 0)
        bad = sum(1 for v in pt.values() if v <= 0)
        print(f"{name:24}{a['n']:>6}{wrr:>6.2f}{a['sl']:>5}{a['rec']:>5}{a['tp']:>5}{a['end']:>5}"
              f"{a['net']:>8.2f}{(a['net']/a['n'] if a['n'] else 0):>7.3f}  {good} / {bad}")

    print("\nper-token net by scenario:")
    _, p0 = results["baseline(fresh=0,pcp=0,mc=0)"]
    for sym in sorted(p0, key=p0.get):
        row = "  ".join(f"{name.split('(')[0]:>9}:{results[name][1].get(sym, 0):>8.2f}" for name, _fb, _pp, _mm in label)
        print(f"  {sym:12}{row}")


if __name__ == "__main__":
    main()