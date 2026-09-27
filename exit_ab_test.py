"""A/B exit test: baseline rec-exit vs Option A (hard +20%) vs Option B (trailing).

Drives the REAL engine.TokenState (entry gates, warmup, watch starts, one open
trade per config, 60m hold time-stop, 60m SL cooldown, gap-aware fills, expire)
over the 3 historical DBs with their RECORDED watch starts + discovery metadata,
exactly like parity_gmgn.py --dbs. Only the exit scan (_my_exit) is swapped per
scenario; every other decision is engine.py / config.py code as shipped.

Scenarios
  baseline : engine exit as shipped: SL -30% -> rec 95% of peak -> tp -> hold-60 end
  A_hard20 : SL -30% -> hard sell ALL at +20% over entry -> hold-60 end.
             (rec/tp are unreachable: dip 0.25 forces ref_peak > 1.333*entry, so
             rec_lvl >= +26.7% over entry -- +20% always fires first.)
  B_trail  : initial stop -20% (pre-arm only) -> arm at +20% over entry -> trail
             15% below the running peak. Arm bar must COMPLETE before the trail
             can trigger (no same-bar arm+exit). rec removed. hold-60 end kept.
             Cooldown after a trail stop too (it is a stop loss).

Speed notes (no behavioral change): refs are precomputed full-series (ref[i]
only uses bars < i, bit-identical to engine's incremental cache) and ingest
keeps a ts set instead of the O(n) any() scan.

Usage:
  python exit_ab_test.py                 # all 3 scenarios
  python exit_ab_test.py --s baseline    # baseline only (certified-numbers check)
"""
import argparse
import json
import os
import shutil
import statistics
import sys
import tempfile
import time

try:   # windows console is cp1252; token symbols are arbitrary unicode
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import strategy
import engine
import db as dbmod
import parity_gmgn as pg

# ---- user spec for this test ----
config.ACTIVE_CONFIG_IDS = [1, 2]   # lb180 + lb240, dip 0.25 both active
# everything else stays as the live bot runs it: COMBO_HOLD_MIN=60, GAP_FILL=True,
# FRESH_DIP_BARS=30, MIN_PCP1H_PCT=10, MIN_META_MC=300000, SL_COOLDOWN_MIN=60

ALLOC = config.ALLOC * config.CAP   # $25 per trade
HERE = os.path.dirname(os.path.abspath(__file__))
DL = os.path.join(HERE, "..", "Downloads", "Telegram Desktop")
# run-1 archive comes in two copies: the local snapshot and the bigger Downloads one
DB1 = "local" if os.environ.get("VAPOR_AB_DB1", "local") == "local" else "downloads"
DBS = [
    ("vapor.db", os.path.join(HERE, "vapor.db") if DB1 == "local"
     else os.path.join(DL, "vapor.db")),
    ("vapor.db.new", os.path.join(DL, "vapor.db.new")),
    ("vapor.db (2).new", os.path.join(DL, "vapor.db (2).new")),
]
COOLDOWN_WHY = ("sl", "trail")      # engine cooldowns on "sl"; trail stops are stops too
REASON_ORDER = ["sl", "rec", "tp", "end", "hard20", "trail"]

REFS = {}   # (src, addr, lb) -> full-series ref array (ref[i] uses only bars < i)
TRADES = []  # per-scenario list of closed trades with entry-time features


# ---------------------------------------------------------------- scenarios
class BaseTS(engine.TokenState):
    """Real engine with fast refs/ingest; _my_exit is a verbatim engine copy."""

    _src = ""      # set per source DB; keys the ref cache

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._c = []; self._o = []; self._h = []; self._l = []
        self._ts_seen = set()
        self._ts_idx = {}          # market_ts -> bar index (for MFE lookups)

    def ingest(self, bars):
        # engine.TokenState.ingest, plus O(1) dup check + incremental OHLC lists
        new = 0
        for b in bars:
            ts = b[0]
            if self.bars and ts <= self.bars[-1][0]:
                continue
            if ts in self._ts_seen:
                continue
            self._ts_seen.add(ts)
            self._ts_idx[ts] = len(self.bars)
            self.bars.append(b)
            self._o.append(b[1]); self._h.append(b[2]); self._l.append(b[3]); self._c.append(b[4])
            new += 1
        return new

    def _ref_for(self, lb):
        r = REFS.get((self._src, self.addr, lb))
        if r is None:
            r = strategy.ref_for([b[2] for b in self.bars], lb)
            REFS[(self._src, self.addr, lb)] = r
        return r

    def _my_exit(self, cfg, i, entry_price, ref_peak):
        # VERBATIM engine._my_exit (baseline), on prebuilt series lists
        c, o, h, l = self._c, self._o, self._h, self._l
        n = len(self.bars)
        sl_lvl = entry_price * (1 - cfg["sl"])
        tp_lvl = entry_price * (1 + cfg["tp"])
        rec_lvl = ref_peak * (1 - strategy.REC)
        we = min(n, i + 1 + cfg["hold"]) if cfg["hold"] else n
        j = i + 1
        while j < we:
            if l[j] <= sl_lvl:
                px = o[j] if config.GAP_FILL and o[j] < sl_lvl else sl_lvl
                return j, px, "sl"
            if h[j] >= rec_lvl and (config.SELL_ALL_AT_REC or rec_lvl <= tp_lvl):
                return j, rec_lvl, "rec"
            if h[j] >= tp_lvl:
                return j, tp_lvl, "tp"
            j += 1
        if cfg["hold"] and we == i + 1 + cfg["hold"]:
            return i + cfg["hold"], c[i + cfg["hold"]], "end"
        return None, None, None

    def _close_paper(self, tid, cfg, ex, px, why, decision_ts, decision_ms):
        # engine._close_paper verbatim, except the cooldown also covers trail stops
        trow = self.db.conn.execute(
            "SELECT token_address, config_id, entry_price, liquidity, entry_ts,"
            " drawdown, ref_peak FROM paper_trades WHERE id=?", (tid,)).fetchone()
        if not trow:
            return
        entry_price, liquidity, ets = trow[2], trow[3], trow[4]
        gross = px / entry_price - 1.0
        net = strategy.net_return(gross, liquidity)
        # ---- harness-only feature capture (no engine behavior) ----
        i = self._ts_idx.get(ets)
        rec = {"src": self._src, "addr": self.addr, "in_ms": ets, "why": why,
               "gross": gross, "net": net, "dd": trow[5], "liq": liquidity,
               "mfe": (max(self._h[i + 1:ex + 1]) / entry_price - 1.0)
                      if (i is not None and ex > i) else None,
               "vol": self.bars[i][5] if i is not None else None,
               "pcp1h": (self._c[i] / self._c[i - 60] - 1.0)
                        if (i is not None and i >= 60) else None,
               "peak_age": None, "mc": None}
        if i is not None:            # peak confirm age: engine's own scan
            for j in range(i - 1, max(0, i - cfg["lb"]) - 1, -1):
                if self._h[j] == trow[6]:
                    rec["peak_age"] = i - j
                    break
            try:                     # entry-bar mc from the discovery snapshot
                cand = self.db.candidate(self.addr)
                mc_d = json.loads(cand[9] or "{}").get("mc") if cand else None
                if isinstance(mc_d, (int, float)):
                    fms = (cand[4] or 0) * 1000.0      # first_seen_at (discovery)
                    for k in range(i, -1, -1):
                        if self.bars[k][0] <= fms:
                            rec["mc"] = mc_d * self._c[i] / self._c[k]
                            break
            except Exception:
                pass
        TRADES.append(rec)
        self.db.close_trade(tid, self.bars[ex][0], px, why, gross, net)
        addr = trow[0]
        self.open_trade_ids.pop(cfg["id"], None)
        self.last_exit_idx[cfg["id"]] = ex
        if why in COOLDOWN_WHY and config.SL_COOLDOWN_MIN:
            self.sl_cooldown_until_ms = self.bars[ex][0] + config.SL_COOLDOWN_MIN * 60000
        self.db.event("paper_exit", addr, cfg["id"], {
            "config_id": cfg["id"], "exit_ts": self.bars[ex][0], "exit_price": px,
            "reason": why, "gross_ret": gross, "net_ret": net,
            "observed_at": decision_ms})
        row = self.db.candidate(addr)
        if row and row[8] == "PAPER_TRADE":
            open_remaining = self.db.open_trades(addr)
            self.db.set_candidate_state(addr, "EXITED" if not open_remaining else "PAPER_TRADE")


class Hard20TS(BaseTS):
    """Option A: SL -30% unchanged, then hard sell-all at +20% over entry."""
    HARD = 0.20

    def _my_exit(self, cfg, i, entry_price, ref_peak):
        c, o, h, l = self._c, self._o, self._h, self._l
        n = len(self.bars)
        sl_lvl = entry_price * (1 - cfg["sl"])
        hard = entry_price * (1 + self.HARD)
        we = min(n, i + 1 + cfg["hold"]) if cfg["hold"] else n
        j = i + 1
        while j < we:
            if l[j] <= sl_lvl:
                px = o[j] if config.GAP_FILL and o[j] < sl_lvl else sl_lvl
                return j, px, "sl"
            if h[j] >= hard:
                return j, hard, "hard20"
            j += 1
        if cfg["hold"] and we == i + 1 + cfg["hold"]:
            return i + cfg["hold"], c[i + cfg["hold"]], "end"
        return None, None, None


class TrailTS(BaseTS):
    """Option B: initial stop -20% until armed at +20%, then trail 15% off the
    running peak. The arming bar must complete before the trail can trigger."""
    ARM = 0.20
    TRAIL = 0.15
    INIT_SL = 0.20

    def _my_exit(self, cfg, i, entry_price, ref_peak):
        c, o, h, l = self._c, self._o, self._h, self._l
        n = len(self.bars)
        init = entry_price * (1 - self.INIT_SL)
        arm = entry_price * (1 + self.ARM)
        we = min(n, i + 1 + cfg["hold"]) if cfg["hold"] else n
        j = i + 1
        peak = None
        while j < we:
            if peak is None:
                if l[j] <= init:                       # pre-arm fixed stop
                    px = o[j] if config.GAP_FILL and o[j] < init else init
                    return j, px, "sl"
                if h[j] >= arm:
                    peak = h[j]                        # armed; trail starts NEXT bar
            else:
                peak = max(peak, h[j - 1])             # running peak thru previous bar
                stop = peak * (1 - self.TRAIL)
                if l[j] <= stop:
                    px = o[j] if config.GAP_FILL and o[j] < stop else stop
                    return j, px, "trail"
            j += 1
        if cfg["hold"] and we == i + 1 + cfg["hold"]:
            return i + cfg["hold"], c[i + cfg["hold"]], "end"
        return None, None, None


# ---------------------------------------------------------------- plumbing
def precompute_refs(universe):
    t0 = time.time()
    lbs = sorted({c["lb"] for c in config.FROZEN_CONFIGS
                  if c["id"] in config.ACTIVE_CONFIG_IDS})
    try:
        import pandas as pd
        for u in universe:
            highs = [b[2] for b in u["bars"]]
            for lb in lbs:
                REFS[(u["src"], u["addr"], lb)] = (
                    pd.Series(highs, dtype="float64")
                    .rolling(lb, min_periods=1).max().shift(1).to_numpy())
        # parity check vs strategy.ref_for on one series
        import numpy as np
        u0 = universe[0]
        a = REFS[(u0["src"], u0["addr"], lbs[-1])]
        b = strategy.ref_for([x[2] for x in u0["bars"]], lbs[-1])
        if not np.allclose(a, b, equal_nan=True, rtol=0, atol=0):
            raise AssertionError("pandas ref != strategy.ref_for")
    except Exception as e:                              # no pandas / mismatch: slow path
        print(f"  (pandas refs unavailable: {e!r}; falling back to strategy.ref_for)")
        REFS.clear()
        for u in universe:
            highs = [b[2] for b in u["bars"]]
            for lb in lbs:
                REFS[(u["src"], u["addr"], lb)] = strategy.ref_for(highs, lb)
    print(f"refs precomputed: {len(universe)} tokens x lb{lbs} in {time.time()-t0:.1f}s",
          flush=True)


def tally_db(d):
    r = {"n": 0, "reasons": {}, "net": 0.0, "net_why": {}, "nets_why": {}, "wins": [], "losses": [],
         "open": 0, "cfg": {}}
    for why, nr, st, cid in d.conn.execute(
            "SELECT exit_reason, net_ret, state, config_id FROM paper_trades"):
        r["n"] += 1
        r["reasons"][why] = r["reasons"].get(why, 0) + 1
        if st == "OPEN":
            r["open"] += 1
        if nr is not None:
            r["net"] += nr
            r["net_why"][why] = r["net_why"].get(why, 0.0) + nr * ALLOC
            r["nets_why"].setdefault(why, []).append(nr * ALLOC)
            (r["wins"] if nr > 0 else r["losses"]).append(nr * ALLOC)
        r["cfg"][cid] = r["cfg"].get(cid, 0) + 1
    return r


def agg_tallies(ts):
    a = {"n": 0, "reasons": {}, "net": 0.0, "net_why": {}, "nets_why": {}, "wins": [], "losses": [],
         "open": 0, "cfg": {}}
    for r in ts:
        a["n"] += r["n"]
        a["net"] += r["net"]
        a["open"] += r["open"]
        a["wins"] += r["wins"]
        a["losses"] += r["losses"]
        for k, v in r["reasons"].items():
            a["reasons"][k] = a["reasons"].get(k, 0) + v
        for k, v in r["net_why"].items():
            a["net_why"][k] = a["net_why"].get(k, 0.0) + v
        for k, v in r["nets_why"].items():
            a["nets_why"].setdefault(k, []).extend(v)
        for k, v in r["cfg"].items():
            a["cfg"][k] = a["cfg"].get(k, 0) + v
    return a


def fmt(label, a):
    wr = len(a["wins"]) / a["n"] if a["n"] else 0.0
    aw = sum(a["wins"]) / len(a["wins"]) if a["wins"] else 0.0
    al = sum(a["losses"]) / len(a["losses"]) if a["losses"] else 0.0
    rs = a["reasons"]
    cells = " ".join(f"{k}:{rs.get(k, 0)}" for k in REASON_ORDER)
    print(f"{label:<22}{a['n']:>6}{wr:>7.1%}{a['net']*ALLOC:>+10.2f}"
          f"{aw:>8.2f}{al:>8.2f}   {cells}  open={a['open']}", flush=True)
    why_net = "  ".join(f"{k}:{a['net_why'].get(k, 0.0):+.1f}" for k in REASON_ORDER
                        if rs.get(k))
    print(f"{'':<22}net by reason: {why_net}", flush=True)
    for k in REASON_ORDER:                      # win/loss split per exit reason
        nets = a.get("nets_why", {}).get(k)
        if not nets:
            continue
        pos = [x for x in nets if x > 0]
        neg = [x for x in nets if x <= 0]
        pos_s = f"{len(pos)} avg {sum(pos)/len(pos):+.2f}" if pos else "0"
        neg_s = f"{len(neg)} avg {sum(neg)/len(neg):+.2f}" if neg else "0"
        print(f"{'':<22}  {k:<7} pos: {pos_s:<24} neg: {neg_s}", flush=True)


def print_mfe_dist(label, rows):
    """MFE distribution for SL trades (parity_gmgn convention: max high over
    bars entry+1 .. exit inclusive, vs entry price)."""
    if not rows:
        return
    mfes = sorted(r["mfe"] * 100 for r in rows)
    buckets = [("<0%", -1e9, 0.0), ("0-5%", 0.0, 5.0), ("5-10%", 5.0, 10.0),
               ("10-15%", 10.0, 15.0), ("15-20%", 15.0, 20.0), (">=20%", 20.0, 1e9)]
    print(f"{label} SL-trade MFE before the stop fired (n={len(mfes)}, "
          f"median {statistics.median(mfes):+.1f}%):", flush=True)
    for nm, lo, hi in buckets:
        k = sum(1 for m in mfes if lo <= m < hi)
        print(f"  {nm:>7}: {k:>4}  ({k/len(mfes):>5.1%})", flush=True)


def print_group_compare(label, trades):
    """Entry-time features: SL duds (MFE 0-5%) vs winning trail exits."""
    duds = [t for t in trades if t["why"] == "sl" and t.get("mfe") is not None
            and 0.0 <= t["mfe"] * 100 < 5.0]
    wins = [t for t in trades if t["why"] == "trail" and (t["net"] or 0.0) > 0]
    if not duds or not wins:
        return

    def num(x):
        a = abs(x)
        if a >= 1e6:
            return f"{x/1e6:,.2f}M"
        if a >= 1000:
            return f"{x:,.0f}"
        return f"{x:.4g}"

    print(f"\n{label}: entry-time features, DUDS (SL, MFE 0-5%, n={len(duds)}) "
          f"vs TRAIL WINNERS (net>0, n={len(wins)})", flush=True)
    fields = [("market cap $ (entry-bar, scaled from disc snapshot)", "mc"),
              ("pcp1h % (close[i]/close[i-60]-1, entry bar)", "pcp1h"),
              ("liquidity $ (candidate, at trade open)", "liq"),
              ("entry-bar volume $", "vol"),
              ("peak age (bars)", "peak_age"),
              ("entry drawdown %", "dd")]
    for name, key in fields:
        g = {}
        for tag, grp in (("duds", duds), ("win", wins)):
            v = sorted(t[key] for t in grp if t.get(key) is not None)
            g[tag] = v
        sd, sw = g["duds"], g["win"]
        if not sd or not sw:
            missing = "duds" if not sd else "winners"
            print(f"  {name:<52} no data for {missing}", flush=True)
            continue
        med = statistics.median
        print(f"  {name:<52} duds: n={len(sd)} med={num(med(sd))} mean={num(sum(sd)/len(sd))} "
              f"p25={num(sd[len(sd)//4])} p75={num(sd[(3*len(sd))//4])}", flush=True)
        print(f"  {'':<52} win : n={len(sw)} med={num(med(sw))} mean={num(sum(sw)/len(sw))} "
              f"p25={num(sw[len(sw)//4])} p75={num(sw[(3*len(sw))//4])}", flush=True)
        # best single-threshold separation (which side of a cut the duds sit on)
        allv = sorted(set(sd + sw))
        best = (0.0, None, "")
        for t in ((a + b) / 2 for a, b in zip(allv, allv[1:])):
            for sym, f in (("<", lambda x: x < t), (">", lambda x: x > t)):
                fd = sum(1 for x in sd if f(x)) / len(sd)
                fw = sum(1 for x in sw if f(x)) / len(sw)
                if fd - fw > best[0]:
                    best = (fd - fw, t, sym)
        if best[1] is not None:
            j, t, sym = best
            fd = sum(1 for x in sd if (x < t if sym == "<" else x > t)) / len(sd)
            print(f"  {'':<52} best split: duds {sym} {num(t)}  catches {fd:.0%} of duds, "
                  f"{j:.0%} net separation from winners", flush=True)
    print(flush=True)


def run_scenario(name, cls, by_src, workdir):
    engine.TokenState = cls        # pg.run_token drives this class
    TRADES.clear()
    rows, gates = {}, {}
    for src, uni in by_src.items():
        p = os.path.join(workdir, "".join(ch if ch.isalnum() else "_" for ch in src) + ".db")
        if os.path.exists(p):
            os.remove(p)
        d = dbmod.DB(p, run_id="ab")

        def row_eval(addr, cfg, market_ts, dts, dms, ep, ref, dd, trig, reason, _g=gates):
            _g[reason] = _g.get(reason, 0) + 1     # count only; skip the big insert
        d.row_eval = row_eval

        t0 = time.time()
        cls._src = src                # refs are keyed per source DB
        for k, u in enumerate(uni):
            pg.run_token(d, u["addr"], u["sym"], u["bars"], None, u["liq"], "disc",
                         meta=u["meta"], watch_start_ms=u["watch_start_ms"], src=src)
            if (k + 1) % 50 == 0:
                print(f"  [{name}] {src}: {k+1}/{len(uni)} tokens ({time.time()-t0:.0f}s)",
                      flush=True)
        d.commit()
        rows[src] = tally_db(d)
        d.close()
    return rows, gates, list(TRADES)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--s", "--scenario", dest="scenario", default="all",
                    choices=("all", "baseline", "A", "B"))
    ap.add_argument("--gates", default="live", choices=("live", "fresh"),
                    help="live = fresh30+pcp10+mc300k (current bot); "
                         "fresh = fresh30 only (the certified 578-trade replay)")
    args = ap.parse_args()
    if args.gates == "fresh":
        config.MIN_PCP1H_PCT = 0
        config.MIN_META_MC = 0

    workdir = tempfile.mkdtemp(prefix="vapor_ab_")
    by_src = {}
    for src, path in DBS:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            print(f"!! MISSING/EMPTY db: {src} at {path}")
            continue
        uni = pg.load_db_universe([path])
        nb = sum(len(u["bars"]) for u in uni)
        print(f"{src}: tokens={len(uni)} bars={nb}", flush=True)
        if uni:
            by_src[src] = uni
    if not by_src:
        sys.exit("no data")
    precompute_refs([u for uni in by_src.values() for u in uni])

    print(f"\nconfigs: {config.ACTIVE_CONFIG_IDS}  hold={config.COMBO_HOLD_MIN}m  "
          f"gap_fill={config.GAP_FILL}  sl_cooldown={config.SL_COOLDOWN_MIN}m  "
          f"gates(fresh/pcp/mc)={config.FRESH_DIP_BARS}/{config.MIN_PCP1H_PCT}/"
          f"{config.MIN_META_MC}  alloc=${ALLOC:.0f}/trade", flush=True)

    scenarios = [("baseline", BaseTS), ("A_hard20", Hard20TS), ("B_trail", TrailTS)]
    if args.scenario != "all":
        key = {"baseline": "baseline", "A": "A_hard20", "B": "B_trail"}[args.scenario]
        scenarios = [(k, c) for k, c in scenarios if k == key]

    results, mfe_by_scen = {}, {}
    for name, cls in scenarios:
        t0 = time.time()
        print(f"\n=== {name} ===", flush=True)
        rows, gates, mfes = run_scenario(name, cls, by_src, workdir)
        mfe_by_scen[name] = mfes
        results[name] = agg_tallies(rows.values())
        fmt(name, results[name])
        for src, r in rows.items():
            fmt(f"  {src}", r)
        blocked = "  ".join(f"{k}={v}" for k, v in sorted(gates.items()) if v)
        print(f"  entry gates blocked: {blocked}")
        print(f"  ({time.time()-t0:.0f}s)", flush=True)
        print_mfe_dist(name, [t for t in mfes if t["why"] == "sl"])
        print_group_compare(name, mfes)

    if "baseline" in results:
        b = results["baseline"]
        print(f"\ncertified check: baseline trades={b['n']} (certified 578), "
              f"sl={b['reasons'].get('sl', 0)} (certified 264) -> "
              f"{'MATCH' if b['n'] == 578 and b['reasons'].get('sl', 0) == 264 else 'DIFFERS (see notes)'}")

    if len(results) == 3:
        print("\n" + "=" * 100)
        print("FINAL SIDE-BY-SIDE (3 DBs combined, real engine.TokenState, $25/trade, fees included)")
        print(f"{'scenario':<22}{'trades':>6}{'WR':>7}{'net $':>10}{'avgWin$':>8}{'avgLoss$':>8}"
              f"   exit reasons (sl rec tp end hard20 trail)")
        for name in ("baseline", "A_hard20", "B_trail"):
            fmt(name, results[name])
        print("=" * 100)
        print("notes: A = SL stays -30%, rec/tp unreachable (dip 0.25 => rec >= +26.7% over entry);")
        print("       B = -20% initial stop pre-arm, arm +20%, trail 15% off running peak")
        print("           (arm bar completes before trail triggers), cooldown covers trail stops;")
        print("       both keep gap-aware fills, 60m hold time-stop, 60m SL cooldown, cfg1+cfg2,")
        print("       all live entry gates, recorded watch starts + discovery metadata.")
    shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
