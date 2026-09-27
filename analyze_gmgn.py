"""Deep WHY/HOW analysis of the GMGN 24h pull through engine logic.

Every decision is recorded with its inputs:
  - per-bar signal detail (drawdown, peak age, reconstructed pcp1h & mc, volume)
  - per-trade full path (entry, SL/rec/TP levels, MAE/MFE excursions, bars held)
  - gate ablation (fresh-only / pcp-only / mc-only / all / none) on THIS universe
  - winner vs loser anatomy, config 1 vs 2, cooldown & open-trade swallows.
Writes full detail to gmgn_replay_detail.json and prints a structured report.
"""
import json
import time

import config
import strategy

ALLOC = 0.25 * config.CAP  # $25 position per trade

META = {
    "6uPEdkU2xPg1iyfLzV1ssUwvve8nndA2sd6nBFrb1n58": (1017980.0, 297252.0),
    "CbcyNo7m1amFWqEQm2m4PLv1UNvpcL3C1Ujm6AkzpKoU": (3864070.0, 294111.0),
    "J1yxV53EmVRmt9RUj6PPufMfdAYUbtgwXCuNYTBTsYzJ": (395585.0, 74033.0),
    "FGZScHYURa8qMezk5gpZj5C2ExazybAwBp5TbNE4M3A8": (108028.0, 28474.6),
    "GW5xNXBtW9usH62Uimis4L5T1Fi5ceeMa9dupjTVpxaD": (22376.1, 10906.2),
    "FGoHcBuWfy6XGGkXwTLe9AuXAQEUmDR5g1LkxK98d1vJ": (38015.5, 19583.6),
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


def peak_age_bars(bars, i, lb, peak):
    """(bars since peak-confirm, index of peak confirm). Matches engine scan."""
    hi = [b[2] for b in bars]
    lo = max(0, i - lb)
    for j in range(i - 1, lo - 1, -1):
        if hi[j] == peak:
            return (i - j, j)
    return (None, None)


def run_token(bars, mc_now, liquidity, gates):
    """Full decision log for one token. gates = dict(fresh, pcp, mc) on/off."""
    n = len(bars)
    close = [b[4] for b in bars]
    high = [b[2] for b in bars]
    low = [b[3] for b in bars]
    open_ = [b[1] for b in bars]
    vol = [b[5] for b in bars]
    ts = [b[0] for b in bars]
    close_last = close[-1]
    configs = [dict(c) for c in config.FROZEN_CONFIGS
               if not config.ACTIVE_CONFIG_IDS or c["id"] in config.ACTIVE_CONFIG_IDS]
    for c in configs:
        if config.COMBO_HOLD_MIN:
            c["hold"] = config.COMBO_HOLD_MIN
    refs = {c["id"]: strategy.ref_for(high, c["lb"]) for c in configs}

    log = []        # every dip-condition met bar + decision
    trades = []
    gate_count = {}
    swallowed = {"open-trade": 0, "sl-cooldown": 0}
    open_state = {}   # cfg -> entry index
    last_exit = {c["id"]: -1 for c in configs}
    sl_cooldown_until_ms = 0.0

    for i in range(1, n):
        if n < config.MIN_BARS_ENTER:
            break
        for c in configs:
            if i <= last_exit.get(c["id"], -1):
                continue
            ref_i = refs[c["id"]][i]
            if ref_i != ref_i:
                continue
            if not (close[i] < (1 - c["dip"]) * ref_i):
                continue
            drawdown = 1.0 - close[i] / ref_i
            age, pkj = peak_age_bars(bars, i, c["lb"], ref_i)
            rec = {
                "sym": obj_sym, "cfg": c["id"], "i": i, "t": TS(ts[i]),
                "ref": ref_i, "dd": drawdown, "peak_age": age, "peak_at": pkj,
                "close": close[i],
                "vol": vol[i], "vol_avg30": sum(vol[max(0, i - 30):i]) / max(1, i),
                "pcp1h": (close[i] / close[i - 60] - 1) if i >= 60 else None,
                "mc": mc_now * close[i] / close_last,
            }
            if open_state.get(c["id"]) is not None:
                swallowed["open-trade"] += 1
                rec["decision"] = "open-trade"
                log.append(rec)
                continue
            if sl_cooldown_until_ms and ts[i] < sl_cooldown_until_ms:
                swallowed["sl-cooldown"] += 1
                rec["decision"] = "sl-cooldown"
                log.append(rec)
                continue
            if gates["fresh"] and age is not None and age > config.FRESH_DIP_BARS:
                gate_count["stale-peak"] = gate_count.get("stale-peak", 0) + 1
                rec["decision"] = "stale-peak"
                log.append(rec)
                continue
            if gates["pcp"] and not (
                    isinstance(rec["pcp1h"], (int, float)) and rec["pcp1h"] >= config.MIN_PCP1H_PCT / 100.0):
                gate_count["cold-pcp1h"] = gate_count.get("cold-pcp1h", 0) + 1
                rec["decision"] = "cold-pcp1h"
                log.append(rec)
                continue
            if gates["mc"] and not rec["mc"] >= config.MIN_META_MC:
                gate_count["small-cap"] = gate_count.get("small-cap", 0) + 1
                rec["decision"] = "small-cap"
                log.append(rec)
                continue

            entry = close[i]
            sl_lvl = entry * (1 - c["sl"])
            tp_lvl = entry * (1 + c["tp"])
            rec_lvl = ref_i * (1 - strategy.REC)
            we = min(n, i + 1 + c["hold"]) if c["hold"] else n
            ex = px = None
            why = "end"
            mfe = mae = 0.0
            jwin = jlose = -1
            for j in range(i + 1, we):
                if low[j] <= sl_lvl:
                    px = open_[j] if config.GAP_FILL and open_[j] < sl_lvl else sl_lvl
                    ex, why = j, "sl"
                    break
                if high[j] >= rec_lvl and (config.SELL_ALL_AT_REC or rec_lvl <= tp_lvl):
                    ex, px, why = j, rec_lvl, "rec"
                    break
                if high[j] >= tp_lvl:
                    ex, px, why = j, tp_lvl, "tp"
                    break
            if ex is None and c["hold"] and we == i + 1 + c["hold"]:
                ex, px, why = i + c["hold"], close[i + c["hold"]], "end"
            # excursions over the FULL hold window / to exit
            scan_end = ex if ex is not None else we - 1
            if scan_end > i:
                for j in range(i + 1, scan_end + 1):
                    # rec is the first touched barrier; track best/worst reach
                    best = (high[j] / entry - 1) if ex is None or ex >= j else mfe
                    pass
            for j in range(i + 1, scan_end + 1):
                mfe = max(mfe, high[j] / entry - 1.0)
                mae = min(mae, low[j] / entry - 1.0)
                if ex == j:
                    jwin, jlose = (mfe if why in ("rec", "tp") else mae), 0.0
            gross = (px / entry - 1.0) if ex is not None else None
            net = strategy.net_return(gross, liquidity) if gross is not None else None
            if why == "sl":
                sl_cooldown_until_ms = ts[ex] + config.SL_COOLDOWN_MIN * 60000
            last_exit[c["id"]] = ex if ex is not None else n - 1
            trade = {
                "sym": obj_sym, "cfg": c["id"], "lb": c["lb"],
                "in_i": i, "in_t": TS(ts[i]), "entry": entry,
                "ref_peak": ref_i, "dd": drawdown, "peak_age": age,
                "pcp1h": rec["pcp1h"], "mc": rec["mc"],
                "vol_entry": rec["vol"], "vol_avg30": rec["vol_avg30"],
                "sl_lvl": sl_lvl, "rec_lvl": rec_lvl, "tp_lvl": tp_lvl,
                "out_i": ex, "out_t": TS(ts[ex]) if ex is not None else None,
                "exit_px": px, "why": why,
                "mfe": mfe, "mae": mae,
                "gross": gross, "net": net, "dollars": net * ALLOC if net is not None else None,
                "bars": (ex - i) if ex is not None else None,
            }
            trades.append(trade)
            rec["decision"] = "ENTRY"
            rec["trade_idx"] = len(trades) - 1
            log.append(rec)
    return log, trades, gates, gate_count, swallowed


def summarize(trades):
    s = {"n": len(trades), "sl": 0, "rec": 0, "tp": 0, "end": 0, "net$": 0.0,
         "wr": 0, "open": 0}
    for t in trades:
        s[t["why"]] += 1
        if t["dollars"] is not None:
            s["net$"] += t["dollars"]
    s["wr"] = (s["rec"] + s["tp"]) / s["n"] if s["n"] else 0
    return s


def main():
    with open("_gmgn_24h.json", "r", encoding="utf-8") as f:
        raw = json.load(f)["raw"]
    global obj_sym
    detail = {"signals": [], "trades": [], "swallowed": {}, "gates": {}}
    for addr, o in raw.items():
        if "err" in o or not o.get("bars"):
            continue
        obj_sym = o["s"]
        bars = o["bars"]
        mc_now, liq = META.get(addr, (0.0, 1000.0))
        if not mc_now:
            continue
        log, trades, gates, gc, sw = run_token(bars, mc_now, liq,
                                               {"fresh": True, "pcp": True, "mc": True})
        detail["signals"].extend(log)
        detail["trades"].extend(trades)
        for k, v in gc.items():
            detail["gates"][k] = detail["gates"].get(k, 0) + v
        for k, v in sw.items():
            detail["swallowed"][k] = detail["swallowed"].get(k, 0) + v
    full = summarize(detail["trades"])
    with open("gmgn_replay_detail.json", "w", encoding="utf-8") as f:
        json.dump(detail, f, indent=1, default=str)

    # ---------- report ----------
    print("=" * 74)
    print(f"FULL 24h REPLAY — decisions from {len(detail['signals'])} dip-condition bars")
    print(f"totals: trades={full['n']} rec={full['rec']} sl={full['sl']} tp={full['tp']} "
          f"end={full['end']} WR={full['wr']:.2f} net=${full['net$']:.2f}")
    print(f"gate rejects: " + "  ".join(f"{k}={v}" for k, v in detail['gates'].items()))
    print(f"state swallows: " + "  ".join(f"{k}={v}" for k, v in detail['swallowed'].items()))
    print()

    # ---- configuration split ----
    print("--- config 1 (lb180) vs config 2 (lb240) ---")
    for cf in (1, 2):
        tr = [t for t in detail["trades"] if t["cfg"] == cf]
        s = summarize(tr)
        print(f"  cfg{cf}: trades={s['n']} rec={s['rec']} sl={s['sl']} end={s['end']} "
              f"WR={s['wr']:.2f} net=${s['net$']:.2f}")
    print()

    # ---- trade path table ----
    print("--- every trade, full path ---")
    hdr = ("sym      c  in_t   dd% pa pcp%  mc_k  vol/avg  out_t  why  "
           "entry$  sl$  rec$  mfe% mae% gr%   n$%")
    print(hdr)
    for t in sorted(detail["trades"], key=lambda x: (x["sym"], x["in_i"])):
        print(f"{t['sym']:8} {t['cfg']} {t['in_t']} {t['dd']*100:4.0f} "
              f"{str(t['peak_age']):>3} {t['pcp1h']*100:5.0f} {t['mc']/1000:6.0f} "
              f"{t['vol_entry']/max(1,t['vol_avg30']):5.1f} {t['out_t']} {t['why']:3} "
              f"{t['entry']:.6g} {t['sl_lvl']:.6g} {t['rec_lvl']:.6g} "
              f"{t['mfe']*100:5.1f} {t['mae']*100:5.1f} {t['gross']*100:+6.1f} "
              f"{t['net']*100:+6.1f}")
    print()

    # ---- winners vs losers anatomy ----
    print("--- winner (rec/tp) vs loser (sl) anatomy — means ---")
    w = [t for t in detail["trades"] if t["why"] in ("rec", "tp")]
    l = [t for t in detail["trades"] if t["why"] == "sl"]
    e = [t for t in detail["trades"] if t["why"] == "end"]
    def means(tr):
        if not tr:
            return {}
        return {
            "dd": sum(t["dd"] for t in tr) / len(tr),
            "peak_age": sum((t["peak_age"] or 0) for t in tr) / len(tr),
            "pcp1h": sum((t["pcp1h"] or 0) for t in tr) / len(tr),
            "mc_k": sum(t["mc"] for t in tr) / len(tr) / 1000,
            "vol_ratio": sum(t["vol_entry"] / max(1, t["vol_avg30"]) for t in tr) / len(tr),
            "mfe_hold": sum(t["mfe"] for t in tr) / len(tr),
            "mae_hold": sum(t["mae"] for t in tr) / len(tr),
            "bars": sum(t["bars"] or 0 for t in tr) / len(tr),
        }
    wm, lm, em = means(w), means(l), means(e)
    print(f"  {'':8}{'n':>3}{'dd%':>6}{'pAge':>6}{'pcp%':>7}{'mc_k':>7}{'volR':>6}"
          f"{'MFE%':>7}{'MAE%':>7}{'bars':>6}")
    for name, m in (("rec", wm), ("sl", lm), ("end", em)):
        if not m: continue
        mm = w if name == "rec" else (l if name == "sl" else e)
        print(f"  {name:8}{len(mm):>3}{m['dd']*100:6.0f}{m['peak_age']:6.0f}"
              f"{m['pcp1h']*100:7.0f}{m['mc_k']:7.0f}{m['vol_ratio']:6.1f}"
              f"{m['mfe_hold']*100:7.1f}{m['mae_hold']*100:7.1f}{m['bars']:6.0f}")
    print()

    # ---- gate ablation ----
    print("--- gate ablation on this universe (whole 24h watch) ---")
    ab = {}
    for name, g in (("none", {"fresh": False, "pcp": False, "mc": False}),
                    ("fresh30", {"fresh": True, "pcp": False, "mc": False}),
                    ("pcp10", {"fresh": False, "pcp": True, "mc": False}),
                    ("mc300k", {"fresh": False, "pcp": False, "mc": True}),
                    ("all", {"fresh": True, "pcp": True, "mc": True})):
        trs = []
        for addr, o in raw.items():
            if "err" in o or not o.get("bars"):
                continue
            obj_sym = o["s"]
            mc_now, liq = META.get(addr, (0.0, 1000.0))
            if not mc_now:
                continue
            _log, trades, _g, _gc, _sw = run_token(o["bars"], mc_now, liq, g)
            trs.extend(trades)
        ab[name] = summarize(trs)
    print(f"  {'':10}{'n':>4}{'WR':>6}{'sl':>4}{'rec':>4}{'tp':>4}{'end':>4}{'net$':>9}")
    for name, s in ab.items():
        print(f"  {name:10}{s['n']:>4}{s['wr']:>6.2f}{s['sl']:>4}{s['rec']:>4}"
              f"{s['tp']:>4}{s['end']:>4}{s['net$']:>9.2f}")
    print()

    # ---- time-of-day ----
    print("--- trades by exit hour (UTC) ---")
    from collections import defaultdict
    byh = defaultdict(lambda: [0, 0, 0.0])
    for t in detail["trades"]:
        if t["out_t"]:
            hr = int(t["out_t"].split()[1].split(":")[0])
            byh[hr][0] += 1
            byh[hr][1] += 1 if t["why"] in ("rec", "tp") else 0
            byh[hr][2] += t["dollars"] or 0
    for hr in sorted(byh):
        v = byh[hr]
        print(f"  {hr:02d}:00  n={v[0]} win={v[1]} net=${v[2]:.2f}")

    print()
    print(f"detail JSON written: gmgn_replay_detail.json "
          f"({len(detail['signals'])} signals, {len(detail['trades'])} trades)")


if __name__ == "__main__":
    main()