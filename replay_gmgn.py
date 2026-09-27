"""Replay the freshly-pulled GMGN 24h klines through the LIVE engine logic.

Mirrors engine._entry_signal + _my_exit exactly (ACTIVE configs 1+2, combo
hold 60, SELL_ALL_AT_REC, gap-aware SL, 60m SL cooldown, one open trade per
config), with watch_start = first bar (whole window eligible — user chose this).

Metadata RECONSTRUCTION (live bot snapshots these at discovery; we rebuild them
per signal bar, clearly labeled):
  pcp1h  = close[i] / close[i-60] - 1      (GMGN's own 1h change for that bar)
  mc     = mc_now * close[i] / close_last  (supply-constant approximation)
Fail closed if not measurable (e.g. i < 60 -> no 1h window -> blocked).
"""
import json
import sys
import time

import config
import strategy

# mc_now / liquidity snapshot from the live trending pull (all 35 tokens).
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


def peak_is_stale(bars, i, lb, peak, limit):
    if limit == 0:
        return False
    hi = [b[2] for b in bars]
    lo = max(0, i - lb)
    for j in range(i - 1, lo - 1, -1):
        if hi[j] == peak:
            return (i - j) > limit
    return True


def replay_token(bars, mc_now, liquidity):
    """Mirror engine._entry_signal + _my_exit. Returns (trades, gates)."""
    n = len(bars)
    close = [b[4] for b in bars]
    high = [b[2] for b in bars]
    low = [b[3] for b in bars]
    open_ = [b[1] for b in bars]
    ts = [b[0] for b in bars]
    close_last = close[-1]
    configs = [dict(c) for c in config.FROZEN_CONFIGS
               if not config.ACTIVE_CONFIG_IDS or c["id"] in config.ACTIVE_CONFIG_IDS]
    for c in configs:
        if config.COMBO_HOLD_MIN:
            c["hold"] = config.COMBO_HOLD_MIN
    refs = {c["id"]: strategy.ref_for(high, c["lb"]) for c in configs}

    trades = []
    gates = {"stale-peak": 0, "cold-pcp1h": 0, "small-cap": 0, "warmup-bars": 0}
    open_ts = {}      # config_id -> entry index (one open trade per config)
    last_exit = {c["id"]: -1 for c in configs}
    sl_cooldown_until_ms = 0.0

    for i in range(1, n):
        # warmup is on the bars ingested SO FAR (i+1), not the total series
        # length: live only ever has the bars it has polled. Checking n here let
        # short-history tokens trade on bars 0..MIN_BARS_ENTER-1 (6 phantom
        # trades in the 24h run). See parity_gmgn.py for the certified replay.
        if i + 1 < config.MIN_BARS_ENTER:
            gates["warmup-bars"] += 1
            continue
        for c in configs:
            if i <= last_exit[c["id"]]:
                continue
            if open_ts.get(c["id"]) is not None:
                continue
            if sl_cooldown_until_ms and ts[i] < sl_cooldown_until_ms:
                continue
            ref_i = refs[c["id"]][i]
            if ref_i != ref_i:
                continue
            if not (close[i] < (1 - c["dip"]) * ref_i):
                continue
            if peak_is_stale(bars, i, c["lb"], ref_i, config.FRESH_DIP_BARS):
                gates["stale-peak"] += 1
                continue
            # --- reconstructed metadata gates (fail closed) ---
            if i < 60:
                gates["cold-pcp1h"] += 1
                continue
            pcp1h = close[i] / close[i - 60] - 1.0
            if not (isinstance(pcp1h, (int, float)) and pcp1h >= config.MIN_PCP1H_PCT / 100.0):
                gates["cold-pcp1h"] += 1
                continue
            mc_hist = mc_now * close[i] / close_last
            if not mc_hist >= config.MIN_META_MC:
                gates["small-cap"] += 1
                continue
            # --- open trade (entry at close[i]) ---
            entry = close[i]
            sl_lvl = entry * (1 - c["sl"])
            tp_lvl = entry * (1 + c["tp"])
            rec_lvl = ref_i * (1 - strategy.REC)
            we = min(n, i + 1 + c["hold"]) if c["hold"] else n
            ex = px = None
            why = "end"
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
            # engine._my_exit: full hold window scanned w/o a barrier -> time-stop
            # at the close of bar i+hold (only when we wasn't clamped by n).
            if ex is None and c["hold"] and we == i + 1 + c["hold"]:
                ex, px, why = i + c["hold"], close[i + c["hold"]], "end"
            open_ts[c["id"]] = i
            gross = (px / entry - 1.0) if ex is not None else None
            if why == "sl":
                sl_cooldown_until_ms = ts[ex] + config.SL_COOLDOWN_MIN * 60000
            last_exit[c["id"]] = ex if ex is not None else n - 1
            trades.append({
                "cfg": c["id"], "lb": c["lb"],
                "entry_ts": ts[i], "entry": entry, "ref_peak": ref_i,
                "pcp1h": round(pcp1h * 100, 1), "mc": round(mc_hist),
                "exit_ts": ts[ex] if ex is not None else None,
                "why": why, "gross": gross, "net": strategy.net_return(gross, liquidity) if gross is not None else None,
                "open": ex is None,
            })
            open_ts[c["id"]] = None
    return trades, gates


def main():
    with open("_gmgn_24h.json", "r", encoding="utf-8") as f:
        raw = json.load(f)["raw"]
    tot = {"n": 0, "sl": 0, "rec": 0, "tp": 0, "end": 0, "open": 0, "net": 0.0}
    all_gates = {}
    per_token = []
    for addr, obj in raw.items():
        if "err" in obj or not obj.get("bars"):
            continue
        bars = obj["bars"]
        mc_now, liq = META.get(addr, (0.0, 1000.0))
        if not mc_now:
            continue
        trades, gates = replay_token(bars, mc_now, liq)
        for k, v in gates.items():
            all_gates[k] = all_gates.get(k, 0) + v
        net = sum(t["net"] for t in trades if t["net"] is not None)
        per_token.append((obj["s"], len(bars), trades, net))
        for t in trades:
            tot["n"] += 1
            tot[t["why"]] = tot.get(t["why"], 0) + 1
            if t["net"] is not None:
                tot["net"] += t["net"]
    print(f"tokens={len(per_token)}  gate-blocked signals: "
          + "  ".join(f"{k}={v}" for k, v in all_gates.items()))
    print(f"totals: trades={tot['n']}  sl={tot['sl']} rec={tot['rec']} tp={tot['tp']} "
          f"end={tot['end']} still_open={tot['open']}  net=${tot['net']:.2f}")
    for sym, nb, trades, net in sorted(per_token, key=lambda x: -x[3]):
        if not trades:
            print(f"{sym:12} bars={nb:5d}  no qualifying trades")
        else:
            detail = "; ".join(
                f"#{t['cfg']} lb{t['lb']} {time.strftime('%H:%M', time.gmtime(t['entry_ts']/1000))}"
                f"->{time.strftime('%H:%M', time.gmtime(t['exit_ts']/1000)) if t['exit_ts'] else 'OPEN'}"
                f" {t['why']} g{t['gross']*100:+.1f}% n{t['net']*100:+.1f}%"
                f" pcp{t['pcp1h']:+.0f} mc{t['mc']:.0f}"
                for t in trades)
            print(f"{sym:12} bars={nb:5d} net={net*100:+.1f}%  {detail}")


if __name__ == "__main__":
    main()