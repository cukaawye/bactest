"""Frozen dip-recovery strategy, mirrored EXACTLY from analysis/common.py
(ref_for / sigs_for / walk) so Vapor evaluates the 8 frozen configs with the
same math as the 34,560-config sweep. Parity verified by test_strategy.py
against the real analysis module.

Caveat parity assumptions reproduced precisely:
- ref = rolling max of HIGH over `lb` bars, min_periods=1, shifted by 1
  (pandas rolling(lb,min_periods=1).max().shift(1) -> first element NaN).
- signal bar i qualifies when close[i] < (1-dip)*ref[i] (strict), ref[i] not NaN.
- entry price = close of signal bar i.
- exits scanned from bar i+1, in order SL -> rec -> TP (exactly like pandas/existing).
- rec fires only when rec_lvl <= tp_lvl.
- "end" exit = close of last scanned bar (or the entry bar if no bars follow).
- hold (minutes) = max bars to scan; hold=0 means scan to end of series.
- trade cooldown: after an exit at index k, next signal must be at index > k
  (mirrors `pos` skipping in common.walk).
"""
import numpy as np

# recovery exit level is fixed at 0.05 of reference peak (common.REC)
REC = 0.05


def ref_for(high, lb):
    """Rolling max of high over lb bars, min_periods=1, shifted 1.
    high: 1-d numpy array (list coerced). Returns same-length array; ref[0] = nan.
    Matches pandas rolling(lb,min_periods=1).max().shift(1)."""
    high = np.asarray(high, dtype=np.float64)
    n = len(high)
    ref = np.full(n, np.nan)
    if n == 0:
        return ref
    for i in range(1, n):
        lo = max(0, i - lb)
        ref[i] = high[lo:i].max()
    return ref


def _pandas_ref_equal(impl, high, lb):
    """numpy mirror of pandas rolling(lb,min_periods=1).max().shift(1)."""
    import pandas as pd
    return pd.Series(high).rolling(lb, min_periods=1).max().shift(1).to_numpy()


def sigs_for(close, ref, dip):
    return np.where(close < (1 - dip) * ref)[0]


def walk(close, high, low, ref, dip, tp, sl, hold):
    """Returns list of (entry_idx, exit_idx, gross, why) - indices into series.
    hold in bars (0 = no maximum). Entry at signal bar close, exit priority
    SL -> rec -> TP, fill at level prices. Mirrors common.walk."""
    n = len(close)
    sigs = sigs_for(close, ref, dip)
    trades = []
    pos = 0
    nS = len(sigs)
    while pos < nS:
        i = sigs[pos]
        if np.isnan(ref[i]):
            pos += 1
            continue
        entry = close[i]
        p0 = ref[i]
        sl_lvl = entry * (1 - sl)
        tp_lvl = entry * (1 + tp)
        rec_lvl = p0 * (1 - REC)
        we = min(n, i + 1 + hold) if hold else n
        j = i + 1
        exit_idx = exit_price = None
        why = "end"
        while j < we:
            if low[j] <= sl_lvl:
                exit_idx, exit_price, why = j, sl_lvl, "sl"
                break
            if high[j] >= rec_lvl and rec_lvl <= tp_lvl:
                exit_idx, exit_price, why = j, rec_lvl, "rec"
                break
            if high[j] >= tp_lvl:
                exit_idx, exit_price, why = j, tp_lvl, "tp"
                break
            j += 1
        if exit_idx is None:
            exit_idx = we - 1 if we > i + 1 else i
            if exit_idx < i:
                exit_idx = i
            exit_price = close[exit_idx]
        trades.append((i, exit_idx, exit_price / entry - 1.0, why))
        pos += 1
        while pos < nS and sigs[pos] <= exit_idx:
            pos += 1
    return trades


def net_return(gross, liquidity, cap=100.0, alloc=0.25):
    """Frozen cost model from replay: pos=min(alloc*cap, 0.5*liq), cost clipped."""
    liq = max(liquidity, 1.0)
    pos = min(alloc * cap, 0.5 * liq)
    cost = min(max(0.025 + 0.25 * (pos / liq), 0.025), 0.7)
    return gross - cost