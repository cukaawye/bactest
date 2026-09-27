"""Vapor Phase 1 — research baseline constants. Frozen from
really_learn analysis (best_configs.json / common.py / AUDIT_REPORT.md).
Do not tune these: they are the research baseline.
"""

# ---- the 8 frozen configurations (best_configs.json -> frozen_candidates) ----
# lb = rolling-high lookback (1m bars); dip; tp (fraction of entry; 1.00 = +100%);
# sl (fraction of entry); hold = max-hold minutes (0 = no maximum).
# sl=0.30: live gated run (109 closed) flips -9.20 -> +1.35 and the 15h pre-gate
# run -135.66 -> -35.56 (sl50 was oversized vs +0.40 rec wins; needs 57% WR, we run ~47%).
FROZEN_CONFIGS = [
    {"id": 1,  "lb": 180, "dip": 0.25, "tp": 1.00, "sl": 0.30, "hold": 0},
    {"id": 2,  "lb": 240, "dip": 0.25, "tp": 1.00, "sl": 0.30, "hold": 0},
    {"id": 3,  "lb": 360, "dip": 0.25, "tp": 1.00, "sl": 0.30, "hold": 0},
    {"id": 4,  "lb": 120, "dip": 0.25, "tp": 1.00, "sl": 0.30, "hold": 0},
    {"id": 5,  "lb": 180, "dip": 0.30, "tp": 1.00, "sl": 0.30, "hold": 0},
    {"id": 6,  "lb": 180, "dip": 0.25, "tp": 1.00, "sl": 0.30, "hold": 360},
    {"id": 7,  "lb": 180, "dip": 0.30, "tp": 1.00, "sl": 0.30, "hold": 360},
    {"id": 8,  "lb": 240, "dip": 0.30, "tp": 1.00, "sl": 0.30, "hold": 0},
]
MAX_LB = max(c["lb"] for c in FROZEN_CONFIGS)
# research entrance gate (backtest.py WINDOW=120 -> load_tokens requires >= 122 bars).
# tokens below this are not scored by the frozen baseline; Vapor mirrors it so the
# live paper trades sit in the same population. min_periods=1 still applies above it,
# matching common.ref_for exactly (partial rolling window for lb > bars-1).
MIN_BARS_ENTER = 122
# ponytail: fresh-dip gate (Vapor-only, deviates from walk()): the entry signal's
# reference peak must have formed within this many bars of the signal bar, else the
# token peaked long ago and is a post-apex bleeder (live trending surfaces these and
# they lose ~100% of trades). 0 disables. Peaks older than this -> reason "stale-peak".
FRESH_DIP_BARS = 30
# ponytail: metadata gate (Vapor-only): min 1-hour % price change of the token at
# discovery (metadata snapshot). Live winners had pcp1h +19..+54, losers -19..-94
# (buy strength, not weakness). 0 disables; pcp1h < this -> reason "cold-pcp1h".
MIN_PCP1H_PCT = 10
# ponytail: metadata gate (Vapor-only): min market cap at discovery. Live data
# segregated hard on market cap: mc<50k net -22.2 / WR 14%, mc>=300k net +6.8 /
# WR 79% (same factor as holders + liquidity — big, established tokens survive the
# dip; micro-caps keep bleeding). 0 disables; mc < this -> reason "small-cap".
MIN_META_MC = 300000

# ---- exit mechanics (common.py) ----
REC = 0.05  # recovery-exit level as fraction of entry reference peak
# ponytail: gap-aware SL fills (Vapor-only, deviates from walk()): when a bar
# OPENS below the stop level, the stop gapped through and the real fill is at
# the open, not the optimistic stop level. Live 1m-bar fills showed a mean
# ~4.8pp shortfall vs the exact-stop model on meme sell-offs. False = exact
# stop level (old behavior, matches the research replay exactly).
GAP_FILL = True

# ---- combo policy (the validated live re-entry/exit plan) ----
# partial_sim.py "combo": only configs 1+2 (dip 0.25 twins) ACTIVE, sell-ALL at
# the REC level as soon as it is touched (rec fires even above tp — the tp level
# is never a better exit than a reached rec), a 60-minute time-stop (hold 60),
# and a 60-minute re-entry cooldown per token after any stop-loss (churn killer).
# Replay across all 3 DBs: 1576 -> 562 trades, net -179.90 -> +137.25,
# WR 34.5% -> 65.1%. Deviates from the frozen research baseline on purpose.
SELL_ALL_AT_REC = True        # rec fires whenever high >= rec_lvl (ignore tp order)
# ponytail: trailing exit (Vapor-only). 0 = OFF, old fixed-level behaviour.
# 578-trade certified replay of the 3 historical runs, configs 1+2, fresh30 only:
#   - the 264 SL exits each peaked +20.3% before closing -30.0%  (50pts handed back)
#   - SELL_ALL_AT_REC dumped winners at +30.5% that went on to +108.9%  (78pts)
#   - 62% of trades peaked between +1% and +37%, i.e. BELOW the rec level, so
#     nothing was a sell trigger and all 358 of them booked -30.0%.
# There was no rule that banked anything except three fixed levels. Arm the trail
# once green by TRAIL_ARM, then ride TRAIL_PCT off the running high. A plain
# breakeven arm was tested and is much worse (fit exp -0.7..-2.0%): it turns
# every runner into a scratch, so the arm has to sit well above the noise.
# Certified 3-DB replay of this exact rule set (engine.TokenState, configs 1+2,
# fresh30, hold60, gap fills, 60m cooldown): 447 trades / 203 sl / 171 trail /
# 73 end, trail bucket +$904.81 net (152 pos avg +$6.12 / 19 neg avg -$1.37).
# While the trail is active (TRAIL_ARM > 0) the rec/tp levels are bypassed: with
# dip 0.25 the rec level is always >= +26.7% over entry, i.e. above the arm, so
# rec could never fire before the trail is live anyway. "trail" exits trigger
# the same 60m re-entry cooldown as "sl" exits (a trail stop IS a stop loss).
TRAIL_ARM = 0.0               # 0 = OFF (rec exit as shipped); 0.20 = arm at +20% over entry
TRAIL_PCT = 0.0               # trail width below the running peak; 0.15 = trail 15% off the high
TRAIL_INIT_SL = 0.0           # initial stop before arm; 0.20 = -20%. While the trail is
                              # active, 0 here falls back to 0.20 (and TRAIL_PCT 0 -> 0.15)
                               # so an arm-only misconfiguration can't create a breakeven stop.
# ponytail: initial stop for the combo, overriding the frozen sl=0.30. Replay: a
# -30% stop sits AT the median MAE (-27.4%), so half of all trades get near-stopped
# and the stop ends up defining the loss size rather than limiting it.
COMBO_SL = 0.0                # 0 = use each config's frozen sl; 0.20 = tighten
# cfg1 (lb180) DROPPED. It was NOT "the bleeding config": through the real engine
# cfg1 and cfg2 fire on the SAME bars (fresh30 forces any surviving lb240 peak to
# be <=30 bars old, hence also inside lb180), so cfg1 double-booked cfg2's trades.
# The old "cfg1 28 trades WR 43% / cfg2 11 trades WR 91%" split was a replay-harness
# bug (warmup checked the full series length, not bars ingested). Certified
# cfg2-only, 24h/35 tokens: 10 trades WR 30% -$28.81 (live metadata snapshot).
ACTIVE_CONFIG_IDS = [2]    # subset of FROZEN_CONFIGS the live engine runs
COMBO_HOLD_MIN = 60           # forced hold (minutes) for ACTIVE configs
SL_COOLDOWN_MIN = 60          # minutes without new entries after an SL exit; 0 = off

# ---- cost model (backtest.py / common.py replay) ----
FEE = 0.025
SLIP_FACTOR = 0.25
CAP = 100.0
ALLOC = 0.25

# ---- GMGN discovery (already-researched SOL/6h/frozen filter) ----
GMGN_BASE = "https://gmgn.ai"
TRENDING_MULTICHAIN = GMGN_BASE + "/rrs/api/v1/trending_multichain"
KLINE_URL = GMGN_BASE + "/defi/quotation/v1/tokens/kline/sol/{addr}?resolution=1m&from={begin}&to={end}"
MULTI_INFO = GMGN_BASE + "/mrwapi/v1/multi_token_full_info"

# CAUTION: the exact launchpad_platform list from the research session was never
# persisted. We reconstruct the filter from the research notes; the server returns
# a filter_id we store for provenance. If a stricter list is needed, add it here.
DISCOVERY_FILTER = {
    "filters": ["frozen"],
    "max_created": "2880m",
    "min_liquidity": 10000,
    "min_volume_6h": 130000,
    "min_swaps_6h": 500,
    "limit": 500,
}
DISCOVERY_BODY = {
    "meta": {},
    "params": [{
        "chain": "sol",
        "interval": "6h",
        "filter": DISCOVERY_FILTER,
    }],
}

# ---- cadence / reliability ----
DISCOVERY_SECONDS = 300        # universe refresh
POLL_SECONDS = 30              # per-token price poll
REQUEST_PACING_S = 0.2         # spacing between GMGN requests
# 360m lookback needs >= 360 bars; fetch 420m to be safe:
KLINE_HISTORY_SECONDS = 60 * 420
RETRY_BACKOFF_S = 1.5
RETRY_BACKOFF_MAX_S = 20.0
RETRY_MAX_TRIES = 3
# a candidate with no fresh data longer than this is EXPIRED (only if no open paper trades)
STALE_EXPIRY_SECONDS = 60 * 30
# how many consecutive discovery misses before a token is dropped from active monitoring
MAX_CONSECUTIVE_DISCOVERY_MISSES = 4