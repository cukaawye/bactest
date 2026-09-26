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