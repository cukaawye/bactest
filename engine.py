"""Vapor engine: candidate lifecycle + incremental causal bar scanning.

Methodology: prospective only. At decision time Vapor uses only bars with
market_ts <= observed_at. Each (token, config) independently:
  1. signal bar i: close[i] < (1-dip)*ref[i] (ref = rolling high over lb, shifted 1)
  2. entry at close[i]
  3. scan subsequent bars for SL -> rec -> TP; hold timeout closes at close[i+1+hold];
     if hold=0, trade stays OPEN until a barrier fires or token expires.
One virtual paper trade per qualifying config (never deduped/priority-ordered).

Causal processing of newly arrived bars mirrors analysis/common.walk:
for each new bar (in market order): resolve exits for open trades first, then
evaluate entry signals on that bar. A trade opened on bar i scans exits from
bar i+1 onward (never same bar), and a signal on a bar already covered by an
open trade is skipped (walk's `pos` cooldown).
"""

import time

import config
import strategy


class TokenState:
    def __init__(self, addr, db, gmgn, watch_start_ms=0):
        self.addr = addr
        self.db = db
        self.gmgn = gmgn
        self.watch_start_ms = watch_start_ms  # earliest market_ts eligible for entries
        self.bars = []          # list of (market_ts, open, high, low, close, volume)
        self.configs = [dict(c) for c in config.FROZEN_CONFIGS]
        self.state = "DISCOVERED"
        self.open_trade_ids = {}   # config_id -> db trade id
        self.last_exit_idx = {c["id"]: -1 for c in self.configs}
        self.last_eval_idx = -2   # last bar index evaluated (setup detection passthrough)
        self.last_trigger_idx = {c["id"]: None for c in self.configs}

    # ---- data feed ----
    def ingest(self, bars):
        new = 0
        for b in bars:
            ts = b[0]
            if self.bars and ts <= self.bars[-1][0]:
                continue
            if any(x[0] == ts for x in self.bars):
                continue
            self.bars.append(b)
            new += 1
        return new

    def refresh_history(self):
        end = time.time()
        begin = end - config.KLINE_HISTORY_SECONDS
        try:
            bars = self.gmgn.klines(self.addr, begin, end)
        except Exception as e:
            self.db.event("error", self.addr, payload={"msg": str(e)})
            return False
        self.ingest(bars)
        return True

    # ---- strategy ----
    def _ref_for(self, lb):
        highs = [b[2] for b in self.bars]
        return strategy.ref_for(highs, lb)

    def _my_exit(self, cfg, open_trade_idx, entry_price, ref_peak):
        """Causal scan for one open trade on the CURRENT series, matching
        walk()'s SL -> rec -> TP -> (hold "end") ordering exactly.
        Returns (exit_idx, exit_price, why) or (None, None, None) if still open.
        - hold>0: at most bars i+1 .. i+hold are scanned; if none breach in the
          full hold window, trades time out at the close of bar i+hold.
        - hold=0: scan the whole current series; no timeout (stays OPEN).
        """
        c = [b[4] for b in self.bars]
        h = [b[2] for b in self.bars]
        l = [b[3] for b in self.bars]
        n = len(self.bars)
        i = open_trade_idx
        sl_lvl = entry_price * (1 - cfg["sl"])
        tp_lvl = entry_price * (1 + cfg["tp"])
        rec_lvl = ref_peak * (1 - strategy.REC)
        we = min(n, i + 1 + cfg["hold"]) if cfg["hold"] else n
        j = i + 1
        while j < we:
            if l[j] <= sl_lvl:
                return j, sl_lvl, "sl"
            if h[j] >= rec_lvl and rec_lvl <= tp_lvl:
                return j, rec_lvl, "rec"
            if h[j] >= tp_lvl:
                return j, tp_lvl, "tp"
            j += 1
        if cfg["hold"] and we == i + 1 + cfg["hold"]:
            # scanned the FULL hold window without a barrier (we not clamped by n)
            return i + cfg["hold"], c[i + cfg["hold"]], "end"
        return None, None, None

    def _barrier_close(self, cfg, decision_ts, decision_ms):
        tid = self.open_trade_ids.get(cfg["id"])
        if not tid:
            return
        row = self.db.conn.execute(
            "SELECT entry_ts, entry_price, ref_peak FROM paper_trades WHERE id=?", (tid,)).fetchone()
        if not row:
            return
        ents = [b[0] for b in self.bars]
        try:
            eidx = ents.index(row[0])
        except ValueError:
            return
        ex, px, why = self._my_exit(cfg, eidx, row[1], row[2])
        if ex is not None:
            self._close_paper(tid, cfg, ex, px, why, decision_ts, decision_ms)

    def _close_paper(self, tid, cfg, ex, px, why, decision_ts, decision_ms):
        trow = self.db.conn.execute(
            "SELECT token_address, config_id, entry_price, liquidity FROM paper_trades WHERE id=?",
            (tid,)).fetchone()
        entry_price, liquidity = trow[2], trow[3]
        gross = px / entry_price - 1.0
        net = strategy.net_return(gross, liquidity)
        self.db.close_trade(tid, self.bars[ex][0], px, why, gross, net)
        addr = trow[0]
        self.open_trade_ids.pop(cfg["id"], None)
        self.last_exit_idx[cfg["id"]] = ex
        self.db.event("paper_exit", addr, cfg["id"], {
            "config_id": cfg["id"], "exit_ts": self.bars[ex][0], "exit_price": px,
            "reason": why, "gross_ret": gross, "net_ret": net,
            "observed_at": decision_ts * 1000})
        row = self.db.candidate(addr)
        if row and row[8] == "PAPER_TRADE":
            open_remaining = self.db.open_trades(addr)
            self.db.set_candidate_state(addr, "EXITED" if not open_remaining else row[7])

    def _entry_signal(self, cfg, decision_ts, decision_ms, i=None):
        n = len(self.bars)
        if n < 2:
            return
        i = n - 1 if i is None else i
        refs = self._ref_for(cfg["lb"])
        if i <= self.last_exit_idx.get(cfg["id"], -1):
            return
        market_ts = self.bars[i][0]
        close_i = self.bars[i][4]
        ref_i = refs[i]
        if ref_i != ref_i:  # NaN
            self.db.row_eval(self.addr, cfg, market_ts, decision_ts, decision_ms,
                             close_i, ref_i, None, False, "no-ref")
            return
        if len(self.bars) < config.MIN_BARS_ENTER:
            # mirror the research entrance gate (backtest required >= 122 bars
            # before a token enters any config); partial windows below it never
            # traded in the frozen baseline, so Vapor doesn't either.
            self.db.row_eval(self.addr, cfg, market_ts, decision_ts, decision_ms,
                             close_i, ref_i, None, False, "warmup-bars")
            return
        if market_ts < self.watch_start_ms:
            # bar completed before we started watching: lookback/warm-up only,
            # must NEVER trigger a paper entry (prospective method).
            self.db.row_eval(self.addr, cfg, market_ts, decision_ts, decision_ms,
                             close_i, ref_i, None, False, "pre-watch")
            return
        if self.open_trade_ids.get(cfg["id"]):
            self.db.row_eval(self.addr, cfg, market_ts, decision_ts, decision_ms,
                             close_i, ref_i, None, False, "open-trade")
            return
        drawdown = 1.0 - close_i / ref_i
        cond = close_i < (1 - cfg["dip"]) * ref_i
        if not cond:
            self.db.row_eval(self.addr, cfg, market_ts, decision_ts, decision_ms,
                             close_i, ref_i, drawdown, False, "no-signal")
            return
        self.db.row_eval(self.addr, cfg, market_ts, decision_ts, decision_ms,
                         close_i, ref_i, drawdown, True, "signal")
        self.db.event("setup_detected", self.addr, cfg["id"], {
            "market_ts": market_ts, "observed_at": decision_ms,
            "close": close_i, "ref_peak": ref_i, "drawdown": drawdown, "dip": cfg["dip"]})
        self.db.event("strategy_evaluated", self.addr, cfg["id"], {
            "market_ts": market_ts, "observed_at": decision_ms,
            "dip": cfg["dip"], "tp": cfg["tp"], "sl": cfg["sl"], "lb": cfg["lb"],
            "hold": cfg["hold"], "qualified": True})
        self._open_paper(cfg, i, close_i, ref_i, drawdown, decision_ts, decision_ms)

    def _open_paper(self, cfg, i, close_i, ref_i, drawdown, decision_ts, decision_ms):
        cand = self.db.candidate(self.addr)
        liquidity = (cand[7] or 1000.0) if cand else 1000.0
        tid = self.db.open_trade(self.addr, cfg, self.bars[i][0], decision_ts, decision_ms,
                                 close_i, ref_i, drawdown, liquidity)
        self.open_trade_ids[cfg["id"]] = tid
        self.last_trigger_idx[cfg["id"]] = i
        if self.db.candidate(self.addr):
            self.db.set_candidate_state(self.addr, "PAPER_TRADE")
        self.state = "PAPER_TRADE"
        self.db.event("paper_entry", self.addr, cfg["id"], {
            "config_id": cfg["id"], "entry_ts": self.bars[i][0],
            "observed_at": decision_ms, "decision_ts": decision_ts,
            "entry_price": close_i, "ref_peak": ref_i, "drawdown": drawdown,
            "liquidity": liquidity, "dip": cfg["dip"], "tp": cfg["tp"],
            "sl": cfg["sl"], "lb": cfg["lb"]})

    def process_new_bars(self, decision_ts=None):
        """Evaluate newly-ingested bars causally, in market order.
        Order within a bar: resolve exits (open trades) then entries, matching walk."""
        decision_ts = decision_ts or time.time()
        decision_ms = decision_ts * 1000
        lo = self.last_eval_idx + 1
        if lo < 0:
            lo = 0
        for i in range(lo, len(self.bars)):
            for cfg in self.configs:
                if self.open_trade_ids.get(cfg["id"]):
                    self._barrier_close(cfg, decision_ts, decision_ms)
            for cfg in self.configs:
                if not self.open_trade_ids.get(cfg["id"]):
                    self._entry_signal(cfg, decision_ts, decision_ms, i)
            self.last_eval_idx = i

    # ---- lifecycle ----
    def mark(self, state):
        self.state = state
        self.db.set_candidate_state(self.addr, state)
        self.db.event(state.lower(), self.addr, payload={"state": state})

    def expire(self):
        # close any open trades at last known price (no market data -> end exit)
        for cfg in self.configs:
            tid = self.open_trade_ids.get(cfg["id"])
            if not tid:
                continue
            row = self.db.conn.execute(
                "SELECT entry_price, liquidity FROM paper_trades WHERE id=?", (tid,)).fetchone()
            if not row or not self.bars:
                continue
            last = self.bars[-1]
            self._close_paper(tid, cfg, len(self.bars) - 1, last[4], "end",
                              time.time(), time.time() * 1000)
        self.db.set_candidate_state(self.addr, "EXPIRED")
        self.state = "EXPIRED"
        self.db.event("candidate_expired", self.addr, payload={"state": "EXPIRED"})