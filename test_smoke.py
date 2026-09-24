"""Vapor Phase 1 smoke tests. Run: python test_smoke.py
Covers: configs, causal entry, SL/rec/TP/timeout ordering, ambiguous candle,
simultaneous triggers, restart recovery, gmgn retry/parse, dry-run discipline,
and parity of strategy.walk vs analysis.common.walk on real harvested data.
"""
import json
import os
import sys
import tempfile
import time
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import db as dbmod
import engine
import gmgn
import strategy
import live


T = []
def test(fn):
    T.append(fn)
    return fn


def bar(ts, o, h, l, c, v=0.0):
    return (ts, o, h, l, c, v)


def mk_series(n, high, dip_cfg_open=None):
    """n bars (min MIN_BARS_ENTER to clear the research entrance gate), flat at
    `high`; optional final dip bar."""
    n = max(n, config.MIN_BARS_ENTER)
    base = int(time.time() * 1000)
    bars = [bar(base + i * 60000, high, high, high, high) for i in range(n)]
    if dip_cfg_open is not None:
        c = dip_cfg_open
        bars.append(bar(base + n * 60000, high, high, c, c))
    return bars


# ---- data feed / discovery ----
@test
def test_trending_parse():
    class F(gmgn.GMGN):
        def _request(self, method, url, body=None):
            return {"code": 0, "data": [{"interval": "6h", "chain": "sol", "filter_id": "f-123",
                "tokens": [
                    {"a": "abc123", "s": "TST", "nm": "Test", "p": "0.5", "lq": "12000", "mc": "9e5"},
                    {"a": None},  # malformed row to skip
                    {"a": "def456", "s": "", "nm": "NoSym", "p": 1.2, "lq": 0},
                ]}]}
    g = F()
    g.requests = 0
    out = g.trending()
    assert g.last_filter_id == "f-123", g.last_filter_id
    assert len(out) == 2, out
    assert out[0]["symbol"] == "TST" and out[0]["liquidity"] == 12000.0
    assert out[1]["symbol"] == "" and out[1]["liquidity"] == 0.0


@test
def test_trending_nonzero_code_raises():
    class F(gmgn.GMGN):
        def _request(self, method, url, body=None):
            return {"code": 1}
    try:
        F().trending()
        assert False, "expected GMGNError"
    except gmgn.GMGNError:
        pass


@test
def test_klines_parses_ms_and_sort():
    class F(gmgn.GMGN):
        def _request(self, method, url, body=None):
            return {"code": 0, "data": [
                {"time": "300000", "open": "1", "high": "2", "low": "0", "close": "1.5", "volume": "10"},
                {"time": "100000", "open": "1", "high": "1", "low": "1", "close": "1", "volume": "5"},
                {"time": "junk", "open": "1", "high": "1", "low": "1", "close": "1", "volume": "1"},
            ]}
    bars = F().klines("addr", 0, 1000)
    assert [b[0] for b in bars] == [100000, 300000], bars  # sorted, ms kept, junk row skipped


@test
def test_token_info_batches():
    seen = []
    class F(gmgn.GMGN):
        def _request(self, method, url, body=None):
            seen.append(len(body["addresses"]))
            return {"code": 0, "data": [{"address": a} for a in body["addresses"]]}
    out = F().token_info(list("abcdefgh"))
    assert seen == [6, 2] and len(out) == 8, seen


@test
def test_gress_retry_then_success():
    calls = {"n": 0}
    import urllib.request as ur
    real = ur.urlopen
    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3:
            raise urllib.error.HTTPError(req.full_url, 429, "slow down", None, None)
        class R:
            def read(self): return json.dumps({"code": 0, "data": [{"tokens": []}]}).encode()
            def __enter__(self): return self
            def __exit__(self, *a): return False
        return R()
    ur.urlopen = fake_urlopen
    try:
        g = gmgn.GMGN(); g.sleep = 0.0
        g.trending()
    finally:
        ur.urlopen = real
    assert g.requests == 1 and g.failures == 2 and calls["n"] == 3, (g.requests, g.failures, calls["n"])


@test
def test_exhausted_retries_raise():
    import urllib.request as ur
    real = ur.urlopen
    def fake_urlopen(req, timeout=None):
        raise urllib.error.HTTPError(req.full_url, 429, "slow down", None, None)
    ur.urlopen = fake_urlopen
    try:
        g = gmgn.GMGN(); g.sleep = 0.0
        try:
            g.trending(body={"meta": {}, "params": []})
            assert False, "expected GMGNError after retries"
        except gmgn.GMGNError:
            pass
    finally:
        ur.urlopen = real


@test
def test_ingest_dedupes_and_orders():
    st = engine.TokenState("addr", None, None)
    s = mk_series(3, 1.0)
    assert st.ingest(s) == config.MIN_BARS_ENTER
    assert st.ingest([s[1]]) == 0          # duplicate ts
    assert st.ingest([bar(s[-1][0] + 60000, 2, 2, 2, 2)]) == 1
    ts = [b[0] for b in st.bars]
    assert ts == sorted(ts)


# ---- strategy math ----
@test
def test_ref_for_matches_pandas():
    import numpy as np
    rng = np.random.RandomState(42)
    h = rng.rand(500).astype(np.float64)
    for lb in (10, 120, 240, 360):
        mine = strategy.ref_for(h, lb)
        pd = strategy._pandas_ref_equal(mine, h, lb)
        assert np.allclose(np.nan_to_num(mine), np.nan_to_num(pd), atol=1e-12)
        assert np.isnan(mine[0])


@test
def test_each_frozen_config_fires_and_so_is_unique():
    for cfg in config.FROZEN_CONFIGS:
        s = mk_series(cfg["lb"] + 10, 1.0)
        if cfg["dip"] == 0.25:
            s.append(bar(s[-1][0] + 60000, 1.0, 1.0, 0.70, 0.70))
        else:
            s.append(bar(s[-1][0] + 60000, 1.0, 1.0, 0.66, 0.66))
        ref = strategy.ref_for([b[3] for b in s], cfg["lb"])
        sigs = strategy.sigs_for([b[4] for b in s], ref, cfg["dip"])
        # unique global: each frozen config has a distinct parameter tuple
        key = (cfg["lb"], cfg["dip"], cfg["tp"], cfg["sl"], cfg["hold"])
        assert sum(1 for c in config.FROZEN_CONFIGS if (c["lb"], c["dip"], c["tp"], c["sl"], c["hold"]) == key) == 1
        assert len(sigs) >= 1, f"config {cfg['id']} never signals on crafted dip"


@test
def test_simultaneous_triggers_allow_independent_configs():
    # two different configs dip on the same bar: both may trade (no dedup across configs)
    cfg_a = {"id": 999, "lb": 10, "dip": 0.25, "tp": 1.00, "sl": 0.50, "hold": 0}
    cfg_b = {"id": 998, "lb": 10, "dip": 0.30, "tp": 1.00, "sl": 0.50, "hold": 0}
    s = mk_series(20, 1.0)
    c = 0.66   # below both 0.25 and 0.30 refs
    s.append(bar(s[-1][0] + 60000, 1.0, 1.0, c, c))
    db = dbmod.DB(temp_db())
    st = engine.TokenState("addrx", db, None)
    st.configs = [cfg_a, cfg_b]
    st.ingest(s)
    st.process_new_bars(decision_ts=time.time())
    assert len(st.open_trade_ids) == 2, st.open_trade_ids


@test
def test_tp_exit():
    db = dbmod.DB(temp_db())
    st = engine.TokenState("addr", db, None)
    st.configs = [{"id": 900, "lb": 3, "dip": 0.25, "tp": 0.5, "sl": 0.5, "hold": 0}]
    s = mk_series(6, 1.0)
    sig = bar(s[-1][0] + 60000, 1.0, 1.0, 0.6, 0.6)       # signal (0.6 < 0.75)
    tpbar = bar(sig[0] + 60000, 0.6, 2.0, 0.55, 1.9)       # high >= tp(0.9)
    st.ingest(s + [sig])
    base = time.time()
    st.process_new_bars(base)
    assert len(st.open_trade_ids) == 1
    st.ingest([tpbar])
    st.process_new_bars(base + 1)
    row = db.conn.execute("SELECT state, exit_reason FROM paper_trades").fetchone()
    assert row[0] == "CLOSED" and row[1] == "tp", row


@test
def test_sl_exit_with_rec_priority_order():
    # one bar breaches rec a/o tp AND sl: SL wins (barrel scan order sl->rec->tp)
    db = dbmod.DB(temp_db())
    st = engine.TokenState("addr", db, None)
    st.configs = [{"id": 901, "lb": 3, "dip": 0.25, "tp": 0.5, "sl": 0.5, "hold": 0}]
    s = mk_series(6, 1.0)
    sig = bar(s[-1][0] + 60000, 1.0, 1.0, 0.6, 0.6)       # signal (entry 0.6)
    slbar = bar(sig[0] + 60000, 0.6, 3.0, 0.2, 2.5)      # low<=sl(0.3) AND high>=tp(0.9)
    st.ingest(s + [sig])
    st.process_new_bars(time.time())
    st.ingest([slbar])
    st.process_new_bars(time.time() + 1)
    row = db.conn.execute("SELECT state, exit_reason FROM paper_trades").fetchone()
    assert row[1] == "sl", row


@test
def test_rec_fires_before_tp_when_active():
    db = dbmod.DB(temp_db())
    st = engine.TokenState("addr", db, None)
    # tp=1.00 keeps rec active (rec_lvl 0.95 <= tp 1.6); high must not hit tp first
    st.configs = [{"id": 902, "lb": 3, "dip": 0.25, "tp": 1.00, "sl": 0.5, "hold": 0}]
    s = mk_series(6, 1.0)
    sig = bar(s[-1][0] + 60000, 1.0, 1.0, 0.6, 0.6)       # entry 0.6, ref 1.0, rec 0.95
    recbar = bar(sig[0] + 60000, 0.6, 0.96, 0.55, 0.8)     # high>=0.95, no sl, no tp(1.6)
    st.ingest(s + [sig])
    st.process_new_bars(time.time())
    st.ingest([recbar])
    st.process_new_bars(time.time() + 1)
    row = db.conn.execute("SELECT state, exit_reason, exit_price FROM paper_trades").fetchone()
    assert row[1] == "rec" and abs(row[2] - 0.95) < 1e-9, row


@test
def test_hold_timeout_and_hold0_open():
    db = dbmod.DB(temp_db())
    st = engine.TokenState("addr", db, None)
    st.configs = [
        {"id": 910, "lb": 3, "dip": 0.25, "tp": 1.00, "sl": 0.5, "hold": 3},   # times out
        {"id": 911, "lb": 3, "dip": 0.25, "tp": 1.00, "sl": 0.5, "hold": 0},   # stays open
    ]
    s = mk_series(6, 1.0)
    sig = bar(s[-1][0] + 60000, 1.0, 1.0, 0.6, 0.6)                        # sig idx 6
    st.ingest(s + [sig])
    base = time.time()
    st.process_new_bars(base)
    assert set(st.open_trade_ids) == {910, 911}, st.open_trade_ids
    # feed flat bars one at a time; 910 must time out on its 3rd bar (idx 9)
    for k in range(1, 5):
        st.ingest([bar(st.bars[-1][0] + 60000, 0.6, 0.7, 0.55, 0.65)])
        st.process_new_bars(base + k)
    row_ids = {r[0]: r for r in db.conn.execute(
        "SELECT config_id,state,exit_reason,exit_ts FROM paper_trades").fetchall()}
    assert row_ids[910][1] == "CLOSED" and row_ids[910][2] == "end", row_ids
    assert row_ids[911][1] == "OPEN", row_ids


@test
def test_overlap_config_cooldown_single_config_never_opens_while_owned():
    # same config can't open a 2nd trade while one is open (mirrors walk's cooldown)
    db = dbmod.DB(temp_db())
    st = engine.TokenState("addr", db, None)
    cfg = {"id": 920, "lb": 3, "dip": 0.25, "tp": 1.00, "sl": 0.5, "hold": 0}
    st.configs = [cfg]
    s = mk_series(6, 1.0)
    s.append(bar(s[-1][0] + 60000, 1.0, 1.0, 0.6, 0.6))       # sig
    s.append(bar(s[-1][0] + 60000, 0.6, 0.6, 0.4, 0.5))       # still < 0.75 (another sig, ref=1.0)
    st.ingest(s)
    st.process_new_bars(time.time())
    assert len(st.open_trade_ids) == 1
    st.process_new_bars(time.time() + 1)
    assert len(st.open_trade_ids) == 1, "opened overlapping same-config trade"


# ---- lifecycle / restart ----
@test
def test_exit_drops_state_to_exited_and_expire_closes():
    db = dbmod.DB(temp_db())
    addr = "addrexpire"
    db.upsert_candidate(addr, "EXP", "Exp", 1.0, 10000.0, {})
    st = engine.TokenState(addr, db, None)
    st.configs = [{"id": 930, "lb": 3, "dip": 0.25, "tp": 1.00, "sl": 0.5, "hold": 0}]
    s = mk_series(6, 1.0)
    s.append(bar(s[-1][0] + 60000, 1.0, 1.0, 0.6, 0.6))
    s.append(bar(s[-1][0] + 60000, 0.6, 2.0, 0.55, 1.9))
    st.ingest(s)
    st.mark("MONITORING")
    st.process_new_bars(time.time())
    assert len(st.open_trade_ids) == 0, "trade should exit on the rec bar"
    assert db.candidate(addr)[8] == "EXITED"
    row = db.conn.execute("SELECT state FROM paper_trades").fetchone()
    assert row[0] == "CLOSED"
    # reopen + expire before any barrier -> closed as "end" at last close
    s2 = mk_series(6, 1.0)
    s2.append(bar(s2[-1][0] + 60000, 1.0, 1.0, 0.6, 0.6))
    st2 = engine.TokenState(addr, db, None)
    st2.configs = st.configs
    st2.ingest(s2)
    st2.process_new_bars(time.time())
    st2.expire()
    r = db.conn.execute("SELECT state,exit_reason FROM paper_trades WHERE exit_reason='end'").fetchone()
    assert r is not None and r[1] == "end"
    assert db.candidate(addr)[8] == "EXPIRED"


@test
def test_restart_recovery_no_double_trade():
    path = temp_db()
    db = dbmod.DB(path)
    addr = "addrrest"
    db.upsert_candidate(addr, "RST", "Rest", 1.0, 10000.0, {})
    st = engine.TokenState(addr, db, None)
    st.configs = [{"id": 940, "lb": 3, "dip": 0.25, "tp": 1.00, "sl": 0.5, "hold": 0}]
    s = mk_series(7, 1.0)
    s.append(bar(s[-1][0] + 60000, 1.0, 1.0, 0.6, 0.6))       # sig idx 7 -- open trade
    s.append(bar(s[-1][0] + 60000, 0.6, 0.7, 0.5, 0.65))      # still flat
    st.ingest(s)
    db.add_observations(addr, s)
    st.process_new_bars(time.time())
    assert len(st.open_trade_ids) == 1
    db.commit()
    # simulate restart: new DB instance reading the same file
    db2 = dbmod.DB(path)
    gm = gmgn.GMGN()
    states = live.rebuild_state(db2, time.time(), gm)
    st2 = states[addr]
    assert st2.state == "PAPER_TRADE"
    assert st2.open_trade_ids, "open trade lost across restart"
    assert st2.last_eval_idx == len(st2.bars) - 1
    pre = db2.conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0]
    st2.process_new_bars(time.time() + 10)     # no new data -> no new trades
    post = db2.conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0]
    assert pre == post, "double-traded after restart"


@test
def test_dry_run_records_no_trades():
    path = temp_db()
    db = dbmod.DB(path)
    addr = "addrdry"
    db.upsert_candidate(addr, "DRY", "Dry", 1.0, 10000.0, {})
    st = engine.TokenState(addr, db, None)
    st.configs = [{"id": 950, "lb": 3, "dip": 0.25, "tp": 1.00, "sl": 0.5, "hold": 0}]
    s = mk_series(6, 1.0)
    s.append(bar(s[-1][0] + 60000, 1.0, 1.0, 0.6, 0.6))       # would be a signal
    st.ingest(s)
    db.add_observations(addr, s)
    # dry-run discipline: never call process_new_bars, only record observations
    n = db.conn.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0]
    assert n == 0


@test
def test_expiry_no_open_trades_marks_expired():
    db = dbmod.DB(temp_db())
    addr = "addrstale"
    db.upsert_candidate(addr, "STL", "Stale", 1.0, 10000.0, {})
    st = engine.TokenState(addr, db, None)
    st.mark("MONITORING")
    st.bars = [bar(int(time.time() * 1000) - 5 * 60000, 1, 1, 1, 1)]
    st.last_eval_idx = 0
    st.expire()
    assert db.candidate(addr)[8] == "EXPIRED"


# ---- parity vs research engine (real data) ----
def _load_parity():
    """Return chunks of (addr, c, h, l, ts_ms) from gp/klines_part*.json."""
    root = os.path.dirname(os.path.abspath(__file__))
    gp = os.path.join(root, "..", "gp")
    import datetime as dt
    out = []
    for fn in ("klines_part1.json", "klines_part2.json", "klines_part3.json", "klines_part4.json"):
        p = os.path.join(gp, fn)
        if not os.path.isfile(p):
            continue
        with open(p) as f:
            data = json.load(f)
        for addr, rows in data.items():
            if not addr or not rows:
                continue
            c, h, l, ts = [], [], [], []
            for line in rows[:800]:  # cap memory; plenty for parity
                if isinstance(line, list):
                    line = ",".join(line)
                try:
                    tstr = line.split(",")[0]
                    o, hi, lo, cl, v, amt = line.split(",")[1:7]
                    ts.append(int(dt.datetime.fromisoformat(
                        tstr.replace("Z", "+00:00")).timestamp() * 1000))
                    c.append(float(cl)); h.append(float(hi)); l.append(float(lo))
                except (ValueError, IndexError):
                    continue
            if len(c) >= 60:
                out.append((addr, c, h, l, ts))
    return out


@test
def test_pre_watch_and_warmup_bars_never_trigger_entries():
    # warm-up lookback bars (complete before we started watching) must not open
    # paper trades; only bars closing at/after watch_start AND with enough
    # entrance history may trigger.
    db = dbmod.DB(temp_db())
    addr = "addrpw"
    db.upsert_candidate(addr, "PW", "PW", 1.0, 10000.0, {})
    base = int(time.time() * 1000)
    # full warm-up series + dip bar ALL before watch start (entrance too short)
    s = mk_series(config.MIN_BARS_ENTER + 4, 1.0)
    sig = bar(s[-1][0] + 60000, 1.0, 1.0, 0.6, 0.6)     # idx MIN_BARS_ENTER+4
    watch = bar(sig[0] + 30000, 1, 1, 1, 1)[0]          # watch start strictly after sig ts
    # short-history token: also below entrance gate
    st = engine.TokenState(addr, db, None, watch_start_ms=watch)
    cfg = {"id": 960, "lb": 3, "dip": 0.25, "tp": 0.5, "sl": 0.5, "hold": 0}
    st.configs = [cfg]
    st.ingest(s + [sig])
    st.process_new_bars(time.time())
    assert len(st.open_trade_ids) == 0, "pre-watch/warmup bar triggered a trade"
    reasons = db.conn.execute("SELECT DISTINCT reason FROM evaluations").fetchall()
    assert ("pre-watch",) in reasons, reasons
    # now a bar closing AFTER watch_start with a dip AND enough bars must trigger
    post = bar(sig[0] + 60000, 1.0, 1.0, 0.6, 0.6)
    st.ingest([post])
    st.process_new_bars(time.time() + 1)
    assert len(st.open_trade_ids) == 1, st.open_trade_ids


@test
def test_refresh_history_drops_in_progress_bar():
    # GMGN returns the current in-progress minute with a provisional close; the
    # engine must never evaluate it (a backtest only sees closed bars).
    db = dbmod.DB(temp_db())
    addr = "hotbar"
    db.upsert_candidate(addr, "HOT", "HOT", 1.0, 10000.0, {})
    base = int(time.time() * 1000) // 60000 * 60000
    closed = [bar(base - (5 - k) * 60000, 1.0, 1.0, 1.0, 1.0) for k in range(5)]
    inprog = bar(base, 1.0, 1.0, 0.4, 0.4)  # would-be signal candle, still forming

    class FakeGMGN:
        def klines(self, addr, begin, end):
            return closed + [inprog]

    st = engine.TokenState(addr, db, FakeGMGN(), watch_start_ms=base)
    assert st.refresh_history()
    assert len(st.bars) == 5 and st.bars[-1][0] < base, "in-progress bar ingested"


@test
def test_fresh_dip_gate_blocks_stale_peaks():
    # Vapor-only cohort guard (deviation from walk()): a dip signal whose ref peak
    # formed > FRESH_DIP_BARS ago is a post-apex bleeder -> skip (reason stale-peak).
    base = int(time.time() * 1000)
    cfg = {"id": 960, "lb": 120, "dip": 0.25, "tp": 0.5, "sl": 0.5, "hold": 0}

    def run(peak_bar_idx):
        db = dbmod.DB(temp_db())
        addr = "freshtest"
        db.upsert_candidate(addr, "FD", "FD", 1.0, 10000.0, {})
        s = [bar(base + k * 60000, 1.0, 1.0, 1.0, 1.0) for k in range(122)]
        hi = s[peak_bar_idx]
        s[peak_bar_idx] = bar(hi[0], hi[1], 2.0, hi[3], hi[4])  # peak 2.0 here
        sig = bar(base + 122 * 60000, 1.0, 1.0, 0.6, 0.6)       # close 0.6 < 0.75*2.0
        st = engine.TokenState(addr, db, None, watch_start_ms=s[-1][0] + 1000)
        st.configs = [cfg]
        st.ingest(s + [sig])
        st.process_new_bars(time.time())
        reasons = [r[0] for r in db.conn.execute("SELECT DISTINCT reason FROM evaluations")]
        return st.open_trade_ids, reasons

    # peak 60 bars before signal bar -> stale (62 > 30) -> no trade
    open_t, reasons_stale = run(peak_bar_idx=60)
    assert len(open_t) == 0, open_t
    assert "stale-peak" in reasons_stale, reasons_stale
    # peak on the bar right before the signal -> fresh (1 <= 30) -> trade opens
    open_t, reasons_fresh = run(peak_bar_idx=121)
    assert len(open_t) == 1, open_t
    assert "stale-peak" not in reasons_fresh, reasons_fresh


@test
def test_metadata_gate_blocks_cold_tokens():
    # Vapor-only gate: pcp1h (1h price change) below MIN_PCP1H_PCT -> skip (cold-pcp1h)
    base = int(time.time() * 1000)
    cfg = {"id": 960, "lb": 10, "dip": 0.25, "tp": 0.5, "sl": 0.5, "hold": 0}
    s = mk_series(config.MIN_BARS_ENTER, 1.0)                    # 122 flat bars
    sig = bar(base + 122 * 60000, 1.0, 1.0, 0.6, 0.6)            # dip bar 0.6 < 0.75*1.0
    prev = config.MIN_PCP1H_PCT
    try:
        config.MIN_PCP1H_PCT = 0          # gate off -> fresh dip fires (peak is recent)
        db = dbmod.DB(temp_db())
        db.upsert_candidate("coldtest", "CD", "CD", 1.0, 10000.0, {"pcp1h": -50.0})
        st = engine.TokenState("coldtest", db, None, watch_start_ms=s[-1][0] + 1000)
        st.configs = [cfg]
        st.ingest(s + [sig]); st.process_new_bars(time.time())
        assert len(st.open_trade_ids) == 1, st.open_trade_ids
        # gate on (>= 10) -> same setup, cold meta (-50) -> skipped
        config.MIN_PCP1H_PCT = 10
        db = dbmod.DB(temp_db())
        db.upsert_candidate("coldtest", "CD", "CD", 1.0, 10000.0, {"pcp1h": -50.0})
        st = engine.TokenState("coldtest", db, None, watch_start_ms=s[-1][0] + 1000)
        st.configs = [cfg]
        st.ingest(s + [sig]); st.process_new_bars(time.time())
        assert len(st.open_trade_ids) == 0, st.open_trade_ids
        reasons = [r[0] for r in db.conn.execute("SELECT DISTINCT reason FROM evaluations")]
        assert "cold-pcp1h" in reasons, reasons
        # warm meta (+50) passes the gate -> trade fires
        config.MIN_PCP1H_PCT = 10
        db = dbmod.DB(temp_db())
        db.upsert_candidate("coldtest", "CD", "CD", 1.0, 10000.0, {"pcp1h": 50.0})
        st = engine.TokenState("coldtest", db, None, watch_start_ms=s[-1][0] + 1000)
        st.configs = [cfg]
        st.ingest(s + [sig]); st.process_new_bars(time.time())
        assert len(st.open_trade_ids) == 1, st.open_trade_ids
    finally:
        config.MIN_PCP1H_PCT = prev


@test
def test_metadata_gate_blocks_small_caps():
    # Vapor-only gate: mc below MIN_META_MC -> skip (small-cap)
    base = int(time.time() * 1000)
    mk = lambda mc: ({"mc": mc}, {"mc": mc})
    s = mk_series(config.MIN_BARS_ENTER, 1.0)
    sig = bar(base + 122 * 60000, 1.0, 1.0, 0.6, 0.6)
    cfg = {"id": 960, "lb": 10, "dip": 0.25, "tp": 0.5, "sl": 0.5, "hold": 0}
    prev = config.MIN_META_MC
    try:
        config.MIN_META_MC = 0
        db = dbmod.DB(temp_db())
        db.upsert_candidate("mctest", "MC", "MC", 1.0, 10000.0, {"mc": 50000.0})
        st = engine.TokenState("mctest", db, None, watch_start_ms=s[-1][0] + 1000)
        st.configs = [cfg]
        st.ingest(s + [sig]); st.process_new_bars(time.time())
        assert len(st.open_trade_ids) == 1, st.open_trade_ids
        config.MIN_META_MC = 300000
        db = dbmod.DB(temp_db())
        db.upsert_candidate("mctest", "MC", "MC", 1.0, 10000.0, {"mc": 50000.0})
        st = engine.TokenState("mctest", db, None, watch_start_ms=s[-1][0] + 1000)
        st.configs = [cfg]
        st.ingest(s + [sig]); st.process_new_bars(time.time())
        assert len(st.open_trade_ids) == 0, st.open_trade_ids
        reasons = [r[0] for r in db.conn.execute("SELECT DISTINCT reason FROM evaluations")]
        assert "small-cap" in reasons, reasons
        config.MIN_META_MC = 300000
        db = dbmod.DB(temp_db())
        db.upsert_candidate("mctest", "MC", "MC", 1.0, 10000.0, {"mc": 900000.0})
        st = engine.TokenState("mctest", db, None, watch_start_ms=s[-1][0] + 1000)
        st.configs = [cfg]
        st.ingest(s + [sig]); st.process_new_bars(time.time())
        assert len(st.open_trade_ids) == 1, st.open_trade_ids
    finally:
        config.MIN_META_MC = prev


@test
def test_parity_with_research_walk():
    import numpy as np
    rl = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "really_learn")
    if not os.path.isdir(rl):
        raise AssertionError("really_learn not found for parity (expected next to vapor)")
    cwd = os.getcwd()
    try:
        os.chdir(rl)
        sys.path.insert(0, rl)
        from analysis import common
    finally:
        os.chdir(cwd)
    data = _load_parity()
    assert data, "no parity data loaded from gp/klines_part*.json"
    checked = 0
    for cfg in config.FROZEN_CONFIGS:
        for addr, c, h, l, ts in data:
            c = np.asarray(c, dtype=np.float64)
            h = np.asarray(h, dtype=np.float64)
            l = np.asarray(l, dtype=np.float64)
            ref = strategy.ref_for(h, cfg["lb"])
            sigs = strategy.sigs_for(c, ref, cfg["dip"])
            t = {"c": c, "h": h, "l": l, "ts": np.asarray(ts, dtype=np.float64)}
            research = common.walk(t, sigs, ref, cfg["tp"], cfg["sl"], cfg["hold"])  # (ts,ts,gross,why)
            mine = strategy.walk(c, h, l, ref, cfg["dip"], cfg["tp"], cfg["sl"], cfg["hold"])  # (i,i,gross,why)
            assert len(research) == len(mine), f"trade count differs cfg={cfg['id']} addr={addr}: research={len(research)} mine={len(mine)}"
            for rt, mt in zip(research, mine):
                re_ts, re2_ts, re_g, re_why = rt
                # decide already-mutated ref locks: only compare which bars + gross + why
                assert abs(re_g - mt[2]) < 1e-12, f"gross differs cfg={cfg['id']} addr={addr}"
                assert re_why == mt[3], f"why differs cfg={cfg['id']} addr={addr}: {re_why} vs {mt[3]}"
                # entry/exit ts: my indices -> ts
                assert ts[mt[0]] == re_ts and ts[mt[1]] == re2_ts, "entry/exit bar mismatch"
            checked += 1
    assert checked > 0


@test
def test_cost_parity_vs_replay():
    for gross, liq in [(0.1, 10_000.0), (-0.5, 500.0), (0.05, 200_000.0), (0.5, 1000.0)]:
        mine = strategy.net_return(gross, liq)
        c = 0.025 + 0.25 * (min(0.25 * 100.0, 0.5 * liq) / max(liq, 1.0))
        c = min(max(c, 0.025), 0.7)
        assert abs(mine - (gross - c)) < 1e-12, (gross, liq, mine, c)


def temp_db():
    fd, p = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return p


def main():
    os.environ["VAPOR_DB"] = temp_db()
    import pandas as pd, numpy as np  # noqa: F401 (parity needs them importable)
    failures = 0
    for fn in T:
        try:
            fn()
            print(f"  ok   {fn.__name__}")
        except Exception as e:
            failures += 1
            import traceback
            print(f"  FAIL {fn.__name__}: {e}")
            traceback.print_exc()
    print(f"\n{len(T) - failures}/{len(T)} smoke checks passed")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()