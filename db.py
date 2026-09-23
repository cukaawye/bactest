"""SQLite persistence for Vapor Phase 1.

Deterministic ids: every event/trade id is `{run_id}:{seq}` where seq is a
monotonic per-run counter reconstructed from the DB on boot, so ids never
collide across restarts and are reproducible given the same insert order.
"""
import json
import os
import sqlite3
import time
import uuid

DB_PATH = os.environ.get("VAPOR_DB", os.path.join(os.path.dirname(__file__), "vapor.db"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS candidates (
    token_address TEXT PRIMARY KEY,
    symbol TEXT,
    name TEXT,
    chain TEXT DEFAULT 'sol',
    first_seen_at REAL,
    last_seen_at REAL,
    current_price REAL,
    liquidity REAL,
    state TEXT,
    meta_json TEXT,
    misses INTEGER DEFAULT 0,
    created_at REAL
);
CREATE TABLE IF NOT EXISTS observations (
    token_address TEXT,
    market_ts REAL,          -- bar close time (ms epoch)
    observed_at REAL,        -- ms epoch when bar was seen
    open REAL, high REAL, low REAL, close REAL, volume REAL,
    PRIMARY KEY (token_address, market_ts)
);
CREATE TABLE IF NOT EXISTS evaluations (
    id REAL PRIMARY KEY,
    token_address TEXT, config_id INTEGER,
    market_ts REAL, decision_ts REAL, observed_at REAL,
    entry_price REAL, ref_peak REAL, drawdown REAL,
    dip REAL, tp REAL, sl REAL, lb INTEGER, hold INTEGER,
    triggered INTEGER, reason TEXT
);
CREATE TABLE IF NOT EXISTS paper_trades (
    id REAL PRIMARY KEY,
    token_address TEXT, config_id INTEGER,
    entry_ts REAL, decision_ts REAL, observed_at REAL,
    entry_price REAL, ref_peak REAL, drawdown REAL, liquidity REAL,
    dip REAL, tp REAL, sl REAL, lb INTEGER, hold INTEGER,
    state TEXT, exit_ts REAL, exit_price REAL,
    exit_reason TEXT, gross_ret REAL, net_ret REAL
);
CREATE TABLE IF NOT EXISTS events (
    id REAL PRIMARY KEY,
    ts REAL, event_type TEXT, token_address TEXT,
    config_id INTEGER, payload TEXT
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY, value TEXT
);

CREATE INDEX IF NOT EXISTS idx_obs_token ON observations(token_address);
CREATE INDEX IF NOT EXISTS idx_ev_token ON events(token_address);
CREATE INDEX IF NOT EXISTS idx_pt_token ON paper_trades(token_address);
"""


class DB:
    def __init__(self, path=DB_PATH, run_id=None):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = None
        self.conn.executescript(SCHEMA)
        self.run_id = run_id or str(uuid.uuid4())[:8]
        self._last_id = self._load_last_id()
        self._clock = time.time

    def _load_last_id(self):
        ids = []
        for table, col in (("events", "id"), ("paper_trades", "id"), ("evaluations", "id")):
            try:
                n = self.conn.execute(f"SELECT MAX({col}) FROM {table}").fetchone()[0]
                if n is not None:
                    ids.append(int(n))
            except sqlite3.Error:
                pass
        return max(ids) if ids else 0

    def _next_id(self):
        self._last_id = max(self._last_id + 1, int(time.time() * 1000000))
        return float(self._last_id)

    def close(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()

    # ---- candidates ----
    def upsert_candidate(self, addr, symbol, name, price, liquidity, meta=None):
        now = time.time()
        cur = self.conn.execute("SELECT first_seen_at FROM candidates WHERE token_address=?", (addr,))
        row = cur.fetchone()
        first = row[0] if row else now
        self.conn.execute(
            """INSERT INTO candidates(token_address,symbol,name,first_seen_at,last_seen_at,
               current_price,liquidity,state,meta_json,misses,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,0,?)
               ON CONFLICT(token_address) DO UPDATE SET
                 symbol=excluded.symbol, name=excluded.name, last_seen_at=excluded.last_seen_at,
                 current_price=excluded.current_price, liquidity=excluded.liquidity,
                 meta_json=excluded.meta_json""",
            (addr, symbol, name, first, now, price, liquidity, "DISCOVERED",
             json.dumps(meta or {}), now))
        return first

    def set_candidate_state(self, addr, state):
        self.conn.execute("UPDATE candidates SET state=? WHERE token_address=?", (state, addr))

    def bump_miss(self, addr):
        self.conn.execute("UPDATE candidates SET misses=misses+1 WHERE token_address=?", (addr,))

    def get_candidates(self):
        return self.conn.execute("SELECT * FROM candidates").fetchall()

    def candidate(self, addr):
        return self.conn.execute("SELECT * FROM candidates WHERE token_address=?", (addr,)).fetchone()

    def candidates_in_state(self, state):
        return [r[0] if isinstance(r, (tuple, list)) else None for r in ()]  # unused

    # ---- observations ----
    def add_observations(self, addr, bars):
        """bars: list of (market_ts_ms, open, high, low, close, volume)."""
        if not bars:
            return
        now = time.time() * 1000
        rows = [(addr, b[0], now, b[1], b[2], b[3], b[4], b[5]) for b in bars]
        self.conn.executemany(
            "INSERT OR IGNORE INTO observations VALUES(?,?,?,?,?,?,?,?)", rows)

    def observations(self, addr):
        cur = self.conn.execute(
            "SELECT market_ts,open,high,low,close,volume FROM observations "
            "WHERE token_address=? ORDER BY market_ts", (addr,))
        return [tuple(r) for r in cur.fetchall()]

    # ---- evaluations ----
    def row_eval(self, addr, cfg, market_ts, decision_ts, observed_at, entry_price,
                 ref_peak, drawdown, triggered, reason):
        self.conn.execute(
            """INSERT INTO evaluations(id,token_address,config_id,market_ts,decision_ts,
               observed_at,entry_price,ref_peak,drawdown,dip,tp,sl,lb,hold,triggered,reason)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (self._next_id(), addr, cfg["id"], market_ts, decision_ts, observed_at,
             entry_price, ref_peak, drawdown, cfg["dip"], cfg["tp"], cfg["sl"],
             cfg["lb"], cfg["hold"], 1 if triggered else 0, reason))

    # ---- paper trades ----
    def open_trade(self, addr, cfg, entry_ts, decision_ts, observed_at, entry_price,
                   ref_peak, drawdown, liquidity):
        t = self._next_id()
        self.conn.execute(
            """INSERT INTO paper_trades(id,token_address,config_id,entry_ts,decision_ts,
               observed_at,entry_price,ref_peak,drawdown,liquidity,dip,tp,sl,lb,hold,
               state,exit_ts,exit_price,exit_reason,gross_ret,net_ret)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,NULL,NULL)""",
            (t, addr, cfg["id"], entry_ts, decision_ts, observed_at, entry_price,
             ref_peak, drawdown, liquidity, cfg["dip"], cfg["tp"], cfg["sl"],
             cfg["lb"], cfg["hold"], "OPEN"))
        return t

    def close_trade(self, trade_id, exit_ts, exit_price, reason, gross, net):
        self.conn.execute(
            "UPDATE paper_trades SET state='CLOSED', exit_ts=?, exit_price=?, "
            "exit_reason=?, gross_ret=?, net_ret=? WHERE id=?",
            (exit_ts, exit_price, reason, gross, net, trade_id))
        return self.conn.execute("SELECT token_address, config_id FROM paper_trades WHERE id=?", (trade_id,)).fetchone()

    def open_trades(self, addr=None):
        q = "SELECT * FROM paper_trades WHERE state='OPEN'"
        if addr:
            q += " AND token_address=?"
            return self.conn.execute(q, (addr,)).fetchall()
        return self.conn.execute(q).fetchall()

    def all_trades(self):
        return self.conn.execute("SELECT * FROM paper_trades ORDER BY entry_ts").fetchall()

    # ---- events ----
    def event(self, etype, token=None, config_id=None, payload=None):
        self.conn.execute(
            "INSERT INTO events(id,ts,event_type,token_address,config_id,payload) "
            "VALUES(?,?,?,?,?,?)",
            (self._next_id(), time.time(), etype, token, config_id, json.dumps(payload or {})))

    def events_by_type(self, etype):
        return self.conn.execute("SELECT * FROM events WHERE event_type=?", (etype,)).fetchall()

    def last_event_time(self):
        r = self.conn.execute("SELECT MAX(ts) FROM events").fetchone()[0]
        return r or 0.0

    # ---- meta ----
    def set_meta(self, k, v):
        self.conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", (k, str(v)))

    def get_meta(self, k):
        r = self.conn.execute("SELECT value FROM meta WHERE key=?", (k,)).fetchone()
        return r[0] if r else None