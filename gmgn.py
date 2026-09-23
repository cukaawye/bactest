"""GMGN HTTP client: discovery (trending), klines, token info.
Direct HTTP to the same researched endpoints; pacing + retry/backoff on 429.
"""
import json
import time
import urllib.error
import urllib.parse
import urllib.request

import config

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Referer": "https://gmgn.ai/sol/trending",
}


class GMGNError(Exception):
    pass


class GMGN:
    def __init__(self, sleep=config.REQUEST_PACING_S):
        self._last = 0.0
        self.sleep = sleep
        self.requests = 0
        self.failures = 0
        self.last_filter_id = None

    def _pace(self):
        dt = time.time() - self._last
        if dt < self.sleep:
            time.sleep(self.sleep - dt)

    def _request(self, method, url, body=None):
        for attempt in range(1, config.RETRY_MAX_TRIES + 1):
            self._pace()
            req = urllib.request.Request(url, headers=HEADERS, method=method)
            if body is not None:
                req.data = json.dumps(body).encode()
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    self.requests += 1
                    self._last = time.time()
                    return json.loads(r.read().decode())
            except urllib.error.HTTPError as e:
                self.failures += 1
                if e.code == 429 or e.code >= 500:
                    backoff = min(config.RETRY_BACKOFF_S * (2 ** (attempt - 1)),
                                  config.RETRY_BACKOFF_MAX_S)
                    time.sleep(backoff)
                    continue
                raise GMGNError(f"HTTP {e.code} {url}")
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
                self.failures += 1
                time.sleep(config.RETRY_BACKOFF_S)
                continue
        raise GMGNError(f"failed after {config.RETRY_MAX_TRIES} tries: {url}")

    # ---- discovery ----
    def trending(self, body=None):
        d = self._request("POST", config.TRENDING_MULTICHAIN, body or config.DISCOVERY_BODY)
        if d.get("code") != 0:
            raise GMGNError(f"trending code={d.get('code')}")
        rows = d.get("data") or []
        self.last_filter_id = rows[0].get("filter_id") if rows else None
        out = []
        for group in rows:
            if not isinstance(group, dict):
                continue
            for t in group.get("tokens") or []:
                try:
                    out.append({
                        "address": t.get("a"),
                        "symbol": t.get("s") or "",
                        "name": t.get("nm") or "",
                        "price": float(t.get("p") or 0),
                        "liquidity": float(t.get("lq") or 0),
                        "market_cap": float(t.get("mc") or 0),
                        "meta": t,
                    })
                except (TypeError, ValueError):
                    continue
        return [x for x in out if x["address"]]

    # ---- klines ----
    def klines(self, addr, begin_s, end_s):
        url = config.KLINE_URL.format(addr=urllib.parse.quote(addr),
                                      begin=int(begin_s), end=int(end_s))
        d = self._request("GET", url)
        if d.get("code") not in (0, None):
            raise GMGNError(f"kline code={d.get('code')}")
        rows = d.get("data") or []
        bars = []
        for r in rows:
            try:
                bars.append((int(r["time"]), float(r["open"]), float(r["high"]),
                             float(r["low"]), float(r["close"]), float(r.get("volume") or 0)))
            except (KeyError, TypeError, ValueError):
                continue
        bars.sort(key=lambda b: b[0])
        return bars

    # ---- token info ----
    def token_info(self, addresses):
        out = []
        for i in range(0, len(addresses), 6):
            chunk = addresses[i:i + 6]
            d = self._request("POST", config.MULTI_INFO, {"chain": "sol", "addresses": chunk})
            if d.get("code") != 0:
                raise GMGNError(f"token_info code={d.get('code')}")
            out.extend(d.get("data") or [])
        return out