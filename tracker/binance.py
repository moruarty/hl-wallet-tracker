"""Binance copy-trading lead traders: polls each trader's completed trades and reports new ones.

Binance has no official API for following other traders, so this uses the endpoints behind the
copy-trading pages on binance.com. They're undocumented and can change without warning. Open
positions are only available when the trader chooses to show them, but completed trades are public.

Each trade comes with the time it happened, so every alert also records how long it took to show
up, in data/binance_latency.csv.
"""
import asyncio
import csv
import logging
import os
import time
from decimal import Decimal
from typing import Awaitable, Callable

import aiohttp

log = logging.getLogger(__name__)

API = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade"
PROFILE_URL = "https://www.binance.com/en/copy-trading/lead-details/{}"
PAGE_SIZE = 20  # Binance answers "system busy" to pages much bigger than 50
MAX_PAGES = 5  # pages read in one check when a trader has many new trades at once
LOOKBACK_MS = 24 * 3600 * 1000  # trades older than this are never reported
FORGET_AFTER_MS = 2 * LOOKBACK_MS
GAP_BETWEEN_TRADERS = 1.0  # seconds; bursts of requests get "system busy"
LATENCY_PATH = "data/binance_latency.csv"
HISTORY_PAGE_SIZE = 50
HISTORY_PAGES = 3  # closed positions read to find when each coin's last position ended
REBUILD_LOOKBACK_MS = 30 * 24 * 3600 * 1000  # trades read to rebuild hidden positions
REBUILD_MAX_PAGES = 30  # 600 trades; a very active trader's older trades are cut off


class BinanceError(Exception):
    """Binance answered, but with an error such as "system busy"."""


def now_ms() -> int:
    return int(time.time() * 1000)


def dec(value) -> Decimal:
    """Binance sends JSON numbers, so 14.837 arrives as a float; str() keeps it as 14.837."""
    return Decimal(str(value))


class BinanceClient:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    async def _request(self, method: str, path: str, body: dict | None = None):
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
                # The endpoints serve binance.com's own pages and expect a browser.
                headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"},
            )
        async with self._session.request(method, API + path, json=body) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
        if data.get("code") != "000000":
            raise BinanceError(data.get("message") or data.get("code"))
        return data.get("data")

    async def search(self, nickname: str) -> list[dict]:
        """Lead traders whose nickname matches, best match first as Binance ranks them."""
        data = await self._request("POST", "/home-page/query-list", {
            "pageNumber": 1, "pageSize": 10, "timeRange": "30D", "dataType": "ROI", "favoriteOnly": False,
            "hideFull": False, "nickname": nickname, "order": "DESC", "portfolioType": "ALL",
        })
        return (data or {}).get("list") or []

    async def detail(self, portfolio_id: str) -> dict:
        return await self._request("GET", f"/lead-portfolio/detail?portfolioId={portfolio_id}") or {}

    async def trades(self, portfolio_id: str, start_ms: int, end_ms: int, page: int = 1) -> list[dict]:
        """Completed trades, newest first."""
        data = await self._request("POST", "/lead-portfolio/order-history", {
            "portfolioId": portfolio_id, "startTime": start_ms, "endTime": end_ms,
            "pageNumber": page, "pageSize": PAGE_SIZE,
        })
        return (data or {}).get("list") or []

    async def positions(self, portfolio_id: str) -> list[dict]:
        """Open positions, only filled in when the trader shows them."""
        return await self._request("GET", f"/lead-data/positions?portfolioId={portfolio_id}") or []

    async def position_history(self, portfolio_id: str, page: int = 1) -> list[dict]:
        """Positions newest first. Lists a position once some of it is closed, never while it's only open."""
        data = await self._request("POST", "/lead-portfolio/position-history", {
            "portfolioId": portfolio_id, "pageNumber": page, "pageSize": HISTORY_PAGE_SIZE, "sort": "OPENING",
        })
        return (data or {}).get("list") or []

    async def close(self) -> None:
        if self._session:
            await self._session.close()


def trade_key(raw: dict) -> tuple:
    """Trades have no ID, so this identifies one. Its filled size can grow while it stays the same trade."""
    return raw["symbol"], raw["side"], raw["positionSide"], raw["orderTime"]


def direction(side: str, position_side: str) -> str:
    if position_side == "LONG":
        return "Open Long" if side == "BUY" else "Close Long"
    if position_side == "SHORT":
        return "Open Short" if side == "SELL" else "Close Short"
    return "Buy" if side == "BUY" else "Sell"


Notifier = Callable[[str, list[dict]], Awaitable[None]]


class BinancePoller:
    """Checks each tracked trader's trades every `interval` seconds."""

    def __init__(self, client: BinanceClient, notify: Notifier, storage, interval: float = 15.0,
                 latency_path: str = LATENCY_PATH):
        self._client = client
        self._notify = notify
        self._storage = storage
        self._interval = interval
        self._latency_path = latency_path
        self._traders: set[str] = set()
        # Per trader: trade key -> (filled size, PnL) as last seen. Missing means not loaded yet.
        self._seen: dict[str, dict[tuple, tuple[Decimal, Decimal]]] = {}
        self._task: asyncio.Task | None = None

    def sync(self, portfolio_ids: set[str]) -> None:
        for pid in self._traders - portfolio_ids:
            self._seen.pop(pid, None)
            log.info("Stopped tracking Binance trader %s", pid)
        for pid in portfolio_ids - self._traders:
            log.info("Started tracking Binance trader %s", pid)
        self._traders = set(portfolio_ids)

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="binance")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        failures: dict[str, int] = {}
        while True:
            started = time.monotonic()
            for pid in sorted(self._traders):
                try:
                    await self.poll(pid)
                    if failures.pop(pid, 0) >= 3:
                        log.info("Binance trader %s is reachable again", pid)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    failures[pid] = failures.get(pid, 0) + 1
                    # "System busy" now and then is normal, so only a run of failures is worth a warning.
                    level = logging.WARNING if failures[pid] == 3 else logging.DEBUG
                    log.log(level, "Binance check failed for %s (%d in a row): %r", pid, failures[pid], e)
                await asyncio.sleep(GAP_BETWEEN_TRADERS)
            await asyncio.sleep(max(self._interval - (time.monotonic() - started), 1))

    async def _fetch(self, pid: str, now: int) -> list[dict]:
        seen = self._seen.get(pid)
        trades = []
        for page in range(1, MAX_PAGES + 1):
            batch = await self._client.trades(pid, now - LOOKBACK_MS, now, page)
            trades.extend(batch)
            # Stop at a short page, or once this page reaches trades we already have.
            if len(batch) < PAGE_SIZE or seen is None or trade_key(batch[-1]) in seen:
                break
        return trades

    async def poll(self, pid: str) -> None:
        now = now_ms()
        raw = await self._fetch(pid, now)
        seen = self._seen.get(pid)
        new = []
        if seen is None:
            # First check: what's already there isn't news, except trades made while the bot was down.
            cursor = self._storage.binance_cursor(pid)
            if cursor is None and not raw:
                self._storage.set_binance_cursor(pid, now)  # so a trade made during a restart isn't lost
            seen = self._seen[pid] = {}
            for t in raw:
                seen[trade_key(t)] = (dec(t["executedQty"]), dec(t["totalPnl"]))
                if cursor is not None and t["orderUpdateTime"] > cursor:
                    new.append(self._trade(t, dec(t["executedQty"]), dec(t["totalPnl"]), now, missed=True))
        else:
            for t in sorted(raw, key=lambda t: t["orderUpdateTime"]):
                key, qty, pnl = trade_key(t), dec(t["executedQty"]), dec(t["totalPnl"])
                old_qty, old_pnl = seen.get(key, (Decimal(0), Decimal(0)))
                if qty > old_qty:  # a new trade, or more of an order filled since the last check
                    new.append(self._trade(t, qty - old_qty, pnl - old_pnl, now, more=old_qty > 0))
                    seen[key] = (qty, pnl)
        for key in [k for k in seen if k[3] < now - FORGET_AFTER_MS]:
            del seen[key]
        if raw:
            self._storage.set_binance_cursor(pid, max(t["orderUpdateTime"] for t in raw))
        if new:
            new.sort(key=lambda t: t["time"])
            self._record_latency(pid, [t for t in new if not t["missed"]], now)
            await self._notify(pid, new)

    @staticmethod
    def _trade(raw: dict, qty: Decimal, pnl: Decimal, now: int, missed: bool = False, more: bool = False) -> dict:
        return {
            "symbol": raw["symbol"], "base": raw.get("baseAsset") or raw["symbol"], "side": raw["side"],
            "position_side": raw["positionSide"], "type": raw.get("type", ""), "qty": qty,
            "price": dec(raw["avgPrice"]), "pnl": pnl, "time": raw["orderUpdateTime"],
            "delay": max(now - raw["orderUpdateTime"], 0) / 1000, "missed": missed, "more": more,
        }

    def _record_latency(self, pid: str, trades: list[dict], now: int) -> None:
        if not trades:
            return
        try:
            if os.path.dirname(self._latency_path):
                os.makedirs(os.path.dirname(self._latency_path), exist_ok=True)
            new_file = not os.path.exists(self._latency_path)
            with open(self._latency_path, "a", newline="") as f:
                writer = csv.writer(f)
                if new_file:
                    writer.writerow(["seen_ms", "portfolio_id", "symbol", "trade_ms", "delay_s", "poll_interval_s"])
                for t in trades:
                    writer.writerow([now, pid, t["symbol"], t["time"], f"{t['delay']:.1f}", self._interval])
        except OSError:
            log.warning("Could not record Binance delays", exc_info=True)
        for t in trades:
            log.info("Binance trade %s %s seen %.1fs after it happened", pid, t["symbol"], t["delay"])


def latency_summary(path: str = LATENCY_PATH, last: int = 500) -> dict | None:
    """Delay statistics over the most recent trades, or None before any were recorded."""
    try:
        with open(path, newline="") as f:
            delays = sorted(float(row["delay_s"]) for row in list(csv.DictReader(f))[-last:])
    except (OSError, KeyError, ValueError):
        return None
    if not delays:
        return None
    pick = lambda q: delays[min(int(q * len(delays)), len(delays) - 1)]
    return {"count": len(delays), "median": pick(0.5), "p90": pick(0.9), "max": delays[-1]}


# ---- open positions ---------------------------------------------------------------------
#
# Most lead traders hide their open positions, so they're rebuilt from public data instead.
# Binance's position history lists a position once part of it is closed, and gives the time each
# fully closed position ended. A position that's only been opened isn't listed anywhere, but every
# trade is: so for each coin and side, the trades since its last position ended add up to what's
# open now. Checked against traders who do show their positions, it matched them exactly.

def base_asset(symbol: str) -> str:
    for quote in ("USDT", "USDC", "BUSD"):
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)]
    return symbol


def _position(symbol: str, long: bool, qty, entry, opened, **extra) -> dict:
    return {"symbol": symbol, "base": base_asset(symbol), "long": long, "qty": qty, "entry": entry,
            "opened": opened, "mark": None, "pnl": None, "leverage": None, **extra}


def shown_positions(raw: list[dict]) -> list[dict]:
    """Positions from a trader who shows them, in the same shape as rebuilt ones."""
    out = []
    for p in raw:
        amount = dec(p.get("positionAmount") or 0)
        if amount == 0:
            continue
        side = p.get("positionSide")
        long = side == "LONG" or (side == "BOTH" and amount > 0)
        out.append(_position(
            p["symbol"], long, abs(amount), dec(p["entryPrice"]), None,
            mark=dec(p["markPrice"]) if p.get("markPrice") is not None else None,
            pnl=dec(p["unrealizedProfit"]) if p.get("unrealizedProfit") is not None else None,
            leverage=p.get("leverage"),
        ))
    return out


def rebuild_positions(trades: list[dict], history: list[dict]) -> list[dict]:
    """Open positions worked out from completed trades and position history.

    A size of None means the position is open but started before the trades that were read.
    """
    last_end: dict[tuple, int] = {}
    partly_closed = {}
    for h in history:
        key = (h["symbol"], h["side"].upper())
        if h["status"] == "All Closed":
            last_end[key] = max(last_end.get(key, 0), h.get("closed") or 0)
        else:
            partly_closed.setdefault(key, h)

    # Per (symbol, LONG/SHORT/BOTH): signed size, cost of what's open, when it opened, and whether
    # it closed more than the trades opened, which means it started before them.
    books: dict[tuple, dict] = {}
    for t in sorted(trades, key=lambda t: t["orderTime"]):
        side, symbol = t["positionSide"], t["symbol"]
        if side == "BOTH":  # one-way mode: buys and sells move one position between long and short
            start = max(last_end.get((symbol, "LONG"), 0), last_end.get((symbol, "SHORT"), 0))
            delta = dec(t["executedQty"]) * (1 if t["side"] == "BUY" else -1)
        else:
            start = last_end.get((symbol, side), 0)
            opening = (t["side"] == "BUY") == (side == "LONG")
            delta = dec(t["executedQty"]) * (1 if opening else -1)
        if t["orderTime"] <= start:
            continue
        book = books.setdefault((symbol, side), {"size": Decimal(0), "cost": Decimal(0), "opened": None, "early": False})
        price, size = dec(t["avgPrice"]), book["size"]
        if size == 0 or (size > 0) == (delta > 0):
            book["cost"] += abs(delta) * price
            book["opened"] = book["opened"] or t["orderTime"]
            book["size"] = size + delta
        elif abs(delta) <= abs(size):
            book["cost"] *= (abs(size) - abs(delta)) / abs(size)
            book["size"] = size + delta
            if book["size"] == 0:
                book["opened"] = None
        else:  # flipped through zero; what's left over opens the other way at this price
            book["size"] = size + delta
            book["cost"] = abs(book["size"]) * price
            book["opened"] = t["orderTime"]
        if side != "BOTH" and book["size"] < 0:
            book["early"] = True

    out = []
    seen = set()
    for (symbol, side), book in books.items():
        if book["early"]:
            continue
        if book["size"] == 0:
            continue
        long = side == "LONG" or (side == "BOTH" and book["size"] > 0)
        qty = abs(book["size"])
        out.append(_position(symbol, long, qty, book["cost"] / qty, book["opened"]))
        seen.add((symbol, "LONG" if long else "SHORT"))
    for key, h in partly_closed.items():
        if key not in seen:
            out.append(_position(key[0], key[1] == "LONG", None, dec(h["avgCost"]) if h.get("avgCost") else None,
                                 h.get("opened"), partly_closed=True))
    return out


async def _retry(call, *args, attempts: int = 4, wait: float = 2.0):
    """Binance answers "system busy" now and then, especially alongside the poller's checks."""
    for attempt in range(attempts):
        try:
            return await call(*args)
        except BinanceError:
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(wait * (attempt + 1))


async def load_positions(client: BinanceClient, portfolio_id: str, show: bool) -> tuple[list[dict], dict]:
    """Open positions and how they were found: {"shown": bool, "cut_off": bool}."""
    if show:
        return shown_positions(await _retry(client.positions, portfolio_id)), {"shown": True, "cut_off": False}
    history = []
    for page in range(1, HISTORY_PAGES + 1):
        batch = await _retry(client.position_history, portfolio_id, page)
        history.extend(batch)
        if len(batch) < HISTORY_PAGE_SIZE:
            break
        await asyncio.sleep(GAP_BETWEEN_TRADERS)
    now = now_ms()
    trades, cut_off = [], True
    for page in range(1, REBUILD_MAX_PAGES + 1):
        batch = await _retry(client.trades, portfolio_id, now - REBUILD_LOOKBACK_MS, now, page)
        trades.extend(batch)
        if len(batch) < PAGE_SIZE:
            cut_off = False
            break
        await asyncio.sleep(GAP_BETWEEN_TRADERS)
    return rebuild_positions(trades, history), {"shown": False, "cut_off": cut_off}
