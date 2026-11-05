"""Hyperliquid REST (info) and websocket clients.

Docs: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api

orderUpdates messages do not say which user they belong to, so each tracked
wallet gets its own websocket connection. Hyperliquid allows at most 10
websocket connections and 10 unique users in user subscriptions per IP,
which is why the bot caps tracking at 10 wallets.
"""
import asyncio
import json
import logging
import random
from typing import Any, Awaitable, Callable

import aiohttp
import websockets

log = logging.getLogger(__name__)

PING_INTERVAL = 30  # server drops connections idle for 60s
# The server answers every ping, so this long without any message means the link is dead.
STALE_AFTER = PING_INTERVAL * 2 + 15
# Dexes queried for open orders and positions: "" is the main perp dex (and spot, for open
# orders), "xyz" is a HIP-3 dex. An open-orders request costs 20 rate-limit weight per dex.
OPEN_ORDER_DEXES = ("", "xyz")


class InfoClient:
    def __init__(self, api_url: str):
        self._url = f"{api_url}/info"
        self._session: aiohttp.ClientSession | None = None

    async def _post(self, payload: dict) -> Any:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        async with self._session.post(self._url, json=payload) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def open_orders(self, user: str) -> list[dict]:
        """Open orders across every dex in OPEN_ORDER_DEXES, with full type details."""
        pages = await asyncio.gather(*(
            self._post({"type": "frontendOpenOrders", "user": user, "dex": dex}) for dex in OPEN_ORDER_DEXES
        ))
        return [order for page in pages for order in page]

    async def clearinghouse_states(self, user: str) -> list[dict]:
        """Perp account state, one per dex in OPEN_ORDER_DEXES. Costs 2 rate-limit weight each."""
        return list(await asyncio.gather(*(
            self._post({"type": "clearinghouseState", "user": user, "dex": dex}) for dex in OPEN_ORDER_DEXES
        )))

    async def order_details(self, user: str, oid: int) -> dict | None:
        """Full order and latest status: {"order": {...}, "status": ..., "statusTimestamp": ...}.

        Works after an order has closed, unlike frontendOpenOrders, and costs 2 rate-limit
        weight rather than 20. Returns None if the order is unknown.
        """
        res = await self._post({"type": "orderStatus", "user": user, "oid": oid})
        return res.get("order") if res.get("status") == "order" else None

    async def spot_meta(self) -> dict:
        return await self._post({"type": "spotMeta"})

    async def close(self) -> None:
        if self._session:
            await self._session.close()


MessageHandler = Callable[[str, str, Any], Awaitable[None]]  # (user, channel, data)
ConnectHandler = Callable[[str], Awaitable[None]]  # (user)


class WalletStream:
    """One websocket connection subscribed to orderUpdates and userFills for one wallet."""

    def __init__(self, ws_url: str, user: str, on_message: MessageHandler, on_connect: ConnectHandler):
        self.user = user
        self._url = ws_url
        self._on_message = on_message
        self._on_connect = on_connect
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"ws-{self.user}")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(self._url, ping_interval=None, max_size=2**23) as ws:
                    for sub_type in ("orderUpdates", "userFills"):
                        await ws.send(json.dumps(
                            {"method": "subscribe", "subscription": {"type": sub_type, "user": self.user}}
                        ))
                    log.info("Connected websocket for %s", self.user)
                    await self._on_connect(self.user)
                    backoff = 1.0
                    pinger = asyncio.create_task(self._ping(ws))
                    try:
                        while True:
                            msg = json.loads(await asyncio.wait_for(ws.recv(), STALE_AFTER))
                            channel = msg.get("channel")
                            if channel in ("orderUpdates", "userFills"):
                                try:
                                    await self._on_message(self.user, channel, msg.get("data"))
                                except Exception:
                                    log.exception("Error handling %s message for %s", channel, self.user)
                            elif channel == "error":
                                log.warning("Hyperliquid error for %s: %s", self.user, msg.get("data"))
                    finally:
                        pinger.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("Websocket for %s dropped (%r); reconnecting in %.0fs", self.user, e, backoff)
            await asyncio.sleep(backoff + random.random())
            backoff = min(backoff * 2, 60)

    @staticmethod
    async def _ping(ws) -> None:
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL)
                await ws.send(json.dumps({"method": "ping"}))
        except websockets.ConnectionClosed:
            pass  # the receive loop sees the same close and reconnects
