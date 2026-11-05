"""Keeps per-wallet order/fill state, turns websocket messages into events,
and flushes them in batches (default every 5 seconds) so bursts of fills
become one Telegram message per wallet."""
import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from .hyperliquid import InfoClient, WalletStream

log = logging.getLogger(__name__)

# Orders that never rested on the book: an IOC with nothing to match, or a post-only that
# would have crossed. They say nothing useful about what a wallet is doing.
HIDDEN_STATUSES = {"iocCancelRejected", "badAloPxRejected"}
# Time-in-force values of orders that execute at once, so only their fills matter.
IMMEDIATE_TIFS = {"Ioc", "FrontendMarket", "LiquidationMarket"}
MAX_GAP_LOOKUPS = 20  # orders to look up per reconnect when they vanished during the gap
BULK_ENRICH_MIN = 10  # from this many orders, one open-orders call is cheaper than one lookup each
MAX_CONCURRENT_LOOKUPS = 5
SEED_GRACE_MS = 60_000  # tolerated clock difference when spotting orders older than tracking


def closed_kind(status: str) -> str | None:
    """Which kind of event a closing order status is, or None if it isn't one."""
    if status == "rejected" or status.endswith("Rejected"):
        return "rejected"
    if status in ("canceled", "scheduledCancel") or status.endswith("Canceled"):
        return "canceled"
    return None


@dataclass
class OrderEvent:
    kind: str  # "new" | "canceled" | "triggered" | "rejected"
    status: str  # raw Hyperliquid status, e.g. "reduceOnlyCanceled"
    order: dict  # WsBasicOrder from the websocket
    info: dict | None = None  # frontendOpenOrders entry (orderType, triggerPx, ...) when known


@dataclass
class WalletState:
    open_orders: dict[int, dict] = field(default_factory=dict)
    triggered: set[int] = field(default_factory=set)
    seen_tids: set[int] = field(default_factory=set)
    tid_order: deque = field(default_factory=lambda: deque(maxlen=5000))
    last_fill_time: int = 0
    connected_once: bool = False
    seeded_ms: int = 0  # when the open-order book was last loaded
    got_fill_snapshot: bool = False
    pending_fills: list[dict] = field(default_factory=list)
    pending_orders: list[OrderEvent] = field(default_factory=list)
    filled_oids: set[int] = field(default_factory=set)  # orders fully filled since last flush

    def remember_tid(self, tid: int) -> None:
        if len(self.tid_order) == self.tid_order.maxlen:
            self.seen_tids.discard(self.tid_order[0])
        self.tid_order.append(tid)
        self.seen_tids.add(tid)


Notifier = Callable[[str, list[dict], list[OrderEvent]], Awaitable[None]]


class Engine:
    def __init__(self, info: InfoClient, ws_url: str, notify: Notifier, batch_interval: float = 5.0):
        self._info = info
        self._ws_url = ws_url
        self._notify = notify
        self._interval = batch_interval
        self._streams: dict[str, WalletStream] = {}
        self._state: dict[str, WalletState] = {}
        self._flush_task: asyncio.Task | None = None

    # ---- lifecycle -------------------------------------------------------
    def start(self) -> None:
        self._flush_task = asyncio.create_task(self._flush_loop(), name="flush")

    async def stop(self) -> None:
        if self._flush_task:
            self._flush_task.cancel()
        for s in list(self._streams.values()):
            await s.stop()
        self._streams.clear()

    async def sync(self, addresses: set[str]) -> None:
        """Make the open websocket streams match the set of tracked wallets."""
        for addr in set(self._streams) - addresses:
            await self._streams.pop(addr).stop()
            self._state.pop(addr, None)
            log.info("Stopped tracking %s", addr)
        for addr in addresses - set(self._streams):
            self._state[addr] = WalletState()
            stream = WalletStream(self._ws_url, addr, self.handle_message, self._on_connect)
            self._streams[addr] = stream
            stream.start()
            log.info("Started tracking %s", addr)

    # ---- websocket callbacks ----------------------------------------------
    async def _on_connect(self, user: str) -> None:
        """Seed the open-order book so existing orders are not reported as new.

        A failure here propagates so the stream reconnects and tries again, rather than
        carrying on with a stale book and losing whatever happened during a gap.
        """
        st = self._state.get(user)
        if st is None:
            return
        fresh = {o["oid"]: o for o in await self._info.open_orders(user)}
        if st.connected_once:
            # Reconnect: report orders that appeared while we were disconnected.
            for oid, o in fresh.items():
                if oid not in st.open_orders:
                    st.pending_orders.append(OrderEvent("new", "open", o, o))
            await self._report_vanished(user, st, fresh)
        st.open_orders = fresh
        st.connected_once = True
        st.seeded_ms = int(time.time() * 1000)

    async def _report_vanished(self, user: str, st: WalletState, fresh: dict[int, dict]) -> None:
        """Work out what happened to orders that left the book during a disconnect."""
        gone = [oid for oid in st.open_orders if oid not in fresh]
        if len(gone) > MAX_GAP_LOOKUPS:
            log.warning("%d orders closed for %s while disconnected; checking %d",
                        len(gone), user, MAX_GAP_LOOKUPS)
        for oid in gone[:MAX_GAP_LOOKUPS]:
            try:
                details = await self._info.order_details(user, oid)
            except Exception:
                log.warning("Could not look up order %s for %s", oid, user, exc_info=True)
                continue
            if details is None:
                continue
            status, order = details["status"], details["order"]
            kind = closed_kind(status)
            if kind:
                st.pending_orders.append(OrderEvent(kind, status, order, order))
            elif status != "filled":
                # Still live, on a HIP-3 dex that the seed doesn't cover.
                fresh[oid] = order

    async def handle_message(self, user: str, channel: str, data) -> None:
        st = self._state.get(user)
        if st is None:
            return
        if channel == "userFills":
            self._handle_fills(st, data)
        elif channel == "orderUpdates":
            for upd in data or []:
                self._handle_order_update(st, upd)

    def _handle_fills(self, st: WalletState, data: dict) -> None:
        fills = data.get("fills", [])
        if data.get("isSnapshot"):
            if not st.got_fill_snapshot:
                for f in fills:
                    st.remember_tid(f["tid"])
                    st.last_fill_time = max(st.last_fill_time, f["time"])
                st.got_fill_snapshot = True
                return
            # Snapshot after a reconnect: report only fills we missed.
            fills = [f for f in fills if f["time"] >= st.last_fill_time]
        for f in sorted(fills, key=lambda f: f["time"]):
            if f["tid"] in st.seen_tids:
                continue
            st.remember_tid(f["tid"])
            st.last_fill_time = max(st.last_fill_time, f["time"])
            st.pending_fills.append(f)

    def _handle_order_update(self, st: WalletState, upd: dict) -> None:
        order, status = upd["order"], upd["status"]
        oid = order["oid"]
        if status in HIDDEN_STATUSES:
            return
        known = st.open_orders.get(oid)
        # Orders first seen on the websocket have no type details until they are enriched.
        info = known if known and "orderType" in known else None

        if status == "open":
            if known is not None:
                known["sz"] = order["sz"]  # partial fill; the fill itself is reported separately
                return
            st.open_orders[oid] = dict(order)
            # An older order we hadn't seen (the seed misses most HIP-3 dexes) isn't a new one.
            if order["timestamp"] >= st.seeded_ms - SEED_GRACE_MS:
                st.pending_orders.append(OrderEvent("new", status, order))
        elif status == "filled":
            st.open_orders.pop(oid, None)
            st.triggered.discard(oid)
            st.filled_oids.add(oid)
        elif status == "triggered":
            if oid not in st.triggered:
                st.triggered.add(oid)
                st.pending_orders.append(OrderEvent("triggered", status, order, info))
        elif kind := closed_kind(status):
            st.open_orders.pop(oid, None)
            st.triggered.discard(oid)
            st.pending_orders.append(OrderEvent(kind, status, order, info))
        else:
            log.debug("Ignoring order status %s for oid %s", status, oid)

    # ---- batching ----------------------------------------------------------
    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            for addr in list(self._state):
                try:
                    await self.flush(addr)
                except Exception:
                    log.exception("Flush failed for %s", addr)

    async def flush(self, addr: str) -> None:
        st = self._state.get(addr)
        if st is None:
            return
        if not (st.pending_fills or st.pending_orders):
            st.filled_oids.clear()
            return
        fills, orders, filled = st.pending_fills, st.pending_orders, st.filled_oids
        st.pending_fills, st.pending_orders, st.filled_oids = [], [], set()

        # A new order that filled within the same window (e.g. a market/IOC order)
        # is fully described by its fills, so skip the "new order" line.
        orders = [e for e in orders if not (e.kind == "new" and e.order["oid"] in filled)]

        await self._enrich(addr, st, orders)
        orders = [e for e in orders if not self._is_immediate(e)]
        if fills or orders:
            await self._notify(addr, fills, orders)

    @staticmethod
    def _is_immediate(e: OrderEvent) -> bool:
        """A market order caught at a batch boundary: its fills tell the whole story."""
        info = e.info or {}
        return (e.kind in ("new", "canceled") and info.get("tif") in IMMEDIATE_TIFS
                and not info.get("isTrigger"))

    async def _enrich(self, addr: str, st: WalletState, events: list[OrderEvent]) -> None:
        """Fetch order type / trigger details (TP, SL, reduce-only) for orders about to be shown."""
        oids = {e.order["oid"] for e in events if e.info is None}
        found = {}
        # A scaled entry arrives as many orders at once, which one open-orders call can cover.
        if len(oids) >= BULK_ENRICH_MIN:
            try:
                found = {o["oid"]: o for o in await self._info.open_orders(addr) if o["oid"] in oids}
            except Exception as e:
                log.warning("Could not load open orders for %s: %r", addr, e)

        limit = asyncio.Semaphore(MAX_CONCURRENT_LOOKUPS)

        async def look_up(oid: int) -> dict | None:
            async with limit:
                return await self._info.order_details(addr, oid)

        remaining = [oid for oid in oids if oid not in found]
        results = await asyncio.gather(*(look_up(oid) for oid in remaining), return_exceptions=True)
        for oid, res in zip(remaining, results):
            if isinstance(res, Exception):
                log.warning("Could not enrich order %s for %s: %r", oid, addr, res)
            elif res:
                found[oid] = res["order"]
        for oid, order in found.items():
            if oid in st.open_orders:
                st.open_orders[oid] = order
        for e in events:
            if e.info is None:
                e.info = found.get(e.order["oid"])
