import asyncio
import time

from tracker.engine import Engine, WalletState
from tracker.formatting import CoinNames, format_update

ADDR = "0x" + "ab" * 20


class FakeInfo:
    def __init__(self, orders, details=None):
        self.orders = orders
        self.details = details or {}

    async def open_orders(self, user):
        self.bulk_calls = getattr(self, "bulk_calls", 0) + 1
        return self.orders

    async def order_details(self, user, oid):
        self.lookups = getattr(self, "lookups", 0) + 1
        return self.details.get(oid)


def fill(tid, oid, sz, px, side="B", dir_="Open Long", t=1000, pnl="0.0"):
    return {"coin": "ETH", "px": px, "sz": sz, "side": side, "time": t, "startPosition": "0",
            "dir": dir_, "closedPnl": pnl, "hash": "0x", "oid": oid, "crossed": True,
            "fee": "0.1", "tid": tid, "feeToken": "USDC"}


def basic(oid, status, side="A", sz="1.0", px="3500.0", ts=None):
    ts = int(time.time() * 1000) if ts is None else ts
    return {"order": {"coin": "ETH", "side": side, "limitPx": px, "sz": sz, "oid": oid,
                      "timestamp": ts, "origSz": sz}, "status": status, "statusTimestamp": ts}


def details(order, status="open"):
    return {"order": order, "status": status, "statusTimestamp": 2}


def full_order(oid, **extra):
    return {"coin": "ETH", "side": "A", "limitPx": "3500.0", "sz": "1.0", "origSz": "1.0", "oid": oid,
            "timestamp": 1, "orderType": "Limit", "isTrigger": False, "triggerPx": "0.0",
            "reduceOnly": False, "isPositionTpsl": False, "tif": "Gtc", **extra}


def take_profit(oid, **extra):
    return full_order(oid, orderType="Take Profit Market", isTrigger=True, reduceOnly=True, tif=None,
                      triggerPx="3500.0", triggerCondition="Price above 3500", **extra)


def stop_loss(oid):
    return full_order(oid, orderType="Stop Market", isTrigger=True, reduceOnly=True, tif=None,
                      limitPx="2900.0", triggerPx="2900.0", triggerCondition="Price below 2900")


def make_engine(info):
    sent = []

    async def notify(addr, fills, orders):
        sent.append((addr, fills, orders))

    eng = Engine(info, "ws://unused", notify)
    eng._state[ADDR] = WalletState()
    return eng, sent


def message(sent):
    return format_update("me", ADDR, sent[0][1], sent[0][2], CoinNames())[0]


def test_snapshot_ignored_and_fills_batched_into_one_message():
    async def go():
        eng, sent = make_engine(FakeInfo([]))
        await eng._on_connect(ADDR)
        await eng.handle_message(ADDR, "userFills", {"isSnapshot": True, "user": ADDR,
                                                     "fills": [fill(1, 9, "1", "3000", t=900)]})
        for tid in (2, 3, 4):
            await eng.handle_message(ADDR, "userFills", {"user": ADDR, "fills": [fill(tid, 10, "0.5", "3000", t=1000 + tid)]})
        await eng.handle_message(ADDR, "userFills", {"user": ADDR, "fills": [fill(3, 10, "0.5", "3000")]})  # dup
        await eng.flush(ADDR)
        assert len(sent) == 1
        assert [f["tid"] for f in sent[0][1]] == [2, 3, 4]
        msg = message(sent)
        assert "1.5 ETH" in msg and "(3 fills)" in msg
        await eng.flush(ADDR)
        assert len(sent) == 1  # nothing new, nothing sent
    asyncio.run(go())


def test_new_tp_order_is_enriched_and_existing_orders_not_reported():
    async def go():
        eng, sent = make_engine(FakeInfo([full_order(1)], {2: details(take_profit(2))}))
        await eng._on_connect(ADDR)
        await eng.handle_message(ADDR, "orderUpdates", [basic(1, "open")])  # already known
        await eng.handle_message(ADDR, "orderUpdates", [basic(2, "open")])
        await eng.flush(ADDR)
        assert [e.order["oid"] for e in sent[0][2]] == [2]
        msg = message(sent)
        assert "Take profit (market)" in msg and "trigger above <b>3,500</b>" in msg
    asyncio.run(go())


def test_order_labels_for_limits_and_entry_triggers():
    entry_stop = full_order(3, orderType="Stop Market", isTrigger=True, triggerPx="3000.0", tif=None)

    async def go():
        eng, sent = make_engine(FakeInfo([], {
            1: details(full_order(1)),
            2: details(full_order(2, reduceOnly=True)),
            3: details(entry_stop),
            4: details(stop_loss(4)),
        }))
        await eng._on_connect(ADDR)
        await eng.handle_message(ADDR, "orderUpdates", [basic(i, "open") for i in (1, 2, 3, 4)])
        await eng.flush(ADDR)
        msg = message(sent)
        assert "New Limit order:" in msg
        assert "New Limit order (reduce-only):" in msg
        assert "New Trigger (market):" in msg  # opens a position, so not a stop loss
        assert "New Stop loss (market):" in msg
    asyncio.run(go())


def test_market_order_filled_in_same_window_shows_only_fill():
    async def go():
        eng, sent = make_engine(FakeInfo([]))
        await eng._on_connect(ADDR)
        await eng.handle_message(ADDR, "orderUpdates", [basic(5, "open", side="B")])
        await eng.handle_message(ADDR, "orderUpdates", [basic(5, "filled", side="B")])
        await eng.handle_message(ADDR, "userFills", {"user": ADDR, "fills": [fill(7, 5, "1", "3000")]})
        await eng.flush(ADDR)
        assert sent[0][2] == [] and len(sent[0][1]) == 1
    asyncio.run(go())


def test_fill_before_open_message_still_shows_only_the_fill():
    """Live data often has the fill arrive before the order's own open message."""
    async def go():
        eng, sent = make_engine(FakeInfo([]))
        await eng._on_connect(ADDR)
        await eng.handle_message(ADDR, "userFills", {"user": ADDR, "fills": [fill(7, 5, "1", "3000")]})
        await eng.handle_message(ADDR, "orderUpdates", [basic(5, "open"), basic(5, "filled", sz="0.0")])
        await eng.flush(ADDR)
        assert sent[0][2] == [] and len(sent[0][1]) == 1
    asyncio.run(go())


def test_cancel_keeps_known_order_type_without_a_lookup():
    async def go():
        eng, sent = make_engine(FakeInfo([stop_loss(3)]))
        await eng._on_connect(ADDR)
        await eng.handle_message(ADDR, "orderUpdates", [basic(3, "canceled", px="2900.0")])
        await eng.flush(ADDR)
        assert "Canceled Stop loss (market)" in message(sent)
    asyncio.run(go())


def test_cancel_of_order_only_seen_on_websocket_is_looked_up_not_called_limit():
    """The websocket order has no type, so it must not fall back to a "Limit order" label."""
    async def go():
        eng, sent = make_engine(FakeInfo([], {8: details(stop_loss(8), "canceled")}))
        await eng._on_connect(ADDR)
        await eng.handle_message(ADDR, "orderUpdates", [basic(8, "open"), basic(8, "canceled")])
        await eng.flush(ADDR)
        msg = message(sent)
        assert "New Stop loss (market)" in msg and "Canceled Stop loss (market)" in msg
        assert "Limit" not in msg
    asyncio.run(go())


def test_ioc_and_post_only_rejects_are_hidden_but_real_rejects_are_not():
    async def go():
        eng, sent = make_engine(FakeInfo([]))
        await eng._on_connect(ADDR)
        await eng.handle_message(ADDR, "orderUpdates", [basic(1, "iocCancelRejected"), basic(2, "badAloPxRejected")])
        await eng.flush(ADDR)
        assert sent == []
        await eng.handle_message(ADDR, "orderUpdates", [basic(3, "perpMarginRejected")])
        await eng.flush(ADDR)
        assert [e.status for e in sent[0][2]] == ["perpMarginRejected"]
    asyncio.run(go())


def test_market_order_caught_at_a_batch_boundary_is_dropped_once_known():
    market = full_order(5, orderType="Market", tif="FrontendMarket")

    async def go():
        eng, sent = make_engine(FakeInfo([], {5: details(market, "filled")}))
        await eng._on_connect(ADDR)
        await eng.handle_message(ADDR, "orderUpdates", [basic(5, "open")])
        await eng.flush(ADDR)
        assert sent == []
    asyncio.run(go())


def test_order_older_than_tracking_is_not_reported_as_new():
    async def go():
        eng, sent = make_engine(FakeInfo([]))
        await eng._on_connect(ADDR)
        await eng.handle_message(ADDR, "orderUpdates", [basic(6, "open", ts=1000)])
        await eng.flush(ADDR)
        assert sent == []
    asyncio.run(go())


def test_reconnect_snapshot_reports_missed_fills_only():
    async def go():
        eng, sent = make_engine(FakeInfo([]))
        await eng._on_connect(ADDR)
        await eng.handle_message(ADDR, "userFills", {"isSnapshot": True, "user": ADDR, "fills": [fill(1, 1, "1", "1", t=100)]})
        await eng.handle_message(ADDR, "userFills", {"isSnapshot": True, "user": ADDR,
                                                     "fills": [fill(1, 1, "1", "1", t=100), fill(2, 2, "1", "1", t=200)]})
        await eng.flush(ADDR)
        assert [f["tid"] for f in sent[0][1]] == [2]
    asyncio.run(go())


def test_reconnect_reports_what_happened_to_orders_during_the_gap():
    info = FakeInfo([full_order(i) for i in (1, 2, 3, 4)])

    async def go():
        eng, sent = make_engine(info)
        await eng._on_connect(ADDR)
        info.orders = [full_order(1), full_order(5)]  # 2 canceled, 3 filled, 4 still live elsewhere, 5 new
        info.details = {2: details(full_order(2), "marginCanceled"), 3: details(full_order(3), "filled"),
                        4: details(full_order(4), "open")}
        await eng._on_connect(ADDR)
        await eng.flush(ADDR)
        assert {(e.kind, e.order["oid"]) for e in sent[0][2]} == {("new", 5), ("canceled", 2)}
        assert set(eng._state[ADDR].open_orders) == {1, 4, 5}
    asyncio.run(go())


def test_failed_seed_propagates_so_the_stream_retries():
    class Down(FakeInfo):
        async def open_orders(self, user):
            raise RuntimeError("api down")

    async def go():
        eng, _ = make_engine(Down([]))
        try:
            await eng._on_connect(ADDR)
        except RuntimeError:
            return
        raise AssertionError("expected the seed failure to propagate")
    asyncio.run(go())
