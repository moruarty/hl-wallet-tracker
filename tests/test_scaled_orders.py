"""Scaled orders: summary lines, the "Show all" button, xyz open orders and wallet links."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from telegram import CallbackQuery, Update, User
from telegram.ext import CallbackQueryHandler

from tests.test_bot import ALLOWED, OTHER, WALLET, make_bot
from tests.test_engine import ADDR, FakeInfo, basic, full_order, make_engine, stop_loss
from tracker.engine import OrderEvent
from tracker.formatting import (
    ADDRESS_URL, CoinNames, format_open_orders, format_order_details, format_update, group_orders, OrderGroup,
)
from tracker.hyperliquid import InfoClient

NAMES = CoinNames()


def limit(oid, px, sz, side="B", coin="ETH", **extra):
    return full_order(oid, limitPx=px, sz=sz, origSz=sz, side=side, coin=coin, **extra)


def new_events(*orders):
    return [OrderEvent("new", "open", o, o) for o in orders]


def test_scaled_orders_show_size_weighted_average_range_and_total():
    orders = new_events(limit(1, "100", "1"), limit(2, "110", "1"), limit(3, "120", "2"))
    text = format_update("me", WALLET, [], orders, NAMES)[0]
    # (100 + 110 + 240) / 4 = 112.5, which a plain average of the prices (110) would get wrong
    assert "3 Limit orders: <b>Buy 4 ETH</b> · avg <b>112.5</b> · range 100 – 120 · $450" in text
    assert text.count("📝") == 1


def test_average_is_rounded_to_just_past_the_price_precision():
    orders = new_events(limit(1, "78269", "1"), limit(2, "78300", "2"), limit(3, "78304", "4"))
    assert "avg <b>78,297.9</b> ·" in format_update("me", WALLET, [], orders, NAMES)[0]
    small = new_events(limit(1, "0.004312", "1"), limit(2, "0.004314", "1"), limit(3, "0.004313", "1"))
    assert "avg <b>0.004313</b> ·" in format_update("me", WALLET, [], small, NAMES)[0]


def test_two_orders_are_not_a_scale():
    orders = new_events(limit(1, "100", "1"), limit(2, "110", "1"))
    assert all(isinstance(item, OrderEvent) for item in group_orders(orders))
    assert format_order_details("me", WALLET, orders, NAMES) == []


def test_groups_split_by_coin_side_and_reduce_only():
    orders = new_events(
        *[limit(i, "100", "1") for i in (1, 2, 3)],
        *[limit(i, "100", "1", side="A") for i in (4, 5, 6)],
        *[limit(i, "100", "1", coin="xyz:CL") for i in (7, 8, 9)],
        *[limit(i, "100", "1", reduceOnly=True) for i in (10, 11, 12)],
    )
    items = group_orders(orders)
    assert len(items) == 4 and all(isinstance(item, OrderGroup) for item in items)
    assert "3 Limit orders (reduce-only):" in format_update("me", WALLET, [], orders, NAMES)[0]


def test_triggers_keep_their_own_lines_because_their_prices_matter():
    orders = new_events(stop_loss(1), stop_loss(2), stop_loss(3))
    assert all(isinstance(item, OrderEvent) for item in group_orders(orders))


def test_new_and_canceled_are_grouped_separately():
    placed = limit(1, "100", "1"), limit(2, "100", "1"), limit(3, "100", "1")
    canceled = [OrderEvent("canceled", "canceled", o, o) for o in placed]
    text = format_update("me", WALLET, [], new_events(*placed) + canceled, NAMES)[0]
    assert "New 3 Limit orders" in text and "Canceled 3 Limit orders" in text


def test_details_list_every_order():
    orders = new_events(*[limit(i, str(100 + i), "1") for i in range(100)])
    pages = format_order_details("me", WALLET, orders, NAMES)
    assert sum(page.count("New Limit order:") for page in pages) == 100


def test_open_orders_summary_and_expanded_list():
    orders = [limit(i, "100", "1", coin="xyz:CL") for i in range(5)] + [limit(9, "50", "1", coin="ETH")]
    summary = format_open_orders("me", WALLET, orders, NAMES)[0]
    assert "• 5 Limit orders: <b>Buy 5 xyz:CL</b> · avg <b>100</b>" in summary
    assert "• Limit order: <b>Buy 1 ETH</b> @ <b>50</b> · $50" in summary
    assert format_open_orders("me", WALLET, orders, NAMES, expand=True)[0].count("• Limit order:") == 6


def test_name_and_address_link_to_hypurrscan():
    url = f"https://hypurrscan.io/address/{WALLET}"
    assert ADDRESS_URL.format(WALLET) == url
    text = format_open_orders("big <whale>", WALLET, [], NAMES)[0]
    assert text.startswith(f'<a href="{url}"><b>big &lt;whale&gt;</b> · 0xabab…abab</a>')


def test_default_label_does_not_repeat_the_address():
    text = format_open_orders("0xabab…abab", WALLET, [], NAMES)[0]
    assert text.count("0xabab…abab") == 1


def test_open_orders_covers_main_dex_and_xyz():
    info = InfoClient("http://unused")
    calls = []

    async def post(payload):
        calls.append(payload)
        return [{"oid": payload["dex"] or "main"}]

    info._post = post
    orders = asyncio.run(info.open_orders(WALLET))
    assert {o["oid"] for o in orders} == {"main", "xyz"}
    assert all(c["type"] == "frontendOpenOrders" and c["user"] == WALLET for c in calls)


def test_big_scale_uses_one_bulk_call_instead_of_a_lookup_per_order():
    orders = [limit(i, "100", "1") for i in range(100)]
    info = FakeInfo([])

    async def go():
        eng, sent = make_engine(info)
        await eng._on_connect(ADDR)
        info.orders = orders
        await eng.handle_message(ADDR, "orderUpdates", [basic(i, "open") for i in range(100)])
        info.bulk_calls = 0
        await eng.flush(ADDR)
        assert info.bulk_calls == 1 and getattr(info, "lookups", 0) == 0
        assert "100 Limit orders" in format_update("me", ADDR, [], sent[0][2], NAMES)[0]
    asyncio.run(go())


def test_small_batch_still_looks_up_each_order():
    info = FakeInfo([], {1: {"order": limit(1, "100", "1"), "status": "open", "statusTimestamp": 2}})

    async def go():
        eng, _ = make_engine(info)
        await eng._on_connect(ADDR)
        await eng.handle_message(ADDR, "orderUpdates", [basic(1, "open")])
        info.bulk_calls = 0
        await eng.flush(ADDR)
        assert info.bulk_calls == 0 and info.lookups == 1
    asyncio.run(go())


# ---- the button ------------------------------------------------------------------------

def tap(bot, chat_id, data):
    query = SimpleNamespace(data=data, answer=AsyncMock(), edit_message_reply_markup=AsyncMock())
    update = SimpleNamespace(callback_query=query, effective_chat=SimpleNamespace(id=chat_id))
    asyncio.run(bot.on_show_all(update, None))
    return query


def test_notify_adds_button_only_when_orders_were_summarised(tmp_path):
    bot = make_bot(tmp_path)
    bot.storage.track(ALLOWED, WALLET, "me")
    bot._send = AsyncMock()

    asyncio.run(bot.notify(WALLET, [], new_events(limit(1, "100", "1"))))
    assert bot._send.await_args.kwargs["reply_markup"] is None

    asyncio.run(bot.notify(WALLET, [], new_events(*[limit(i, "100", "1") for i in range(5)])))
    button = bot._send.await_args.kwargs["reply_markup"].inline_keyboard[0][0]
    assert button.text == "Show all orders"
    assert len(button.callback_data.encode()) <= 64  # Telegram's limit

    # The real handler has to route this tap to on_show_all.
    tap_update = Update(1, callback_query=CallbackQuery("1", User(1, "u", False), "inst", data=button.callback_data))
    handlers = [h for h in bot.app.handlers[0] if isinstance(h, CallbackQueryHandler)]
    assert len(handlers) == 1 and handlers[0].callback == bot.on_show_all
    assert handlers[0].check_update(tap_update)


def test_tapping_the_button_sends_the_list_and_removes_the_button(tmp_path):
    bot = make_bot(tmp_path)
    bot._send = AsyncMock()
    markup = bot._show_all_button(ALLOWED, ["page 1", "page 2"])
    query = tap(bot, ALLOWED, markup.inline_keyboard[0][0].callback_data)
    assert [c.args[1] for c in bot._send.await_args_list] == ["page 1", "page 2"]
    query.edit_message_reply_markup.assert_awaited_once()


def test_button_is_refused_in_another_chat_and_when_expired(tmp_path):
    bot = make_bot(tmp_path)
    bot._send = AsyncMock()
    data = bot._show_all_button(ALLOWED, ["secret"]).inline_keyboard[0][0].callback_data
    for chat_id, payload in ((OTHER, data), (ALLOWED, "all:unknown")):
        query = tap(bot, chat_id, payload)
        assert query.answer.await_args.kwargs["show_alert"] is True
    bot._send.assert_not_awaited()


def test_button_refused_after_chat_is_removed_from_allowlist(tmp_path):
    bot = make_bot(tmp_path)
    bot._send = AsyncMock()
    data = bot._show_all_button(OTHER, ["secret"]).inline_keyboard[0][0].callback_data
    tap(bot, OTHER, data)
    bot._send.assert_not_awaited()


def test_saved_lists_are_capped(tmp_path):
    bot = make_bot(tmp_path)
    with patch("tracker.bot.MAX_SAVED_LISTS", 3):
        for _ in range(10):
            bot._show_all_button(ALLOWED, ["x"])
    assert len(bot._saved_lists) == 3


def test_orders_command_uses_both_dexes_and_adds_a_button(tmp_path):
    bot = make_bot(tmp_path)
    bot.storage.track(ALLOWED, WALLET, "me")
    bot.info.open_orders = AsyncMock(return_value=[limit(i, "100", "1", coin="xyz:CL") for i in range(5)])
    update = SimpleNamespace(effective_chat=SimpleNamespace(id=ALLOWED))
    asyncio.run(bot.cmd_orders(update, SimpleNamespace(args=[])))
    call = bot._reply.await_args
    assert "5 Limit orders" in call.args[1] and "xyz:CL" in call.args[1]
    assert call.kwargs["reply_markup"] is not None
