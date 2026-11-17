"""Binance lead traders: spotting new trades, measuring their delay, and the Telegram commands."""
import asyncio
import csv
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.test_bot import ALLOWED, OTHER, handles, make_bot
from tracker import binance
from tracker.binance import BinanceError, BinancePoller, direction, latency_summary
from tracker.formatting import format_binance_update
from tracker.storage import Storage

PID = "5075281354358777856"
NOW = 1_800_000_000_000


def trade(t, symbol="BTCUSDT", side="BUY", pos="LONG", qty="1", price="70000", pnl="0", kind="MARKET"):
    return {"symbol": symbol, "side": side, "positionSide": pos, "orderTime": t, "orderUpdateTime": t,
            "executedQty": qty, "avgPrice": price, "totalPnl": pnl, "type": kind,
            "baseAsset": symbol.removesuffix("USDT"), "quoteAsset": "USDT"}


class FakeClient:
    def __init__(self, trades=()):
        self.list = list(trades)  # newest first, as Binance returns them
        self.pages = []

    async def trades(self, pid, start, end, page=1):
        self.pages.append(page)
        size = binance.PAGE_SIZE
        return self.list[(page - 1) * size: page * size]


def make_poller(tmp_path, trades=()):
    sent = []

    async def notify(pid, new):
        sent.append(new)

    storage = Storage(str(tmp_path / "t.db"))
    client = FakeClient(trades)
    poller = BinancePoller(client, notify, storage, 15, latency_path=str(tmp_path / "latency.csv"))
    return poller, client, storage, sent


def poll(poller, now=NOW):
    with patch.object(binance, "now_ms", return_value=now):
        asyncio.run(poller.poll(PID))


def test_direction_names():
    assert direction("BUY", "LONG") == "Open Long" and direction("SELL", "LONG") == "Close Long"
    assert direction("SELL", "SHORT") == "Open Short" and direction("BUY", "SHORT") == "Close Short"
    assert direction("BUY", "BOTH") == "Buy" and direction("SELL", "BOTH") == "Sell"


def test_existing_trades_are_loaded_silently_then_new_ones_reported(tmp_path):
    poller, client, _, sent = make_poller(tmp_path, [trade(NOW - 60_000)])
    poll(poller)
    assert sent == []
    client.list.insert(0, trade(NOW + 5_000, symbol="ETHUSDT"))
    poll(poller, NOW + 17_000)
    assert len(sent) == 1 and sent[0][0]["symbol"] == "ETHUSDT"
    assert sent[0][0]["delay"] == 12.0
    poll(poller, NOW + 32_000)
    assert len(sent) == 1  # nothing new


def test_json_numbers_keep_their_exact_digits(tmp_path):
    """Binance sends sizes and prices as JSON numbers, which arrive as floats."""
    poller, client, _, sent = make_poller(tmp_path, [])
    poll(poller)
    client.list = [dict(trade(NOW + 1_000), executedQty=14.837, avgPrice=69984.80262, totalPnl=27836.55539876)]
    poll(poller, NOW + 5_000)
    t = sent[0][0]
    assert t["qty"] == Decimal("14.837") and t["price"] == Decimal("69984.80262")
    assert "Open Long 14.837 BTC" in format_binance_update("x", PID, [t])[0]


def test_more_of_an_order_filling_reports_only_the_extra(tmp_path):
    poller, client, _, sent = make_poller(tmp_path, [trade(NOW - 60_000, qty="2", pnl="10")])
    poll(poller)
    client.list[0] = dict(client.list[0], executedQty="5", totalPnl="40", orderUpdateTime=NOW + 1_000)
    poll(poller, NOW + 15_000)
    t = sent[0][0]
    assert t["qty"] == Decimal(3) and t["pnl"] == Decimal(30) and t["more"]


def test_trades_made_while_the_bot_was_down_are_reported_after_a_restart(tmp_path):
    poller, client, storage, sent = make_poller(tmp_path, [trade(NOW - 60_000)])
    poll(poller)
    client.list.insert(0, trade(NOW + 30_000, symbol="SOLUSDT"))
    restarted = BinancePoller(client, poller._notify, storage, 15, latency_path=poller._latency_path)
    with patch.object(binance, "now_ms", return_value=NOW + 90_000):
        asyncio.run(restarted.poll(PID))
    assert [t["symbol"] for t in sent[0]] == ["SOLUSDT"] and sent[0][0]["missed"]
    assert latency_summary(poller._latency_path) is None  # a restart gap isn't Binance's delay


def test_quiet_trader_still_gets_a_restart_cursor(tmp_path):
    poller, client, storage, sent = make_poller(tmp_path, [])
    poll(poller)
    assert storage.binance_cursor(PID) == NOW


def test_burst_of_trades_reads_more_pages(tmp_path):
    poller, client, _, sent = make_poller(tmp_path, [trade(NOW - 60_000)])
    poll(poller)
    burst = [trade(NOW + i * 10, qty=str(i + 1)) for i in range(45)]
    client.list = list(reversed(burst)) + client.list
    client.pages.clear()
    poll(poller, NOW + 15_000)
    assert client.pages == [1, 2, 3] and len(sent[0]) == 45


def test_delays_are_recorded_and_summarised(tmp_path):
    poller, client, _, _ = make_poller(tmp_path, [])
    poll(poller)
    client.list = [trade(NOW + 2_000, symbol="B"), trade(NOW + 1_000, symbol="A")]
    poll(poller, NOW + 12_000)
    with open(poller._latency_path, newline="") as f:
        rows = list(csv.DictReader(f))
    assert [(r["symbol"], r["delay_s"]) for r in rows] == [("A", "11.0"), ("B", "10.0")]
    assert latency_summary(poller._latency_path)["max"] == 11.0


def test_message_shows_direction_value_pnl_and_delay():
    t = {"symbol": "BTCUSDT", "base": "BTC", "side": "SELL", "position_side": "LONG", "type": "MARKET",
         "qty": Decimal("14.837"), "price": Decimal("69984.80262"), "pnl": Decimal("27836.555"),
         "time": NOW, "delay": 14.2, "missed": False, "more": False}
    text = format_binance_update("熬鹰资本", PID, [t])[0]
    assert f'href="https://www.binance.com/en/copy-trading/lead-details/{PID}"' in text
    assert "🟢 <b>Close Long 14.837 BTC</b> @ <b>69,984.8</b>" in text
    assert "🟢 PnL <b>+27,836.56</b> · market · seen after 14s" in text


def test_commands_are_gated(tmp_path):
    bot = make_bot(tmp_path)
    for cmd in ("/track_binance", "/untrack_binance", "/binance_delay"):
        assert handles(bot, ALLOWED, cmd) and not handles(bot, OTHER, cmd)


def run(bot, fn, *args):
    update = SimpleNamespace(effective_chat=SimpleNamespace(id=ALLOWED))
    asyncio.run(fn(update, SimpleNamespace(args=list(args))))
    return bot._reply.call_args.args[1]


def binance_bot(tmp_path, search=(), detail=None):
    bot = make_bot(tmp_path)
    bot.binance = SimpleNamespace(
        search=AsyncMock(return_value=list(search)),
        detail=AsyncMock(return_value=detail or {"nickname": "熬鹰资本", "positionShow": False}),
    )
    return bot


def test_track_by_name_uses_the_nickname_as_label(tmp_path):
    bot = binance_bot(tmp_path, [{"nickname": "熬鹰资本", "leadPortfolioId": PID}])
    text = run(bot, bot.cmd_track_binance, "熬鹰资本")
    assert "Now tracking" in text and PID in text and "hides open positions" in text
    assert [(t.address, t.label) for t in bot.storage.all_binance()] == [(PID, "熬鹰资本")]
    assert bot.binance_poller._traders == {PID}


def test_track_by_link_with_a_label(tmp_path):
    bot = binance_bot(tmp_path)
    run(bot, bot.cmd_track_binance, f"https://www.binance.com/en/copy-trading/lead-details/{PID}?ref=x", "eagle")
    assert [(t.address, t.label) for t in bot.storage.all_binance()] == [(PID, "eagle")]


def test_unknown_id_is_refused(tmp_path):
    bot = binance_bot(tmp_path)
    bot.binance.detail = AsyncMock(side_effect=BinanceError("not found"))
    assert "no lead trader" in run(bot, bot.cmd_track_binance, "1234567890123456789")
    assert bot.storage.all_binance() == []


def test_inexact_name_lists_close_matches(tmp_path):
    bot = binance_bot(tmp_path, [{"nickname": "熬鹰资本2", "leadPortfolioId": "111111111111111111"}])
    text = run(bot, bot.cmd_track_binance, "熬鹰")
    assert "Close matches" in text and "111111111111111111" in text
    assert bot.storage.all_binance() == []


def test_binance_outage_gets_a_friendly_reply(tmp_path):
    bot = binance_bot(tmp_path)
    bot.binance.search = AsyncMock(side_effect=BinanceError("busy"))
    assert "Couldn't reach Binance" in run(bot, bot.cmd_track_binance, "someone")


def test_untrack_forgets_the_restart_cursor_and_list_shows_both_kinds(tmp_path):
    bot = binance_bot(tmp_path, [{"nickname": "熬鹰资本", "leadPortfolioId": PID}])
    run(bot, bot.cmd_track_binance, "熬鹰资本")
    bot.storage.track(ALLOWED, "0x" + "ab" * 20, "hl")
    text = run(bot, bot.cmd_list)
    assert "Tracked wallets (1)" in text and "Binance traders (1)" in text
    bot.storage.set_binance_cursor(PID, NOW)
    assert "Stopped tracking" in run(bot, bot.cmd_untrack_binance, "熬鹰资本")
    assert bot.storage.binance_cursor(PID) is None and bot.binance_poller._traders == set()


def test_notify_binance_only_goes_to_allowed_chats(tmp_path):
    bot = binance_bot(tmp_path)
    bot.storage.track_binance(ALLOWED, PID, "a")
    bot.storage.track_binance(OTHER, PID, "b")
    bot._send = AsyncMock()
    t = {"symbol": "BTCUSDT", "base": "BTC", "side": "BUY", "position_side": "LONG", "type": "LIMIT",
         "qty": Decimal(1), "price": Decimal(1), "pnl": Decimal(0), "time": NOW, "delay": 3,
         "missed": False, "more": False}
    asyncio.run(bot.notify_binance(PID, [t]))
    assert [c.args[0] for c in bot._send.await_args_list] == [ALLOWED]


def test_delay_command(tmp_path):
    bot = binance_bot(tmp_path)
    with patch("tracker.bot.latency_summary", return_value=None):
        assert "No Binance trades seen yet" in run(bot, bot.cmd_binance_delay)
    with patch("tracker.bot.latency_summary", return_value={"count": 4, "median": 9.0, "p90": 21.0, "max": 40.0}):
        text = run(bot, bot.cmd_binance_delay)
    assert "Typical <b>9s</b>" in text and "within <b>21s</b>" in text


# ---- open positions ---------------------------------------------------------------------

from tracker.binance import rebuild_positions, shown_positions  # noqa: E402
from tracker.formatting import format_binance_positions  # noqa: E402


def closed(symbol, side, closed_ms, status="All Closed", opened=1, avg="100"):
    return {"symbol": symbol, "side": side, "status": status, "opened": opened, "closed": closed_ms,
            "avgCost": avg, "maxOpenInterest": 1, "closedVolume": 1}


def by_coin(positions):
    return {(p["base"], p["long"]): p for p in positions}


def test_open_long_is_added_up_from_trades_since_the_last_one_ended():
    trades = [
        trade(100, qty="5", price="60000"),  # belongs to a position that ended at 150
        trade(160, qty="1", price="70000"),
        trade(170, qty="3", price="74000"),
        trade(180, side="SELL", qty="2", price="75000"),  # a partial close keeps the entry price
    ]
    btc = by_coin(rebuild_positions(trades, [closed("BTCUSDT", "Long", 150)]))[("BTC", True)]
    assert btc["qty"] == 2 and btc["entry"] == 73000 and btc["opened"] == 160


def test_fully_closed_and_reopened_short():
    trades = [trade(1, side="SELL", pos="SHORT", qty="2"), trade(2, side="BUY", pos="SHORT", qty="2"),
              trade(3, side="SELL", pos="SHORT", qty="4", price="50")]
    eth = rebuild_positions(trades, [])
    assert [(p["long"], p["qty"], p["entry"], p["opened"]) for p in eth] == [(False, 4, 50, 3)]


def test_one_way_mode_flips_from_long_to_short():
    trades = [trade(1, pos="BOTH", qty="3", price="10"), trade(2, side="SELL", pos="BOTH", qty="5", price="12")]
    (p,) = rebuild_positions(trades, [])
    assert not p["long"] and p["qty"] == 2 and p["entry"] == 12


def test_position_older_than_the_trades_read_is_listed_with_an_unknown_size():
    # Closing more than the trades opened means it started before them, so the sum is wrong.
    trades = [trade(5, side="SELL", qty="4")]
    history = [closed("BTCUSDT", "Long", None, status="Partially Closed", opened=1, avg="65000")]
    (p,) = rebuild_positions(trades, history)
    assert p["qty"] is None and p["entry"] == 65000 and p["partly_closed"]
    assert rebuild_positions(trades, []) == []  # without the history entry there's nothing to say


def test_nothing_open_when_everything_closed():
    trades = [trade(1, qty="2"), trade(2, side="SELL", qty="2")]
    assert rebuild_positions(trades, [closed("BTCUSDT", "Long", 2)]) == []


def test_shown_positions_keep_mark_and_pnl():
    raw = [{"symbol": "ETHUSDT", "positionAmount": "-9.755", "entryPrice": "2053.01", "markPrice": "1811.2",
            "unrealizedProfit": "2358.81", "leverage": 5, "positionSide": "SHORT"},
           {"symbol": "XRPUSDT", "positionAmount": "0", "entryPrice": "0", "positionSide": "LONG"}]
    (p,) = shown_positions(raw)
    assert not p["long"] and p["qty"] == Decimal("9.755") and p["mark"] == Decimal("1811.2")


def test_positions_message():
    rebuilt = rebuild_positions([trade(NOW - 3 * 3600_000, qty="2", price="70000")], [])
    text = format_binance_positions("熬鹰资本", PID, rebuilt, {"shown": False, "cut_off": True}, NOW)[0]
    assert "🟢 <b>Long 2 BTC</b> · <b>$140,000</b> at entry" in text
    assert "Entry <b>70,000</b> · opened 3 h ago" in text
    assert "hides positions" in text and "some sizes may be off" in text
    shown = shown_positions([{"symbol": "ETHUSDT", "positionAmount": "-1", "entryPrice": "2000",
                              "markPrice": "1800", "unrealizedProfit": "200", "leverage": 5, "positionSide": "SHORT"}])
    text = format_binance_positions("x", PID, shown, {"shown": True, "cut_off": False}, NOW)[0]
    assert "🔴 <b>Short 1 ETH</b> · <b>$1,800</b> · 5x" in text and "🟢 PnL <b>+200</b>" in text
    assert "hides" not in text
    assert "No open positions" in format_binance_positions("x", PID, [], {"shown": True, "cut_off": False}, NOW)[0]


def test_positions_command(tmp_path):
    bot = binance_bot(tmp_path)
    assert handles(bot, ALLOWED, "/binance_positions") and not handles(bot, OTHER, "/binance_positions")
    assert "No Binance traders tracked" in run(bot, bot.cmd_binance_positions)
    bot.storage.track_binance(ALLOWED, PID, "eagle")
    with patch("tracker.bot.load_positions", AsyncMock(return_value=([], {"shown": False, "cut_off": False}))) as load:
        assert "No open positions" in run(bot, bot.cmd_binance_positions, "EAGLE")
    load.assert_awaited_once_with(bot.binance, PID, False)
    assert "matches" in run(bot, bot.cmd_binance_positions, "nobody")
    with patch("tracker.bot.load_positions", AsyncMock(side_effect=BinanceError("busy"))):
        assert "Couldn't load positions for eagle" in run(bot, bot.cmd_binance_positions)
