"""The /positions command, using payloads in the shape the live API returns."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from tests.test_bot import ALLOWED, OTHER, WALLET, handles, make_bot
from tracker.formatting import CoinNames, format_positions, format_update
from tracker.hyperliquid import InfoClient

NAMES = CoinNames()

SHORT_ETH = {
    "coin": "ETH", "szi": "-1000.0", "leverage": {"type": "cross", "value": 25}, "entryPx": "2283.87",
    "positionValue": "2321000.0", "unrealizedPnl": "-37120.017516", "returnOnEquity": "-0.4063262715",
    "liquidationPx": "6201.6534000676", "marginUsed": "92840.0",
}
LONG_XYZ = {
    "coin": "xyz:COIN", "szi": "7000.0", "leverage": {"type": "isolated", "value": 10, "rawUsd": "1"},
    "entryPx": "142.978", "positionValue": "3008490.0", "unrealizedPnl": "7641.39185",
    "returnOnEquity": "0.076349128", "liquidationPx": None, "marginUsed": "93771.27",
}


def state(account, *positions):
    return {"marginSummary": {"accountValue": account},
            "assetPositions": [{"type": "oneWay", "position": p} for p in positions]}


def test_position_details_and_totals_across_dexes():
    text = format_positions("me", WALLET, [state("1000", SHORT_ETH), state("500", LONG_XYZ)], NAMES)[0]
    assert "<u>Positions (2)</u>" in text
    assert "Value <b>$5,329,490</b> · 🔴 PnL <b>-29,478.63</b> · Perp account <b>$1,500</b>" in text
    assert "📉 <b>Short 1,000 ETH</b> · <b>$2,321,000</b> · 25x cross" in text
    assert "Entry <b>2,283.87</b> · Mark <b>2,321</b>\n" in text
    assert "🔴 PnL <b>-37,120.02</b> (-40.6%)" in text
    assert "🟠 Liq <b>6,201.65</b>" in text and "167" not in text  # distance is hidden when it's over 100%
    assert "📈 <b>Long 7,000 xyz:COIN</b> · <b>$3,008,490</b> · 10x isolated" in text
    assert "🟢 PnL <b>+7,641.39</b> (+7.6%)" in text


def test_color_follows_profit_not_direction():
    winning_short = dict(SHORT_ETH, unrealizedPnl="500", returnOnEquity="0.1")
    text = format_positions("me", WALLET, [state("1", winning_short)], NAMES)[0]
    assert "📉 <b>Short" in text and "🟢 PnL <b>+500</b>" in text and "🔴" not in text


def test_tiny_pnl_keeps_its_digits_and_fill_price_is_readable():
    tiny = dict(SHORT_ETH, unrealizedPnl="-0.00823")
    assert "🔴 PnL <b>-0.00823</b>" in format_positions("me", WALLET, [state("1", tiny)], NAMES)[0]
    fills = [{"coin": "ETH", "px": px, "sz": "1", "side": "A", "dir": "Close Long", "oid": 1, "tid": t,
              "time": 1, "closedPnl": "0", "fee": "0", "feeToken": "USDC"}
             for t, px in ((1, "534.35"), (2, "534.34"), (3, "534.35"))]
    assert "@ <b>534.347</b> (3 fills)" in format_update("me", WALLET, fills, [], NAMES)[0]


def test_warning_only_when_close_to_liquidation():
    near = dict(SHORT_ETH, positionValue="2000000", szi="-1000", liquidationPx="2090")
    far = dict(SHORT_ETH, positionValue="2000000", szi="-1000", liquidationPx="3000")
    assert "(4.5% away) ⚠️" in format_positions("me", WALLET, [state("1", near)], NAMES)[0]
    assert "⚠️" not in format_positions("me", WALLET, [state("1", far)], NAMES)[0]


def test_fill_colors_its_pnl_and_bolds_numbers():
    fill = {"coin": "ETH", "px": "3000", "sz": "2", "side": "B", "dir": "Close Short", "oid": 1,
            "tid": 1, "time": 1, "closedPnl": "-12.5", "fee": "0.1", "feeToken": "USDC"}
    text = format_update("me", WALLET, [fill], [], NAMES)[0]
    assert "📈 <b>Close Short 2 ETH</b> @ <b>3,000</b>" in text
    assert "<b>$6,000</b> · 🔴 PnL <b>-12.5</b> · fee 0.1 USDC" in text


def test_largest_position_comes_first():
    text = format_positions("me", WALLET, [state("1", SHORT_ETH), state("1", LONG_XYZ)], NAMES)[0]
    assert text.index("xyz:COIN") < text.index("ETH")


def test_position_without_a_liquidation_price_omits_it():
    text = format_positions("me", WALLET, [state("1", LONG_XYZ)], NAMES)[0]
    assert "Liq" not in text


def test_small_prices_keep_their_digits():
    tiny = dict(SHORT_ETH, entryPx="0.004313", positionValue="2.1", szi="-500", liquidationPx="0.0051234")
    text = format_positions("me", WALLET, [state("1", tiny)], NAMES)[0]
    assert "Entry <b>0.004313</b>" in text and "Mark <b>0.0042</b>" in text and "Liq <b>0.0051234</b>" in text


def test_no_positions_still_shows_account_value():
    text = format_positions("me", WALLET, [state("29.78"), state("0.0")], NAMES)[0]
    assert "No open positions. Perp account <b>$29.78</b>." in text


def test_many_positions_split_into_messages_within_telegrams_limit():
    states = [state("1", *[dict(SHORT_ETH, coin=f"C{i}") for i in range(200)])]
    pages = format_positions("me", WALLET, states, NAMES)
    assert len(pages) > 1 and all(len(p) <= 4096 for p in pages)
    assert sum(p.count("<b>Short ") for p in pages) == 200


def test_client_asks_for_main_dex_and_xyz():
    info = InfoClient("http://unused")
    calls = []

    async def post(payload):
        calls.append(payload)
        return state("1")

    info._post = post
    assert len(asyncio.run(info.clearinghouse_states(WALLET))) == 2
    assert {c["dex"] for c in calls} == {"", "xyz"}
    assert all(c["type"] == "clearinghouseState" and c["user"] == WALLET for c in calls)


def test_positions_command_is_gated_to_allowed_chats(tmp_path):
    bot = make_bot(tmp_path)
    assert handles(bot, ALLOWED, "/positions")
    assert not handles(bot, OTHER, "/positions")


def run_positions(bot, *args):
    update = SimpleNamespace(effective_chat=SimpleNamespace(id=ALLOWED))
    asyncio.run(bot.cmd_positions(update, SimpleNamespace(args=list(args))))


def test_positions_command_filters_by_label(tmp_path):
    bot = make_bot(tmp_path)
    other = "0x" + "cd" * 20
    bot.storage.track(ALLOWED, WALLET, "main")
    bot.storage.track(ALLOWED, other, "second")
    bot.info.clearinghouse_states = AsyncMock(return_value=[state("1", SHORT_ETH)])

    run_positions(bot, "SECOND")
    bot.info.clearinghouse_states.assert_awaited_once_with(other)

    bot.info.clearinghouse_states.reset_mock()
    run_positions(bot)
    assert bot.info.clearinghouse_states.await_count == 2

    run_positions(bot, "nobody")
    assert "No tracked wallet" in bot._reply.call_args.args[1]


def test_positions_command_reports_api_failure_and_carries_on(tmp_path):
    bot = make_bot(tmp_path)
    bot.storage.track(ALLOWED, WALLET, "main")
    bot.info.clearinghouse_states = AsyncMock(side_effect=RuntimeError("down"))
    run_positions(bot)
    assert "Couldn't load positions for main" in bot._reply.call_args.args[1]
