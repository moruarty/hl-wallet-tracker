"""Telegram side, without any network: chat gating, commands and flood-control handling."""
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from telegram import Chat, Message, MessageEntity, Update, User
from telegram.error import RetryAfter, TimedOut
from telegram.ext import CommandHandler

from tracker.bot import MAX_WALLETS, TrackerBot
from tracker.config import Config

ALLOWED, OTHER = 111, 222
WALLET = "0x" + "ab" * 20


def make_bot(tmp_path, allowed=frozenset({ALLOWED})):
    cfg = Config(
        bot_token="123:abc", allowed_chat_ids=allowed, batch_interval=5.0,
        db_path=str(tmp_path / "t.db"), hl_api_url="http://unused", hl_ws_url="ws://unused", log_level="INFO",
    )
    b = TrackerBot(cfg)
    b.app.bot._bot_user = User(99, "tracker", True, username="tracker_bot")  # normally from get_me()
    b.engine.sync = AsyncMock()
    b._reply = AsyncMock()
    return b


@pytest.fixture
def bot(tmp_path):
    return make_bot(tmp_path)


def handles(bot, chat_id, text):
    msg = Message(
        1, datetime.now(timezone.utc), Chat(chat_id, "private"), from_user=User(1, "u", False), text=text,
        entities=[MessageEntity("bot_command", 0, len(text.split()[0]))],
    )
    msg.set_bot(bot.app.bot)
    update = Update(1, message=msg)
    return any(
        h.check_update(update) not in (None, False)
        for h in bot.app.handlers[0] if isinstance(h, CommandHandler)
    )


def reply_text(bot):
    return bot._reply.call_args.args[1]


def run_cmd(fn, chat_id, *args):
    update = SimpleNamespace(effective_chat=SimpleNamespace(id=chat_id))
    asyncio.run(fn(update, SimpleNamespace(args=list(args))))


@pytest.mark.parametrize("cmd", ["/track", "/untrack", "/listwallets", "/orders", "/help", "/start"])
def test_commands_only_work_in_allowed_chats(bot, cmd):
    assert handles(bot, ALLOWED, cmd)
    assert not handles(bot, OTHER, cmd)


def test_chatid_works_everywhere_so_ids_can_be_found(bot):
    assert handles(bot, OTHER, "/chatid")


def test_empty_allowlist_locks_every_command_but_chatid(tmp_path):
    b = make_bot(tmp_path, allowed=frozenset())
    assert not handles(b, ALLOWED, "/track")
    assert handles(b, ALLOWED, "/chatid")


def test_track_list_untrack_by_label_and_wallet(bot):
    run_cmd(bot.cmd_track, ALLOWED, "0x" + "AB" * 20, "big", "whale")
    assert "Now tracking" in reply_text(bot)
    bot.engine.sync.assert_awaited_with({WALLET})  # address normalised to lowercase

    run_cmd(bot.cmd_list, ALLOWED)
    assert "big whale" in reply_text(bot) and WALLET in reply_text(bot)

    run_cmd(bot.cmd_untrack, ALLOWED, "BIG", "WHALE")
    assert "Stopped tracking" in reply_text(bot)
    bot.engine.sync.assert_awaited_with(set())

    run_cmd(bot.cmd_track, ALLOWED, WALLET)
    run_cmd(bot.cmd_untrack, ALLOWED, WALLET)
    assert "Stopped tracking" in reply_text(bot)


def test_track_rejects_bad_address_and_duplicate_label(bot):
    run_cmd(bot.cmd_track, ALLOWED, "0x123")
    assert "doesn't look like" in reply_text(bot)
    run_cmd(bot.cmd_track, ALLOWED, WALLET, "main")
    run_cmd(bot.cmd_track, ALLOWED, "0x" + "cd" * 20, "MAIN")
    assert "already used" in reply_text(bot)


def test_label_with_html_is_escaped(bot):
    run_cmd(bot.cmd_track, ALLOWED, WALLET, "<b>x")
    assert "&lt;b&gt;x" in reply_text(bot)


def test_each_chat_has_its_own_list(bot):
    run_cmd(bot.cmd_track, ALLOWED, WALLET, "mine")
    run_cmd(bot.cmd_list, OTHER)
    assert "No wallets" in reply_text(bot)
    run_cmd(bot.cmd_untrack, OTHER, "mine")
    assert "No tracked wallet" in reply_text(bot)
    assert len(bot.storage.all()) == 1


def test_wallet_cap_counts_unique_wallets(bot):
    for i in range(MAX_WALLETS):
        run_cmd(bot.cmd_track, ALLOWED, f"0x{i:040x}", f"w{i}")
    run_cmd(bot.cmd_track, ALLOWED, "0x" + "f" * 40, "extra")
    assert "at most" in reply_text(bot)
    run_cmd(bot.cmd_track, OTHER, f"0x{0:040x}", "shared")  # same wallet in another chat is fine
    assert "Now tracking" in reply_text(bot)


def test_notify_skips_chats_no_longer_allowed(bot):
    bot.storage.track(ALLOWED, WALLET, "a")
    bot.storage.track(OTHER, WALLET, "b")
    bot._send = AsyncMock()
    f = {"coin": "ETH", "px": "1", "sz": "1", "side": "B", "dir": "Open Long", "oid": 1, "tid": 1, "time": 1}
    asyncio.run(bot.notify(WALLET, [f], []))
    assert [c.args[0] for c in bot._send.await_args_list] == [ALLOWED]


@pytest.mark.parametrize("wait", [2, timedelta(seconds=2)])  # the library is moving int -> timedelta
def test_flood_control_waits_and_still_delivers(bot, wait):
    err = type("Flood", (RetryAfter,), {"retry_after": wait})(2)
    send = AsyncMock(side_effect=[err, err, err, err, None])
    with patch.object(type(bot.app.bot), "send_message", send), patch("asyncio.sleep", AsyncMock()) as sleep:
        asyncio.run(bot._send(ALLOWED, "hi"))
    assert send.await_count == 5  # more flood waits than the failure limit, and nothing was lost
    assert sleep.await_args.args[0] == 2.5


def test_send_gives_up_after_repeated_errors(bot):
    send = AsyncMock(side_effect=TimedOut("slow"))
    with patch.object(type(bot.app.bot), "send_message", send), patch("asyncio.sleep", AsyncMock()):
        asyncio.run(bot._send(ALLOWED, "hi"))
    assert send.await_count == 3
