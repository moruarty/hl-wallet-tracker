"""Telegram commands and wiring between Telegram, storage and the engine."""
import asyncio
import html
import logging
import re
import secrets
from collections import OrderedDict

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, LinkPreviewOptions, Update
from telegram.constants import ParseMode
from telegram.error import Forbidden, RetryAfter, TelegramError
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, filters

from .config import Config
from .engine import Engine, OrderEvent
from .formatting import (
    CoinNames, format_open_orders, format_order_details, format_positions, format_update, short_addr, wallet_link,
)
from .hyperliquid import InfoClient
from .storage import Storage

log = logging.getLogger(__name__)

ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
MAX_WALLETS = 10  # Hyperliquid: max 10 unique users in websocket user subscriptions per IP
SHOW_ALL_PREFIX = "all:"
MAX_SAVED_LISTS = 200  # "Show all" lists kept in memory; older buttons stop working

HELP = (
    "<b>Hyperliquid wallet tracker</b>\n"
    "/track &lt;wallet&gt; [label] · start tracking a wallet in this chat\n"
    "/untrack &lt;wallet or label&gt; · stop tracking\n"
    "/listwallets · wallets tracked in this chat\n"
    "/orders [wallet or label] · current open orders\n"
    "/positions [wallet or label] · current open positions\n"
    "/chatid · show this chat's ID\n\n"
    "You'll get new/canceled/triggered orders (limits, take profits, stop losses) and fills, "
    "batched every few seconds."
)


class TrackerBot:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.storage = Storage(cfg.db_path)
        self.info = InfoClient(cfg.hl_api_url)
        self.coin_names = CoinNames()
        self._saved_lists: OrderedDict[str, tuple[int, list[str]]] = OrderedDict()
        self.engine = Engine(self.info, cfg.hl_ws_url, self.notify, cfg.batch_interval)
        self.app = (
            Application.builder()
            .token(cfg.bot_token)
            .post_init(self._post_init)
            .post_shutdown(self._post_shutdown)
            .build()
        )
        allowed = filters.Chat(chat_id=list(cfg.allowed_chat_ids))
        for name, fn in [
            ("start", self.cmd_help), ("help", self.cmd_help), ("track", self.cmd_track),
            ("untrack", self.cmd_untrack), ("listwallets", self.cmd_list), ("orders", self.cmd_orders),
            ("positions", self.cmd_positions),
        ]:
            self.app.add_handler(CommandHandler(name, fn, filters=allowed))
        # /chatid works everywhere so you can find the ID to put in ALLOWED_CHAT_IDS.
        self.app.add_handler(CommandHandler("chatid", self.cmd_chatid))
        # block=False: a long list shouldn't hold up commands while it sends.
        self.app.add_handler(CallbackQueryHandler(self.on_show_all, pattern=f"^{SHOW_ALL_PREFIX}", block=False))

    def run(self) -> None:
        self.app.run_polling(allowed_updates=[Update.MESSAGE, Update.CHANNEL_POST, Update.CALLBACK_QUERY])

    # ---- lifecycle -------------------------------------------------------
    async def _post_init(self, app: Application) -> None:
        try:
            self.coin_names.load_spot_meta(await self.info.spot_meta())
        except Exception:
            log.warning("Could not load spot metadata; spot coins will show as @id", exc_info=True)
        await app.bot.set_my_commands([
            BotCommand("track", "Track a wallet: /track <wallet> [label]"),
            BotCommand("untrack", "Stop tracking: /untrack <wallet or label>"),
            BotCommand("listwallets", "List tracked wallets"),
            BotCommand("orders", "Open orders: /orders [wallet or label]"),
            BotCommand("positions", "Open positions: /positions [wallet or label]"),
            BotCommand("chatid", "Show this chat's ID"),
            BotCommand("help", "Help"),
        ])
        self.engine.start()
        await self._sync()
        log.info("Bot started; tracking %d wallet(s)", len({w.address for w in self.storage.all()}))

    async def _post_shutdown(self, app: Application) -> None:
        await self.engine.stop()
        await self.info.close()

    async def _sync(self) -> None:
        await self.engine.sync({w.address for w in self.storage.all()})

    # ---- notifications -------------------------------------------------
    async def notify(self, address: str, fills: list[dict], orders: list[OrderEvent]) -> None:
        for w in self.storage.all():
            if w.address != address or w.chat_id not in self.cfg.allowed_chat_ids:
                continue
            pages = format_update(w.label, address, fills, orders, self.coin_names)
            details = format_order_details(w.label, address, orders, self.coin_names)
            button = self._show_all_button(w.chat_id, details) if details else None
            for i, text in enumerate(pages):
                await self._send(w.chat_id, text, reply_markup=button if i == len(pages) - 1 else None)

    def _show_all_button(self, chat_id: int, pages: list[str]) -> InlineKeyboardMarkup:
        """Save the full list and return a button that sends it. Lists are lost on restart."""
        token = secrets.token_hex(8)
        self._saved_lists[token] = (chat_id, pages)
        while len(self._saved_lists) > MAX_SAVED_LISTS:
            self._saved_lists.popitem(last=False)
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("Show all orders", callback_data=SHOW_ALL_PREFIX + token)]]
        )

    async def on_show_all(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        chat_id = update.effective_chat.id
        saved = self._saved_lists.get(query.data.removeprefix(SHOW_ALL_PREFIX))
        if chat_id not in self.cfg.allowed_chat_ids or saved is None or saved[0] != chat_id:
            await query.answer("This list has expired. Use /orders for a fresh one.", show_alert=True)
            return
        await query.answer()
        try:
            await query.edit_message_reply_markup(None)  # stops a double tap sending it twice
        except TelegramError:
            log.warning("Could not remove the button in chat %s", chat_id, exc_info=True)
        for page in saved[1]:
            await self._send(chat_id, page)

    async def _send(self, chat_id: int, text: str, attempts: int = 3, reply_markup=None) -> None:
        failures = 0
        while failures < attempts:
            try:
                await self.app.bot.send_message(
                    chat_id, text, parse_mode=ParseMode.HTML,
                    link_preview_options=LinkPreviewOptions(is_disabled=True), reply_markup=reply_markup,
                )
                return
            except RetryAfter as e:
                # Flood control isn't a failure: wait as told and keep the update.
                wait = e.retry_after
                seconds = wait.total_seconds() if hasattr(wait, "total_seconds") else float(wait)
                log.warning("Telegram flood control for chat %s; waiting %.0fs", chat_id, seconds)
                await asyncio.sleep(seconds + 0.5)
            except Forbidden:
                log.warning("Bot can't post in chat %s (removed or blocked)", chat_id)
                return
            except TelegramError:
                failures += 1
                log.exception("Failed to send to chat %s (attempt %d/%d)", chat_id, failures, attempts)
                await asyncio.sleep(1)

    # ---- commands --------------------------------------------------------
    async def _reply(self, update: Update, text: str, reply_markup=None) -> None:
        await update.effective_message.reply_text(
            text, parse_mode=ParseMode.HTML, link_preview_options=LinkPreviewOptions(is_disabled=True),
            reply_markup=reply_markup,
        )

    async def cmd_help(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        await self._reply(update, HELP)

    async def cmd_chatid(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        allowed = "allowed" if chat_id in self.cfg.allowed_chat_ids else "not in ALLOWED_CHAT_IDS"
        await self._reply(update, f"Chat ID: <code>{chat_id}</code> ({allowed})")

    async def cmd_track(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not ctx.args:
            await self._reply(update, "Usage: /track &lt;wallet&gt; [label]")
            return
        address = ctx.args[0].strip()
        if not ADDRESS_RE.match(address):
            await self._reply(update, "That doesn't look like a wallet address (0x followed by 40 hex characters).")
            return
        address = address.lower()
        label = " ".join(ctx.args[1:]).strip() or short_addr(address)
        chat_id = update.effective_chat.id
        if self.storage.label_taken(chat_id, label, address):
            await self._reply(update, f"The label <b>{html.escape(label)}</b> is already used in this chat.")
            return
        tracked = {w.address for w in self.storage.all()}
        if address not in tracked and len(tracked) >= MAX_WALLETS:
            await self._reply(update, f"Hyperliquid allows live tracking of at most {MAX_WALLETS} wallets per server. "
                                      "Untrack one first.")
            return
        is_new = self.storage.track(chat_id, address, label)
        await self._sync()
        verb = "Now tracking" if is_new else "Updated label for"
        await self._reply(update, f"{verb} <b>{html.escape(label)}</b> (<code>{address}</code>).")

    async def cmd_untrack(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not ctx.args:
            await self._reply(update, "Usage: /untrack &lt;wallet or label&gt;")
            return
        removed = self.storage.untrack(update.effective_chat.id, " ".join(ctx.args))
        if not removed:
            await self._reply(update, "No tracked wallet in this chat matches that.")
            return
        await self._sync()
        names = ", ".join(f"<b>{html.escape(w.label)}</b>" for w in removed)
        await self._reply(update, f"Stopped tracking {names}.")

    async def cmd_list(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        wallets = self.storage.list_for_chat(update.effective_chat.id)
        if not wallets:
            await self._reply(update, "No wallets tracked in this chat. Use /track &lt;wallet&gt; [label].")
            return
        lines = [f"<b>Tracked wallets ({len(wallets)})</b>"]
        lines += [f"• {wallet_link(w.label, w.address)}\n  <code>{w.address}</code>" for w in wallets]
        await self._reply(update, "\n".join(lines))

    async def _matching_wallets(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> list:
        """The wallet named in the command, or all of this chat's wallets. Replies if there are none."""
        wallets = self.storage.list_for_chat(update.effective_chat.id)
        key = " ".join(ctx.args).strip().lower()
        if key:
            wallets = [w for w in wallets if w.address == key or w.label.lower() == key]
        if not wallets:
            await self._reply(update, "No tracked wallet in this chat matches that.")
        return wallets

    async def cmd_positions(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        for w in await self._matching_wallets(update, ctx):
            try:
                states = await self.info.clearinghouse_states(w.address)
            except Exception:
                log.exception("Loading positions failed")
                await self._reply(update, f"Couldn't load positions for {html.escape(w.label)} right now.")
                continue
            for text in format_positions(w.label, w.address, states, self.coin_names):
                await self._reply(update, text)

    async def cmd_orders(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        for w in await self._matching_wallets(update, ctx):
            try:
                orders = await self.info.open_orders(w.address)
            except Exception:
                log.exception("Loading open orders failed")
                await self._reply(update, f"Couldn't load orders for {html.escape(w.label)} right now.")
                continue
            pages = format_open_orders(w.label, w.address, orders, self.coin_names)
            full = format_open_orders(w.label, w.address, orders, self.coin_names, expand=True)
            button = self._show_all_button(update.effective_chat.id, full) if full != pages else None
            for i, text in enumerate(pages):
                await self._reply(update, text, reply_markup=button if i == len(pages) - 1 else None)
