# Hyperliquid wallet tracker (Telegram bot)

## Deploy changes (quick reference)

Run these in Git Bash from the project folder.

**1. Commit your changes:**

```bash
git add -A
```

```bash
git commit -m "Describe your change"
```

**2. Send them to GitHub and the VM, and restart the bot:**

```bash
bash deploy/sync.sh
```

That one command pushes to GitHub, pulls the code on the VM, installs any new dependencies and restarts
the bot, so there's no separate stop and start. It stops if you have uncommitted changes, and it prints
the logs if the bot doesn't come back up.

Only one copy of the bot can use a token at a time, so stop any local copy first with `Ctrl+C`.

**To push to GitHub without deploying:**

```bash
git push origin main
```

**To stop or start the bot by hand,** connect to the VM:

```bash
gcloud compute ssh hl-tracker --zone=us-central1-a
```

Then run whichever you need there:

```bash
sudo systemctl restart hl-tracker
```

```bash
sudo systemctl disable --now hl-tracker
```

```bash
sudo systemctl enable --now hl-tracker
```

`disable --now` stops the bot and keeps it off after a reboot, which is what you want while testing locally.
`enable --now` turns it back on. `restart` is also what you need after editing the VM's `.env` with
`sudo nano /opt/hl-wallet-tracker/.env`, because `.env` isn't in Git and `sync.sh` doesn't touch it.

**To check it's healthy,** on the VM:

```bash
sudo journalctl -u hl-tracker -n 30 --no-pager
```

**To test before deploying,** on your machine:

```bash
.venv/Scripts/python -m pytest
```

```bash
.venv/Scripts/python -m tracker
```

The second one runs the bot locally, so turn the VM's copy off first with `disable --now`, or it will conflict.

## About

A Telegram bot that watches Hyperliquid wallets and posts:

- **Orders**: new, canceled, triggered and rejected orders, labelled as limit, reduce-only limit,
  take profit or stop loss (market/limit, with trigger price).
- **Fills**: every fill, with direction (Open Long, Close Short, ...), average price, notional,
  closed PnL and fees. Liquidations are flagged.

Events are **batched every 5 seconds** (configurable): one message per wallet per batch, and partial
fills of the same order are merged into one line, so a burst of fills doesn't spam the chat.
A market order shows only as its fill. IOC and post-only orders rejected on arrival are hidden, because
they never rested on the book.

**Scaled orders:** 3 or more similar limit orders in a list become one line with the size-weighted
average price, price range and total size. A **Show all orders** button sends the full list. Orders are
similar if they share a coin, side and type, and each batch is grouped on its own, so a scale spanning two
batches shows as two lines. TP/SL and other trigger orders are always listed one by one. Buttons stop
working after a restart, or once 200 newer lists have been saved; `/orders` gives a fresh list.

Wallet names and addresses link to the wallet's [Hypurrscan](https://hypurrscan.io) page.

Access is gated by chat ID: commands only work in chats listed in `ALLOWED_CHAT_IDS`.

## Commands

| Command | What it does |
|---|---|
| `/track <wallet> [label]` | Track a wallet in this chat. Running it again with a new label renames it. |
| `/untrack <wallet or label>` | Stop tracking in this chat. |
| `/listwallets` | Wallets tracked in this chat. |
| `/orders [wallet or label]` | Current open orders (all tracked wallets if no argument). |
| `/positions [wallet or label]` | Current perp positions with entry, mark, value, PnL and liquidation price. |
| `/chatid` | Shows the chat's ID. Works in any chat so you can set up `ALLOWED_CHAT_IDS`. |
| `/help` | Command list. |

Each chat has its own wallet list and labels. If two chats track the same wallet, both get the updates.

## How it works

- One Hyperliquid websocket per wallet, subscribed to `orderUpdates` and `userFills`
  ([docs](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)).
  A ping goes out every 30 s, and a connection that goes quiet is replaced, with backoff.
- Order type details (TP/SL, trigger price, reduce-only) aren't in the websocket payload, so each order
  is looked up once with `orderStatus`, which also works after the order has closed.
- On connect, existing open orders and the fill snapshot are loaded silently, so you aren't flooded
  with history. After a reconnect, fills, new orders and cancels from the gap are reported.
- Tracked wallets are stored in SQLite (`data/tracker.db`).

**Limits**

- Hyperliquid allows 10 unique users in websocket subscriptions per IP, so the bot tracks up to
  **10 wallets per server**, however many people use it. Going beyond this needs a polling fallback
  or more IPs.
- The bot is built for normal wallets. A market maker works, but it produces several long messages per
  batch and can hit Hyperliquid's REST rate limit, which leaves some orders without a type label.
- `/orders` covers the main perp dex, spot and the `xyz` HIP-3 dex (see `OPEN_ORDER_DEXES` in
  `tracker/hyperliquid.py`). Open orders on other HIP-3 dexes aren't listed, although their live order
  events and fills are reported.
- `/positions` covers perps on the main dex and `xyz`. Spot balances and other HIP-3 dexes aren't included.

## Project layout

```
tracker/
  __main__.py     entry point (python -m tracker)
  config.py       .env loading
  bot.py          Telegram commands and sending
  engine.py       per-wallet state, event detection, 5 s batching
  hyperliquid.py  websocket + info API clients
  formatting.py   message formatting
  storage.py      SQLite wallet list
deploy/           systemd unit and sync.sh
tests/            engine, formatting and Telegram command tests
```

## Ideas for later

- Funding payments and liquidation alerts via the `userEvents` channel
- Filters per wallet (only fills, minimum notional, specific coins)
- More HIP-3 dexes in `/orders` and `/positions`
- Polling fallback to track more than 10 wallets
