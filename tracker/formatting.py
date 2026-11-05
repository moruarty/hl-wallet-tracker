"""Turns batched fills and order events into Telegram HTML messages."""
import html
import re
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext

from .engine import OrderEvent

ADDRESS_URL = "https://hypurrscan.io/address/{}"
TG_LIMIT = 4000  # Telegram hard limit is 4096 characters
GROUP_MIN_ORDERS = 3  # this many similar orders in a list are summarised as one scaled order
LIQ_WARNING_PERCENT = 5  # flag a position whose mark is this close to its liquidation price


class CoinNames:
    """Maps spot ids like "@107" to readable pairs like "HYPE/USDC"."""

    def __init__(self) -> None:
        self._spot: dict[str, str] = {}

    def load_spot_meta(self, meta: dict) -> None:
        tokens = {t["index"]: t["name"] for t in meta.get("tokens", [])}
        for pair in meta.get("universe", []):
            base, quote = pair["tokens"][0], pair["tokens"][1]
            self._spot[f"@{pair['index']}"] = f"{tokens.get(base, base)}/{tokens.get(quote, quote)}"
            self._spot[pair["name"]] = f"{tokens.get(base, base)}/{tokens.get(quote, quote)}"

    def __call__(self, coin: str) -> str:
        return self._spot.get(coin, coin)


def num(value, places: int | None = None) -> str:
    try:
        d = Decimal(str(value))
    except InvalidOperation:
        return str(value)
    if places is not None:
        d = d.quantize(Decimal(1).scaleb(-places))
    return format(d.normalize() if d else abs(d).normalize(), ",f")  # never "-0"


def short_addr(addr: str) -> str:
    return f"{addr[:6]}…{addr[-4:]}"


def wallet_link(label: str, address: str) -> str:
    """The wallet's name and short address, both linking to its Hypurrscan page."""
    short = short_addr(address)
    name = f"<b>{html.escape(label)}</b> · {short}" if label != short else f"<b>{short}</b>"
    return f'<a href="{ADDRESS_URL.format(address)}">{name}</a>'


def humanize_status(status: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", " ", status).lower()


def side_word(side: str) -> str:
    return "Buy" if side == "B" else "Sell"


def _aggregate_fills(fills: list[dict]) -> list[dict]:
    """Merge partial fills of the same order into one line (size-weighted price)."""
    groups: dict[tuple, dict] = {}
    for f in fills:
        key = (f["coin"], f["oid"], f["dir"])
        sz, px = Decimal(f["sz"]), Decimal(f["px"])
        g = groups.get(key)
        if g is None:
            groups[key] = g = {
                "coin": f["coin"], "dir": f["dir"], "side": f["side"], "sz": Decimal(0),
                "notional": Decimal(0), "fee": Decimal(0), "pnl": Decimal(0), "count": 0,
                "feeToken": f.get("feeToken", "USDC"), "liq": False, "crossed": f.get("crossed"),
            }
        g["sz"] += sz
        g["notional"] += sz * px
        g["fee"] += Decimal(f.get("fee", "0"))
        g["pnl"] += Decimal(f.get("closedPnl", "0"))
        g["count"] += 1
        g["liq"] = g["liq"] or bool(f.get("liquidation"))
    return list(groups.values())


def _signed(value: Decimal, places: int) -> str:
    return f"{'+' if value > 0 else ''}{num(value, places)}"


def _pnl(value: Decimal) -> str:
    """Telegram can't color text, so a colored dot next to the number does that job."""
    dot = "🟢" if value > 0 else "🔴" if value < 0 else "⚪"
    # Amounts under a cent keep their digits, or a small loss would read "🔴 PnL 0".
    text = _signed(value, 2) if abs(value) >= Decimal("0.01") else f"{'+' if value > 0 else ''}{_price(value)}"
    return f"{dot} PnL <b>{text}</b>"


def format_fill(g: dict, coin_name) -> str:
    avg_px = g["notional"] / g["sz"] if g["sz"] else Decimal(0)
    icon = "🔥" if g["liq"] else ("📈" if g["side"] == "B" else "📉")
    parts = [
        f"{icon} <b>{html.escape(g['dir'])} {num(g['sz'])} {html.escape(coin_name(g['coin']))}</b> "
        f"@ <b>{_price(avg_px)}</b>"
    ]
    if g["count"] > 1:
        parts[0] += f" ({g['count']} fills)"
    details = [f"<b>${num(g['notional'], 2)}</b>"]
    if g["pnl"] != 0:
        details.append(_pnl(g["pnl"]))
    details.append(f"fee {num(g['fee'], 4)} {html.escape(g['feeToken'])}")
    if g["liq"]:
        details.append("liquidation")
    return parts[0] + "\n      " + " · ".join(details)


def order_kind(info: dict | None) -> str:
    if not info:
        return "order"
    ot = info.get("orderType", "Limit")
    if info.get("isTrigger") or "Take Profit" in ot or "Stop" in ot:
        # A trigger that isn't reduce-only opens a position, so it isn't a real TP or SL.
        closes_position = info.get("reduceOnly") or info.get("isPositionTpsl")
        if closes_position and "Take Profit" in ot:
            name = "Take profit"
        elif closes_position and "Stop" in ot:
            name = "Stop loss"
        else:
            name = "Trigger"
        name += " (market)" if "Market" in ot else " (limit)"
        if info.get("isPositionTpsl"):
            name += ", position TP/SL"
        return name
    name = f"{ot} order"
    if info.get("reduceOnly"):
        name += " (reduce-only)"
    return name


ORDER_ICONS = {"new": "📝 New", "canceled": "❌ Canceled", "triggered": "⚡ Triggered", "rejected": "⛔ Rejected"}


@dataclass
class OrderGroup:
    """Similar orders shown as one line, typically a scaled entry or exit."""
    events: list[OrderEvent]


def _event_size(e: OrderEvent):
    o = e.order
    return o.get("origSz") if e.kind != "new" and o.get("origSz") else o.get("sz")


def _group_key(e: OrderEvent) -> tuple | None:
    """Orders with the same key can be summarised. Triggers can't: their prices are the point."""
    if e.kind not in ("new", "canceled") or (e.info or {}).get("isTrigger"):
        return None
    return e.kind, e.status, order_kind(e.info), e.order["coin"], e.order["side"]


def group_orders(events: list[OrderEvent]) -> list[OrderEvent | OrderGroup]:
    """Replace each set of similar orders with one group, placed where its first order was."""
    buckets = defaultdict(list)
    for e in events:
        key = _group_key(e)
        if key is not None:
            buckets[key].append(e)
    items, placed = [], set()
    for e in events:
        key = _group_key(e)
        if key is None or len(buckets[key]) < GROUP_MIN_ORDERS:
            items.append(e)
        elif key not in placed:
            placed.add(key)
            items.append(OrderGroup(buckets[key]))
    return items


def format_group(group: OrderGroup, coin_name) -> str:
    first = group.events[0]
    sizes = [Decimal(_event_size(e)) for e in group.events]
    prices = [Decimal(e.order["limitPx"]) for e in group.events]
    total = sum(sizes)
    notional = sum(s * p for s, p in zip(sizes, prices))
    # Weighted by size, which is what the average entry works out to if everything fills.
    avg = notional / total if total else sum(prices) / len(prices)
    # One decimal place beyond the prices themselves: 78,293.8 rather than 78,293.777322.
    avg_places = max(max(-p.normalize().as_tuple().exponent for p in prices), 0) + 1
    kind = order_kind(first.info).replace("order", "orders", 1)
    line = (
        f"{ORDER_ICONS[first.kind]} {len(group.events)} {kind}: <b>{side_word(first.order['side'])} "
        f"{num(total)} {html.escape(coin_name(first.order['coin']))}</b> · avg <b>{num(avg, avg_places)}</b> "
        f"· range {num(min(prices))} – {num(max(prices))} · ${num(notional, 2)}"
    )
    if first.status not in ("open", "canceled"):
        line += f" <i>({humanize_status(first.status)})</i>"
    return line


def format_order(e: OrderEvent, coin_name) -> str:
    o, info = e.order, e.info or {}
    sz = _event_size(e)
    line = (
        f"{ORDER_ICONS[e.kind]} {order_kind(e.info)}: <b>{side_word(o['side'])} "
        f"{num(sz)} {html.escape(coin_name(o['coin']))}</b>"
    )
    is_trigger = info.get("isTrigger")
    if is_trigger and info.get("triggerPx"):
        # The condition reads "Price above 89.1", so only its direction adds anything.
        direction = re.search(r"above|below", info.get("triggerCondition") or "")
        word = f"trigger {direction.group()}" if direction else "trigger"
        line += f" · {word} <b>{num(info['triggerPx'])}</b>"
        if "Limit" in info.get("orderType", ""):
            line += f" · limit <b>{num(o['limitPx'])}</b>"
    else:
        line += f" @ <b>{num(o['limitPx'])}</b> · ${num(Decimal(sz) * Decimal(o['limitPx']), 2)}"
    if e.status not in ("open", "canceled", "triggered", "rejected"):
        line += f" <i>({humanize_status(e.status)})</i>"
    return line


def format_update(label: str, address: str, fills: list[dict], orders: list[OrderEvent], coin_name) -> list[str]:
    lines = [wallet_link(label, address)]
    agg = _aggregate_fills(fills)
    if agg:
        lines.append("")
        lines.append(f"<u>Fills ({len(fills)})</u>")
        lines.extend(format_fill(g, coin_name) for g in agg)
    if orders:
        lines.append("")
        lines.append("<u>Orders</u>")
        lines.extend(
            format_group(item, coin_name) if isinstance(item, OrderGroup) else format_order(item, coin_name)
            for item in group_orders(orders)
        )
    return split_message(lines)


def format_order_details(label: str, address: str, orders: list[OrderEvent], coin_name) -> list[str]:
    """Every order in a batch, one per line. Empty if the batch had nothing summarised."""
    if not any(isinstance(item, OrderGroup) for item in group_orders(orders)):
        return []
    lines = [wallet_link(label, address), f"<u>All orders from that update ({len(orders)})</u>"]
    lines.extend(format_order(e, coin_name) for e in orders)
    return split_message(lines)


def split_message(lines: list[str]) -> list[str]:
    chunks, cur = [], ""
    for line in lines:
        if cur and len(cur) + len(line) + 1 > TG_LIMIT:
            chunks.append(cur)
            cur = lines[0] + " (cont.)"
        cur = f"{cur}\n{line}" if cur else line
    if cur:
        chunks.append(cur)
    return chunks


def _price(value) -> str:
    """Significant figures, not decimal places, so a 0.004 liquidation price doesn't become 0."""
    with localcontext() as ctx:
        ctx.prec = 6
        return num(+Decimal(value))


def _liquidation(price, mark: Decimal) -> str:
    """Liquidation price, how far the mark is from it, and a warning when that's close."""
    liq = Decimal(price)
    away = abs(liq - mark) / mark * 100
    if away > 100:  # with almost no risk, a figure like "1,383,647% away" is only noise
        return f"🟠 Liq <b>{_price(liq)}</b>"
    warning = " ⚠️" if away <= LIQ_WARNING_PERCENT else ""
    return f"🟠 Liq <b>{_price(liq)}</b> ({num(away, 1)}% away){warning}"


def format_position(position: dict, coin_name) -> str:
    size = Decimal(position["szi"])
    value = Decimal(position["positionValue"])
    mark = value / abs(size)
    leverage = position.get("leverage") or {}
    head = (
        f"{'📈' if size > 0 else '📉'} <b>{'Long' if size > 0 else 'Short'} {num(abs(size))} "
        f"{html.escape(coin_name(position['coin']))}</b> · <b>${num(value, 2)}</b>"
    )
    if leverage:
        head += f" · {leverage.get('value')}x {html.escape(str(leverage.get('type')))}"
    pnl = _pnl(Decimal(position["unrealizedPnl"]))
    if position.get("returnOnEquity") is not None:
        pnl += f" ({_signed(Decimal(position['returnOnEquity']) * 100, 1)}%)"
    lines = [
        head,
        f"Entry <b>{_price(position['entryPx'])}</b> · Mark <b>{_price(mark)}</b>",
        pnl,
    ]
    if position.get("liquidationPx") is not None:
        lines.append(_liquidation(position["liquidationPx"], mark))
    return "\n      ".join(lines)


def format_positions(label: str, address: str, states: list[dict], coin_name) -> list[str]:
    """The /positions list: largest first, with totals across every dex in states."""
    header = wallet_link(label, address)
    positions = [
        entry["position"] for state in states for entry in state.get("assetPositions", [])
        if Decimal(entry["position"]["szi"]) != 0
    ]
    account = sum(Decimal(state["marginSummary"]["accountValue"]) for state in states)
    if not positions:
        return [f"{header}\nNo open positions. Perp account <b>${num(account, 2)}</b>."]
    positions.sort(key=lambda p: Decimal(p["positionValue"]), reverse=True)
    total = sum(Decimal(p["positionValue"]) for p in positions)
    pnl = sum(Decimal(p["unrealizedPnl"]) for p in positions)
    lines = [
        header,
        f"<u>Positions ({len(positions)})</u>",
        f"Value <b>${num(total, 2)}</b> · {_pnl(pnl)} · Perp account <b>${num(account, 2)}</b>",
    ]
    for p in positions:
        lines.append("")
        lines.append(format_position(p, coin_name))
    return split_message(lines)


def format_open_orders(
    label: str, address: str, orders: list[dict], coin_name, expand: bool = False
) -> list[str]:
    """The /orders list. Scaled orders are summarised unless expand is set."""
    header = wallet_link(label, address)
    if not orders:
        return [header + "\nNo open orders."]
    lines = [header, f"<u>Open orders ({len(orders)})</u>"]
    ordered = sorted(orders, key=lambda o: (o["coin"], o.get("timestamp", 0)))
    events = [OrderEvent("new", "open", o, o) for o in ordered]
    for item in events if expand else group_orders(events):
        text = format_group(item, coin_name) if isinstance(item, OrderGroup) else format_order(item, coin_name)
        lines.append(text.replace("📝 New ", "• ", 1))
    return split_message(lines)
