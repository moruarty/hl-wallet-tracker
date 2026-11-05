import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    bot_token: str
    allowed_chat_ids: frozenset[int]
    batch_interval: float
    db_path: str
    hl_api_url: str
    hl_ws_url: str
    log_level: str


def load_config() -> Config:
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token or token.endswith("replace-me"):
        raise SystemExit("TELEGRAM_BOT_TOKEN is not set. Copy .env.example to .env and fill it in.")

    raw_ids = os.getenv("ALLOWED_CHAT_IDS", "")
    try:
        chat_ids = frozenset(int(x) for x in raw_ids.replace(" ", "").split(",") if x)
    except ValueError:
        raise SystemExit(f"ALLOWED_CHAT_IDS must be comma-separated integers, got: {raw_ids!r}")
    if not chat_ids:
        print("WARNING: ALLOWED_CHAT_IDS is empty, so no chat can use the bot. Use /chatid to find IDs.")

    return Config(
        bot_token=token,
        allowed_chat_ids=chat_ids,
        batch_interval=float(os.getenv("BATCH_INTERVAL_SECONDS", "5")),
        db_path=os.getenv("DB_PATH", "data/tracker.db"),
        hl_api_url=os.getenv("HL_API_URL", "https://api.hyperliquid.xyz").rstrip("/"),
        hl_ws_url=os.getenv("HL_WS_URL", "wss://api.hyperliquid.xyz/ws"),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )
