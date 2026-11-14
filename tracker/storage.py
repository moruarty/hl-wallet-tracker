"""SQLite storage for which chat tracks which wallet."""
import os
import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class TrackedWallet:
    chat_id: int
    address: str
    label: str


class Storage:
    def __init__(self, path: str):
        if os.path.dirname(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
        self._db = sqlite3.connect(path)
        self._db.execute(
            """CREATE TABLE IF NOT EXISTS tracked (
                chat_id INTEGER NOT NULL,
                address TEXT NOT NULL,
                label TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, address)
            )"""
        )
        self._db.execute(
            """CREATE TABLE IF NOT EXISTS binance_tracked (
                chat_id INTEGER NOT NULL,
                portfolio_id TEXT NOT NULL,
                label TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, portfolio_id)
            )"""
        )
        # The newest trade time seen per trader, so trades made while the bot was down are still reported.
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS binance_cursor (portfolio_id TEXT PRIMARY KEY, last_ms INTEGER NOT NULL)"
        )
        self._db.commit()

    # ---- Binance lead traders ------------------------------------------------
    def track_binance(self, chat_id: int, portfolio_id: str, label: str) -> bool:
        """Insert or relabel. Returns True if the trader was newly added to this chat."""
        exists = self._db.execute(
            "SELECT 1 FROM binance_tracked WHERE chat_id=? AND portfolio_id=?", (chat_id, portfolio_id)
        ).fetchone()
        self._db.execute(
            "INSERT INTO binance_tracked(chat_id, portfolio_id, label) VALUES(?,?,?) "
            "ON CONFLICT(chat_id, portfolio_id) DO UPDATE SET label=excluded.label",
            (chat_id, portfolio_id, label),
        )
        self._db.commit()
        return exists is None

    def untrack_binance(self, chat_id: int, id_or_label: str) -> list[TrackedWallet]:
        key = id_or_label.strip()
        rows = self._db.execute(
            "SELECT chat_id, portfolio_id, label FROM binance_tracked "
            "WHERE chat_id=? AND (portfolio_id=? OR lower(label)=lower(?))",
            (chat_id, key, key),
        ).fetchall()
        for row in rows:
            self._db.execute("DELETE FROM binance_tracked WHERE chat_id=? AND portfolio_id=?", (chat_id, row[1]))
        self._db.commit()
        return [TrackedWallet(*r) for r in rows]

    def binance_for_chat(self, chat_id: int) -> list[TrackedWallet]:
        rows = self._db.execute(
            "SELECT chat_id, portfolio_id, label FROM binance_tracked WHERE chat_id=? ORDER BY created_at",
            (chat_id,),
        ).fetchall()
        return [TrackedWallet(*r) for r in rows]

    def all_binance(self) -> list[TrackedWallet]:
        """Every tracked trader; `address` holds the portfolio ID."""
        rows = self._db.execute("SELECT chat_id, portfolio_id, label FROM binance_tracked").fetchall()
        return [TrackedWallet(*r) for r in rows]

    def binance_label_taken(self, chat_id: int, label: str, except_id: str) -> bool:
        return self._db.execute(
            "SELECT 1 FROM binance_tracked WHERE chat_id=? AND lower(label)=lower(?) AND portfolio_id<>?",
            (chat_id, label, except_id),
        ).fetchone() is not None

    def binance_cursor(self, portfolio_id: str) -> int | None:
        row = self._db.execute(
            "SELECT last_ms FROM binance_cursor WHERE portfolio_id=?", (portfolio_id,)
        ).fetchone()
        return row[0] if row else None

    def set_binance_cursor(self, portfolio_id: str, last_ms: int) -> None:
        self._db.execute(
            "INSERT INTO binance_cursor(portfolio_id, last_ms) VALUES(?,?) "
            "ON CONFLICT(portfolio_id) DO UPDATE SET last_ms=max(last_ms, excluded.last_ms)",
            (portfolio_id, last_ms),
        )
        self._db.commit()

    def clear_binance_cursor(self, portfolio_id: str) -> None:
        """Once nobody tracks a trader, so tracking them again later doesn't replay old trades."""
        self._db.execute("DELETE FROM binance_cursor WHERE portfolio_id=?", (portfolio_id,))
        self._db.commit()

    # ---- Hyperliquid wallets ---------------------------------------------------
    def track(self, chat_id: int, address: str, label: str) -> bool:
        """Insert or relabel. Returns True if the wallet was newly added to this chat."""
        exists = self._db.execute(
            "SELECT 1 FROM tracked WHERE chat_id=? AND address=?", (chat_id, address)
        ).fetchone()
        self._db.execute(
            "INSERT INTO tracked(chat_id, address, label) VALUES(?,?,?) "
            "ON CONFLICT(chat_id, address) DO UPDATE SET label=excluded.label",
            (chat_id, address, label),
        )
        self._db.commit()
        return exists is None

    def untrack(self, chat_id: int, wallet_or_label: str) -> list[TrackedWallet]:
        key = wallet_or_label.strip()
        rows = self._db.execute(
            "SELECT chat_id, address, label FROM tracked "
            "WHERE chat_id=? AND (address=? OR lower(label)=lower(?))",
            (chat_id, key.lower(), key),
        ).fetchall()
        removed = [TrackedWallet(*r) for r in rows]
        for w in removed:
            self._db.execute("DELETE FROM tracked WHERE chat_id=? AND address=?", (chat_id, w.address))
        self._db.commit()
        return removed

    def list_for_chat(self, chat_id: int) -> list[TrackedWallet]:
        rows = self._db.execute(
            "SELECT chat_id, address, label FROM tracked WHERE chat_id=? ORDER BY created_at", (chat_id,)
        ).fetchall()
        return [TrackedWallet(*r) for r in rows]

    def all(self) -> list[TrackedWallet]:
        rows = self._db.execute("SELECT chat_id, address, label FROM tracked").fetchall()
        return [TrackedWallet(*r) for r in rows]

    def label_taken(self, chat_id: int, label: str, except_address: str) -> bool:
        return self._db.execute(
            "SELECT 1 FROM tracked WHERE chat_id=? AND lower(label)=lower(?) AND address<>?",
            (chat_id, label, except_address),
        ).fetchone() is not None
