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
        self._db.commit()

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
