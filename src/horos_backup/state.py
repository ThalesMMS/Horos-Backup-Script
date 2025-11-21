"""State database helpers."""
from __future__ import annotations

import sqlite3

from .config import BackupConfig


def state_connect(config: BackupConfig):
    conn = sqlite3.connect(str(config.paths.state_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Exported (
            studyInstanceUID TEXT PRIMARY KEY,
            when_exported     TEXT NOT NULL,
            zip_path          TEXT NOT NULL
        );
        """
    )
    conn.commit()
    return conn


def mark_exported(state_conn, study_uid: str, zip_path):
    cur = state_conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO Exported (studyInstanceUID, when_exported, zip_path) VALUES (?, datetime('now'), ?);",
        (study_uid, str(zip_path)),
    )
    state_conn.commit()


__all__ = ["state_connect", "mark_exported"]
