#
# state.py
# Horos Backup Script
#
# Manages the lightweight SQLite state database that tracks which studies were exported and when.
#
# Thales Matheus Mendonça Santos - November 2025
#
"""State database helpers."""
from __future__ import annotations

import sqlite3

from .config import BackupConfig


def state_connect(config: BackupConfig):
    # A tiny SQLite DB persisted alongside backups to track exported studies.
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
    # Idempotent insert so repeated runs simply overwrite with the latest path.
    cur = state_conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO Exported (studyInstanceUID, when_exported, zip_path) VALUES (?, datetime('now'), ?);",
        (study_uid, str(zip_path)),
    )
    state_conn.commit()


__all__ = ["state_connect", "mark_exported"]
