#
# db_snapshot.py
# Horos Backup Script
#
# Creates a read-only SQLite snapshot of the Horos database and chooses the correct path for queries during an export cycle.
#
# Thales Matheus Mendonça Santos - November 2025
#
"""SQLite snapshot helpers for the Horos database."""
from __future__ import annotations

import logging
import os
import sqlite3
from typing import Optional

from .config import BackupConfig


def copy_horos_db_consistent(config: BackupConfig) -> str:
    paths = config.paths
    if not paths.horos_db_orig.exists():
        raise FileNotFoundError(f"DB do Horos não encontrado: {paths.horos_db_orig}")

    # Remove stale copy to avoid reading outdated data.
    if paths.dbcopy_path.exists():
        try:
            paths.dbcopy_path.unlink()
        except Exception:
            pass

    # Open source DB in query-only mode to avoid locking Horos.
    src = sqlite3.connect(f"file:{paths.horos_db_orig}?mode=ro", uri=True)
    try:
        src.execute("PRAGMA query_only=ON;")
    except Exception:
        pass

    # Copy to a temp-friendly location inside the backup tree.
    dst = sqlite3.connect(str(paths.dbcopy_path))
    try:
        src.backup(dst)
        dst.commit()
    finally:
        dst.close()
        src.close()

    return str(paths.dbcopy_path)


def choose_db_path(config: BackupConfig, logger: Optional[logging.Logger] = None) -> str:
    log = logger or logging.getLogger("horos_backup")
    use_copy = config.settings.use_db_copy

    if use_copy:
        # Always create a fresh snapshot when configured to do so.
        dbp = copy_horos_db_consistent(config)
        try:
            st = os.stat(dbp)
            log.info("Snapshot criado: %s (size=%d mtime=%d)", dbp, st.st_size, int(st.st_mtime))
        except Exception:
            log.info("Snapshot criado: %s", dbp)
        return dbp

    # Reuse previous snapshot to save time when allowed.
    if config.paths.dbcopy_path.exists():
        try:
            st = os.stat(config.paths.dbcopy_path)
            log.info(
                "Reutilizando snapshot existente: %s (size=%d mtime=%d)", config.paths.dbcopy_path, st.st_size, int(st.st_mtime)
            )
        except Exception:
            log.info("Reutilizando snapshot existente: %s", config.paths.dbcopy_path)
        return str(config.paths.dbcopy_path)

    log.warning("Snapshot ausente em %s; criando uma cópia agora (one-shot).", config.paths.dbcopy_path)
    dbp = copy_horos_db_consistent(config)
    try:
        st = os.stat(dbp)
        log.info("Snapshot criado: %s (size=%d mtime=%d)", dbp, st.st_size, int(st.st_mtime))
    except Exception:
        log.info("Snapshot criado: %s", dbp)
    return dbp


__all__ = ["copy_horos_db_consistent", "choose_db_path"]
