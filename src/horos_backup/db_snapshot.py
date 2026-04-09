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
    """
    Create a read-only SQLite snapshot of the Horos database and return the snapshot path.
    
    Removes any existing stale snapshot at the configured destination, opens the original Horos
    database in read-only/query-only mode to avoid locking the live DB, copies its contents into
    the destination within the backup tree, and returns the destination path.
    
    Parameters:
        config (BackupConfig): Backup configuration whose `paths` attribute provides
            `horos_db_orig` (source DB path) and `dbcopy_path` (destination snapshot path).
    
    Returns:
        str: Filesystem path to the created snapshot.
    
    Raises:
        FileNotFoundError: If the original Horos database at `paths.horos_db_orig` does not exist.
    """
    paths = config.paths
    if not paths.horos_db_orig.exists():
        raise FileNotFoundError(f"Horos DB not found: {paths.horos_db_orig}")

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
    """
    Selects the SQLite database path to use for queries, creating or reusing a read-only snapshot according to configuration.
    
    If `config.settings.use_db_copy` is true, a fresh read-only snapshot is created and its path is returned. Otherwise, an existing snapshot at `config.paths.dbcopy_path` is reused when present; if it is missing, a one-shot snapshot is created and returned.
    
    Parameters:
        config (BackupConfig): Backup configuration containing `settings.use_db_copy` and `paths.dbcopy_path`.
    
    Returns:
        str: Filesystem path to the read-only SQLite database snapshot to use.
    
    Raises:
        FileNotFoundError: If the original Horos database is missing when attempting to create a snapshot.
    """
    log = logger or logging.getLogger("horos_backup")
    use_copy = config.settings.use_db_copy

    if use_copy:
        # Always create a fresh snapshot when configured to do so.
        dbp = copy_horos_db_consistent(config)
        try:
            st = os.stat(dbp)
            log.info("Snapshot created: %s (size=%d mtime=%d)", dbp, st.st_size, int(st.st_mtime))
        except Exception:
            log.info("Snapshot created: %s", dbp)
        return dbp

    # Reuse previous snapshot to save time when allowed.
    if config.paths.dbcopy_path.exists():
        try:
            st = os.stat(config.paths.dbcopy_path)
            log.info(
                "Reusing existing snapshot: %s (size=%d mtime=%d)", config.paths.dbcopy_path, st.st_size, int(st.st_mtime)
            )
        except Exception:
            log.info("Reusing existing snapshot: %s", config.paths.dbcopy_path)
        return str(config.paths.dbcopy_path)

    log.warning("Snapshot missing at %s; creating a copy now (one-shot).", config.paths.dbcopy_path)
    dbp = copy_horos_db_consistent(config)
    try:
        st = os.stat(dbp)
        log.info("Snapshot created: %s (size=%d mtime=%d)", dbp, st.st_size, int(st.st_mtime))
    except Exception:
        log.info("Snapshot created: %s", dbp)
    return dbp


__all__ = ["copy_horos_db_consistent", "choose_db_path"]
