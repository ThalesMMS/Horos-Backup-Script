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

    if paths.dbcopy_path.exists():
        try:
            paths.dbcopy_path.unlink()
        except Exception:
            pass

    src = sqlite3.connect(f"file:{paths.horos_db_orig}?mode=ro", uri=True)
    try:
        src.execute("PRAGMA query_only=ON;")
    except Exception:
        pass

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
        dbp = copy_horos_db_consistent(config)
        try:
            st = os.stat(dbp)
            log.info("Snapshot criado: %s (size=%d mtime=%d)", dbp, st.st_size, int(st.st_mtime))
        except Exception:
            log.info("Snapshot criado: %s", dbp)
        return dbp

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
