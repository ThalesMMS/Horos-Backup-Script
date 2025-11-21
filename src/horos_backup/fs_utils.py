"""Filesystem helpers for the Horos backup pipeline."""
from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Optional

from .config import BackupConfig


def ensure_dirs(config: BackupConfig):
    paths = config.paths
    paths.backup_root.mkdir(parents=True, exist_ok=True)
    paths.tmp_root.mkdir(parents=True, exist_ok=True)
    paths.dbcopy_dir.mkdir(parents=True, exist_ok=True)


def ensure_volume_mounted(config: BackupConfig):
    paths = config.paths
    if not paths.pacs_root.exists() or not paths.sentinel.exists():
        raise RuntimeError(
            f"Volume externo não montado OU sentinela ausente: {paths.pacs_root}\n"
            f"Crie o arquivo sentinela: {paths.sentinel}"
        )


def count_files_early(root: Path, stop_after: int) -> int:
    """Iteratively count files, aborting as soon as ``stop_after`` is exceeded."""
    if not root.exists():
        return 0

    count = 0
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for entry in it:
                    try:
                        if entry.is_file(follow_symlinks=False):
                            count += 1
                            if count > stop_after:
                                return count
                        elif entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
                    except FileNotFoundError:
                        pass
        except FileNotFoundError:
            pass
    return count


def resolve_image_path(zpathstring, zpathnumber, zstored_in_dbfolder, config: BackupConfig) -> Path:
    """
    Resolve the physical path for a ZIMAGE entry.

    Behavior matches the original monolithic script and checks multiple
    candidates under ``DATABASE.noindex`` before falling back to absolute
    paths.
    """
    paths = config.paths
    try:
        in_db = int(zstored_in_dbfolder) == 1
    except Exception:
        in_db = False

    s_raw = zpathstring or ""
    s = str(s_raw).lstrip("/")
    sub = (str(zpathnumber).strip() if zpathnumber is not None else "")

    candidates = []
    if in_db:
        if sub:
            candidates.append(paths.database_dir / sub / s)
        candidates.append(paths.database_dir / s)
        p_abs = Path(s_raw)
        if p_abs.is_absolute():
            candidates.append(p_abs)
    else:
        p = Path(s_raw)
        if p.is_absolute():
            candidates.append(p)
        else:
            candidates.append(paths.horos_data_dir / s)

    for c in candidates:
        try:
            if c.is_file():
                return c
        except Exception:
            pass
    return candidates[0]


def dump_fs_layout(config: BackupConfig, logger: Optional[logging.Logger] = None):
    log = logger or logging.getLogger("horos_backup")
    try:
        log.debug("FS layout check:")
        log.debug("  HOROS_DATA_DIR exists=%s path=%s", config.paths.horos_data_dir.exists(), config.paths.horos_data_dir)
        log.debug("  DATABASE_DIR   exists=%s path=%s", config.paths.database_dir.exists(), config.paths.database_dir)
        if config.paths.database_dir.exists():
            subdirs = []
            with os.scandir(config.paths.database_dir) as it:
                for entry in it:
                    if entry.is_dir() and entry.name.isdigit():
                        subdirs.append(entry.name)
                        if len(subdirs) >= 10:
                            break
            log.debug(
                "  DATABASE_DIR sample subdirs (numeric): %s",
                ", ".join(subdirs) if subdirs else "(none)",
            )
    except Exception:
        log.exception("dump_fs_layout falhou")


def latest_incomplete_month_folder(config: BackupConfig) -> Optional[Path]:
    months = [p for p in config.paths.backup_root.glob("[0-9][0-9][0-9][0-9]_[0-1][0-9]") if p.is_dir()]
    if not months:
        return None
    latest = sorted(months)[-1]
    if not (latest / ".month_done").exists():
        return latest
    return None


def reset_incomplete_latest_month(config: BackupConfig, logger: Optional[logging.Logger] = None):
    log = logger or logging.getLogger("horos_backup")
    mf = latest_incomplete_month_folder(config)
    if mf and mf.exists():
        log.warning("Removendo mês incompleto: %s", mf)
        shutil.rmtree(mf, ignore_errors=True)


def mark_month_done(month_dir: Path, logger: Optional[logging.Logger] = None):
    log = logger or logging.getLogger("horos_backup")
    try:
        month_dir.mkdir(parents=True, exist_ok=True)
        (month_dir / ".month_done").touch()
    except Exception as e:
        log.warning("Falha ao marcar .month_done em %s: %s", month_dir, e)


__all__ = [
    "ensure_dirs",
    "ensure_volume_mounted",
    "count_files_early",
    "resolve_image_path",
    "dump_fs_layout",
    "latest_incomplete_month_folder",
    "reset_incomplete_latest_month",
    "mark_month_done",
]
