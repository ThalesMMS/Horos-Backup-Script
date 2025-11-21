"""Date-related helpers used across the backup pipeline."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

from .config import BackupConfig

APPLE_EPOCH = datetime(2001, 1, 1)


def parse_timestamp_to_parts(ts) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Convert different timestamp shapes to ``(YYYY, MM, DD)`` strings.

    Supported inputs:
    - CoreData timestamp (seconds since 2001-01-01).
    - ``YYYYMMDD``.
    - ``YYYY-MM-DD`` or variations with ``/``.
    - Best-effort lookups for ``YYYY-MM``.
    """
    if ts is None:
        return (None, None, None)

    s = str(ts).strip()
    if not s:
        return (None, None, None)

    try:
        secs = float(s)
        dt = APPLE_EPOCH + timedelta(seconds=secs)
        return (f"{dt.year:04d}", f"{dt.month:02d}", f"{dt.day:02d}")
    except ValueError:
        pass

    if len(s) == 8 and s.isdigit():
        return (s[0:4], s[4:6], s[6:8])

    m = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})", s)
    if m:
        return (m.group(1), m.group(2), m.group(3))

    m2 = re.search(r"(\d{4})[-/](\d{2})", s)
    if m2:
        return (m2.group(1), m2.group(2), "01")

    return (None, None, None)


def fmt_date_for_name(ts, fallback: str = "UNKNOWN") -> str:
    y, mo, d = parse_timestamp_to_parts(ts)
    if y and mo and d:
        return f"{y}-{mo}-{d}"
    if y and mo:
        return f"{y}-{mo}-01"
    return fallback


def month_dir_for(ts, config: BackupConfig) -> Path:
    y, mo, _ = parse_timestamp_to_parts(ts)
    if y and mo:
        return config.paths.backup_root / f"{y}_{mo}"
    return config.paths.backup_root / "UNKNOWN_DATE"


def debug_dump_date(label: str, raw, logger: Optional[logging.Logger] = None):
    log = logger or logging.getLogger("horos_backup")
    y, mo, d = parse_timestamp_to_parts(raw)
    log.debug("%s: raw=%r -> parsed=%s-%s-%s", label, raw, y, mo, d)
    return y, mo, d


__all__ = [
    "APPLE_EPOCH",
    "parse_timestamp_to_parts",
    "fmt_date_for_name",
    "month_dir_for",
    "debug_dump_date",
]
