#
# naming.py
# Horos Backup Script
#
# Sanitizes patient and study identifiers and builds deterministic ZIP filenames within length limits.
#
# Thales Matheus Mendonça Santos - November 2025
#
"""Filename sanitization and ZIP naming helpers."""
from __future__ import annotations

import re
from pathlib import Path

from .config import BackupConfig
from .dates import fmt_date_for_name

SANITIZE_RE = re.compile(r"[^0-9A-Za-z._-]+", re.UNICODE)


def sanitize_name(s: str) -> str:
    # Replace spaces and unwanted chars with underscores, then clamp length.
    # DICOM strings can contain accents or symbols, so we keep it ASCII-safe.
    sanitized = (s or "").strip().replace(" ", "_")
    sanitized = SANITIZE_RE.sub("_", sanitized)
    return sanitized[:128] or "UNKNOWN"


def build_zip_path(
    month_dir: Path,
    patient_name: str,
    dob_ts,
    study_ts,
    study_uid: str,
    config: BackupConfig,
) -> Path:
    """
    Generate a unique ZIP path keeping the full Study UID intact.
    The prefix may be truncated to respect ``max_name_noext``.
    """
    max_noext = config.settings.max_name_noext

    patient = sanitize_name(patient_name)
    dob = fmt_date_for_name(dob_ts, fallback="UNKNOWN")
    sdate = fmt_date_for_name(study_ts, fallback="UNKNOWN")
    uid = sanitize_name(study_uid)

    prefix = f"{patient}_{dob}_{sdate}"
    base_noext = f"{prefix}_{uid}"

    if len(base_noext) > max_noext:
        # Keep full UID and shrink the prefix to fit under the configured cap.
        allow_prefix = max(1, max_noext - (len(uid) + 1))
        prefix = prefix[:allow_prefix].rstrip("_")
        base_noext = f"{prefix}_{uid}"

    candidate = month_dir / f"{base_noext}.zip"
    if not candidate.exists():
        return candidate

    # Avoid collisions if a similarly named study already exists.
    n = 2
    while True:
        cand = month_dir / f"{base_noext}_{n}.zip"
        if not cand.exists():
            return cand
        n += 1


__all__ = ["sanitize_name", "build_zip_path", "SANITIZE_RE"]
