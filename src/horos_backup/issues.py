"""Issues CSV logging."""
from __future__ import annotations

import csv
from datetime import datetime
from typing import Dict, Optional

from .config import BackupConfig


def issues_log(
    config: BackupConfig, kind: str, study_uid: str, detail: str = "", extra: Optional[Dict] = None
):
    """
    Append an issue row to ``issues.csv``.
    """
    issues_csv = config.paths.issues_csv
    issues_csv.parent.mkdir(parents=True, exist_ok=True)
    new_file = not issues_csv.exists()
    with open(issues_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["timestamp", "kind", "study_uid", "detail", "extra"])
        w.writerow(
            [
                datetime.now().isoformat(timespec="seconds"),
                kind,
                study_uid,
                detail,
                (extra and str(extra)) or "",
            ]
        )


__all__ = ["issues_log"]
