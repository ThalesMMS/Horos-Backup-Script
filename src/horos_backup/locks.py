#
# locks.py
# Horos Backup Script
#
# Implements a simple file-based lock to prevent overlapping backup processes.
#
# Thales Matheus Mendonça Santos - November 2025
#
"""File lock utilities to avoid overlapping runs."""
from __future__ import annotations

import fcntl
import os
from pathlib import Path


def acquire_lock(lock_path: Path):
    # Create parent directories to avoid race on first run.
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    f = open(lock_path, "w")
    try:
        # Non-blocking exclusive lock to skip runs when another is active.
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        f.truncate(0)
        f.write(str(os.getpid()))
        f.flush()
        return f
    except BlockingIOError:
        f.close()
        return None


def release_lock(fh):
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        fh.close()
    except Exception:
        pass


__all__ = ["acquire_lock", "release_lock"]
