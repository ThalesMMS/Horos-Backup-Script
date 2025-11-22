#
# __init__.py
# Horos Backup Script
#
# Package initializer exporting the core config dataclass and the single-run entry point for the Horos backup pipeline.
#
# Thales Matheus Mendonça Santos - November 2025
#
"""Horos backup automation package."""
# Re-export the main configuration and runner to match the legacy script API.
from .config import BackupConfig, DEFAULT_CONFIG
from .runner import run_once

__all__ = ["BackupConfig", "DEFAULT_CONFIG", "run_once"]
