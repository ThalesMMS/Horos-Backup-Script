#!/usr/bin/env python3
#
# horos_backup_export.py
# Horos Backup Script
#
# Entry-point wrapper that sets up config/logging and runs a single Horos backup cycle while preserving the original script interface.
#
# Thales Matheus Mendonça Santos - November 2025
#
"""
Thin wrapper that preserves the original entry point while delegating to the
modularised pipeline.
"""
from __future__ import annotations

import sys

from horos_backup.config import DEFAULT_CONFIG
from horos_backup.logging_setup import setup_logging
from horos_backup.runner import run_once


def main():
    # Use shared defaults to mirror the standalone script behavior.
    config = DEFAULT_CONFIG
    # Set up rotating file + stdout logging before doing any work.
    logger = setup_logging(config)
    try:
        # Execute a single export cycle (LaunchAgent will call repeatedly).
        run_once(config=config, logger=logger)
    except Exception as e:  # pragma: no cover - kept to mirror previous behavior
        logger.error("[FATAL] %s", e)
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
