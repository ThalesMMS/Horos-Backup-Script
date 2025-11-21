#!/usr/bin/env python3
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
    config = DEFAULT_CONFIG
    logger = setup_logging(config)
    try:
        run_once(config=config, logger=logger)
    except Exception as e:  # pragma: no cover - kept to mirror previous behavior
        logger.error("[FATAL] %s", e)
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
