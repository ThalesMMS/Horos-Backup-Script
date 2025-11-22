#
# logging_setup.py
# Horos Backup Script
#
# Configures a rotating file logger and stdout handler shared by the backup pipeline.
#
# Thales Matheus Mendonça Santos - November 2025
#
"""Logging configuration helpers."""
import logging
import sys
from logging.handlers import RotatingFileHandler

from .config import BackupConfig


def setup_logging(config: BackupConfig, logger_name: str = "horos_backup") -> logging.Logger:
    """
    Configure a rotating file handler + stdout handler.
    Safe to call multiple times; existing handlers are reused.
    """
    logger = logging.getLogger(logger_name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    paths = config.paths
    # Make sure the backup folder has a place for log rotation.
    paths.logs_dir.mkdir(parents=True, exist_ok=True)

    fh = RotatingFileHandler(
        str(paths.log_file),
        maxBytes=config.settings.log_max_bytes,
        backupCount=config.settings.log_backup_count,
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    # Mirror everything to stdout for launchd/log tailers.
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


__all__ = ["setup_logging"]
