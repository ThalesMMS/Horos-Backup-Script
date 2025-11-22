#
# config.py
# Horos Backup Script
#
# Defines dataclasses for filesystem paths and runtime settings so backups can be configured and reused consistently across runs and tests.
#
# Thales Matheus Mendonça Santos - November 2025
#
"""Configuration objects for the Horos backup pipeline.

The defaults mimic the previous monolithic script. A BackupConfig bundles
filesystem paths and runtime settings, making it easy to reuse with
alternative roots during testing.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple


@dataclass
class Paths:
    pacs_root: Path = Path("/Volumes/PACS")
    backup_root: Optional[Path] = None

    def __post_init__(self):
        # Normalize inputs to Path objects even when callers pass strings.
        self.pacs_root = Path(self.pacs_root)
        if self.backup_root is not None:
            self.backup_root = Path(self.backup_root)
        # Default backup_root stays inside the PACS volume to avoid local SSD.
        self.backup_root = self.backup_root or (self.pacs_root / "Backup")
        # Sentinel prevents running on the wrong drive.
        self.sentinel = self.pacs_root / ".pacs_sentinel"
        self.horos_data_dir = self.pacs_root / "Database" / "Horos Data"
        self.horos_db_orig = self.horos_data_dir / "Database.sql"
        self.incoming_dir = self.horos_data_dir / "INCOMING.noindex"
        self.database_dir = self.horos_data_dir / "DATABASE.noindex"

        # Temporary workspace and state artifacts.
        self.tmp_root = self.backup_root / ".tmp"
        self.dbcopy_dir = self.tmp_root / "dbcopy"
        self.dbcopy_path = self.dbcopy_dir / "Database_copy.sql"
        self.state_db = self.backup_root / "export_state.sqlite"
        self.lockfile_path = self.tmp_root / ".run.lock"

        # Logging and issues tracking.
        self.logs_dir = self.backup_root / "logs"
        self.log_file = self.logs_dir / "horos_backup.log"
        self.issues_csv = self.backup_root / "issues.csv"


@dataclass
class Settings:
    incoming_max_files: int = 25_000
    order_by: str = "study_date"  # accepted values: "study_date" | "date_added"
    batch_size: int = 15
    sleep_between_studies: int = 1  # seconds
    mods: Tuple[str, ...] = ("CT", "MR")
    max_name_noext: int = 128
    use_db_copy: bool = True
    log_max_bytes: int = 100 * 1024 * 1024  # 100 MB
    log_backup_count: int = 10


@dataclass
class BackupConfig:
    paths: Paths = field(default_factory=Paths)
    settings: Settings = field(default_factory=Settings)


DEFAULT_CONFIG = BackupConfig()
