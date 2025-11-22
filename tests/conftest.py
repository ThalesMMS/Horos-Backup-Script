#
# conftest.py
# Horos Backup Script
#
# Creates reusable pytest fixtures that set up a temporary backup configuration and filesystem layout for tests.
#
# Thales Matheus Mendonça Santos - November 2025
#
import pytest

from horos_backup.config import BackupConfig, Paths, Settings


@pytest.fixture
def temp_config(tmp_path):
    pacs_root = tmp_path / "PACS"
    paths = Paths(pacs_root=pacs_root)
    settings = Settings()
    config = BackupConfig(paths=paths, settings=settings)

    # Create the minimal folder structure expected by the code under test.
    paths.pacs_root.mkdir(parents=True, exist_ok=True)
    paths.backup_root.mkdir(parents=True, exist_ok=True)
    paths.database_dir.mkdir(parents=True, exist_ok=True)
    paths.horos_data_dir.mkdir(parents=True, exist_ok=True)
    paths.tmp_root.mkdir(parents=True, exist_ok=True)
    paths.dbcopy_dir.mkdir(parents=True, exist_ok=True)
    paths.logs_dir.mkdir(parents=True, exist_ok=True)
    paths.sentinel.touch()

    return config
