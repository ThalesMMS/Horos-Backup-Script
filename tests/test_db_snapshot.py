#
# test_db_snapshot.py
# Horos Backup Script
#
# Covers SQLite snapshot creation and DB path selection logic added in the
# chore/translate-pt-docs-to-english PR.
#
# Thales Matheus Mendonça Santos - November 2025
#
import logging
import sqlite3

import pytest

from horos_backup.config import BackupConfig, Paths, Settings
from horos_backup.db_snapshot import choose_db_path, copy_horos_db_consistent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(path):
    """Create a minimal SQLite database at *path* so the code under test can open it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE IF NOT EXISTS dummy (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# copy_horos_db_consistent
# ---------------------------------------------------------------------------

class TestCopyHorosDbConsistent:
    def test_raises_when_source_db_missing(self, temp_config):
        # The error message must be in English and include the missing path.
        with pytest.raises(FileNotFoundError) as exc_info:
            copy_horos_db_consistent(temp_config)
        assert "Horos DB not found" in str(exc_info.value)
        assert str(temp_config.paths.horos_db_orig) in str(exc_info.value)

    def test_creates_snapshot_when_source_exists(self, temp_config):
        _make_db(temp_config.paths.horos_db_orig)

        result = copy_horos_db_consistent(temp_config)

        assert result == str(temp_config.paths.dbcopy_path)
        assert temp_config.paths.dbcopy_path.exists()
        # The copy must be a readable SQLite file.
        conn = sqlite3.connect(result)
        conn.execute("SELECT name FROM sqlite_master")
        conn.close()

    def test_removes_stale_snapshot_before_copy(self, temp_config):
        _make_db(temp_config.paths.horos_db_orig)
        # Pre-plant a stale snapshot with known content.
        temp_config.paths.dbcopy_path.parent.mkdir(parents=True, exist_ok=True)
        temp_config.paths.dbcopy_path.write_text("stale content")

        copy_horos_db_consistent(temp_config)

        # The stale text file must have been replaced with a real SQLite database.
        conn = sqlite3.connect(str(temp_config.paths.dbcopy_path))
        conn.execute("SELECT name FROM sqlite_master")
        conn.close()

    def test_returns_string_path(self, temp_config):
        _make_db(temp_config.paths.horos_db_orig)
        result = copy_horos_db_consistent(temp_config)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# choose_db_path
# ---------------------------------------------------------------------------

class TestChooseDbPath:
    def test_use_db_copy_true_creates_fresh_snapshot(self, temp_config):
        _make_db(temp_config.paths.horos_db_orig)
        # Default settings have use_db_copy=True.
        assert temp_config.settings.use_db_copy is True

        result = choose_db_path(temp_config)

        assert result == str(temp_config.paths.dbcopy_path)
        assert temp_config.paths.dbcopy_path.exists()

    def test_use_db_copy_false_reuses_existing_snapshot(self, tmp_path):
        paths = Paths(pacs_root=tmp_path / "PACS")
        settings = Settings(use_db_copy=False)
        config = BackupConfig(paths=paths, settings=settings)
        # Build the minimal directory tree the fixture normally sets up.
        paths.pacs_root.mkdir(parents=True, exist_ok=True)
        paths.dbcopy_dir.mkdir(parents=True, exist_ok=True)
        paths.sentinel.touch()
        _make_db(paths.horos_db_orig)

        # Pre-populate a snapshot so the function should reuse it.
        _make_db(paths.dbcopy_path)
        original_mtime = paths.dbcopy_path.stat().st_mtime

        result = choose_db_path(config)

        assert result == str(paths.dbcopy_path)
        # The snapshot must NOT have been recreated (mtime unchanged).
        assert paths.dbcopy_path.stat().st_mtime == original_mtime

    def test_use_db_copy_false_creates_one_shot_when_snapshot_missing(self, tmp_path):
        paths = Paths(pacs_root=tmp_path / "PACS")
        settings = Settings(use_db_copy=False)
        config = BackupConfig(paths=paths, settings=settings)
        paths.pacs_root.mkdir(parents=True, exist_ok=True)
        paths.dbcopy_dir.mkdir(parents=True, exist_ok=True)
        paths.sentinel.touch()
        _make_db(paths.horos_db_orig)

        # No pre-existing snapshot — the function should create one on the fly.
        assert not paths.dbcopy_path.exists()

        result = choose_db_path(config)

        assert result == str(paths.dbcopy_path)
        assert paths.dbcopy_path.exists()

    def test_choose_db_path_logs_snapshot_created(self, temp_config, caplog):
        _make_db(temp_config.paths.horos_db_orig)

        with caplog.at_level(logging.INFO, logger="horos_backup"):
            choose_db_path(temp_config)

        assert "Snapshot created" in caplog.text

    def test_choose_db_path_logs_reusing_existing(self, tmp_path, caplog):
        paths = Paths(pacs_root=tmp_path / "PACS")
        settings = Settings(use_db_copy=False)
        config = BackupConfig(paths=paths, settings=settings)
        paths.pacs_root.mkdir(parents=True, exist_ok=True)
        paths.dbcopy_dir.mkdir(parents=True, exist_ok=True)
        paths.sentinel.touch()
        _make_db(paths.horos_db_orig)
        _make_db(paths.dbcopy_path)

        with caplog.at_level(logging.INFO, logger="horos_backup"):
            choose_db_path(config)

        assert "Reusing existing snapshot" in caplog.text

    def test_choose_db_path_logs_one_shot_warning(self, tmp_path, caplog):
        paths = Paths(pacs_root=tmp_path / "PACS")
        settings = Settings(use_db_copy=False)
        config = BackupConfig(paths=paths, settings=settings)
        paths.pacs_root.mkdir(parents=True, exist_ok=True)
        paths.dbcopy_dir.mkdir(parents=True, exist_ok=True)
        paths.sentinel.touch()
        _make_db(paths.horos_db_orig)

        with caplog.at_level(logging.WARNING, logger="horos_backup"):
            choose_db_path(config)

        assert "creating a copy now (one-shot)" in caplog.text

    def test_use_db_copy_true_raises_when_source_missing(self, temp_config):
        # Source DB absent — must propagate FileNotFoundError even in use_db_copy mode.
        assert temp_config.settings.use_db_copy is True
        with pytest.raises(FileNotFoundError):
            choose_db_path(temp_config)