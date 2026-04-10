#
# test_fs_utils.py
# Horos Backup Script
#
# Covers filesystem utilities like mount checks, early file counting, and resolving image paths in different scenarios.
#
# Thales Matheus Mendonça Santos - November 2025
#
import logging
from pathlib import Path

import pytest

from horos_backup.config import BackupConfig, Paths, Settings
from horos_backup.fs_utils import (
    count_files_early,
    dump_fs_layout,
    ensure_volume_mounted,
    latest_incomplete_month_folder,
    mark_month_done,
    reset_incomplete_latest_month,
    resolve_image_path,
)


def test_count_files_early(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    for i in range(5):
        (root / f"f{i}").write_text("x")
    # Should stop counting as soon as the threshold is passed.
    assert count_files_early(root, stop_after=3) == 4


def test_ensure_volume_mounted(temp_config):
    # Should not raise when sentinel exists
    ensure_volume_mounted(temp_config)


def test_resolve_image_path_in_db(temp_config):
    db_sub = temp_config.paths.database_dir / "123"
    db_sub.mkdir(parents=True, exist_ok=True)
    target = db_sub / "file.dcm"
    target.write_text("x")

    resolved = resolve_image_path("file.dcm", 123, 1, temp_config)
    # In-database files should resolve into DATABASE.noindex subfolders.
    assert resolved == target


def test_resolve_image_path_relative_outside_db(temp_config):
    target = temp_config.paths.horos_data_dir / "relative" / "image.dcm"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("x")

    resolved = resolve_image_path("relative/image.dcm", None, 0, temp_config)
    # Relative paths outside DB should resolve under Horos Data.
    assert resolved == target


# ---------------------------------------------------------------------------
# ensure_volume_mounted – error messages (English after PR translation)
# ---------------------------------------------------------------------------

class TestEnsureVolumeMountedErrors:
    def test_raises_when_pacs_root_missing(self, tmp_path):
        paths = Paths(pacs_root=tmp_path / "MISSING_PACS")
        config = BackupConfig(paths=paths)
        with pytest.raises(RuntimeError) as exc_info:
            ensure_volume_mounted(config)
        msg = str(exc_info.value)
        assert "External volume not mounted OR sentinel missing" in msg

    def test_raises_when_sentinel_missing(self, tmp_path):
        paths = Paths(pacs_root=tmp_path / "PACS")
        paths.pacs_root.mkdir(parents=True, exist_ok=True)
        # Deliberately omit creating the sentinel.
        config = BackupConfig(paths=paths)
        with pytest.raises(RuntimeError) as exc_info:
            ensure_volume_mounted(config)
        msg = str(exc_info.value)
        assert "Create the sentinel file" in msg
        assert str(paths.sentinel) in msg

    def test_error_mentions_pacs_root(self, tmp_path):
        paths = Paths(pacs_root=tmp_path / "ABSENT")
        config = BackupConfig(paths=paths)
        with pytest.raises(RuntimeError) as exc_info:
            ensure_volume_mounted(config)
        assert str(paths.pacs_root) in str(exc_info.value)


# ---------------------------------------------------------------------------
# latest_incomplete_month_folder
# ---------------------------------------------------------------------------

class TestLatestIncompleteMonthFolder:
    def test_returns_none_when_no_month_dirs(self, temp_config):
        result = latest_incomplete_month_folder(temp_config)
        assert result is None

    def test_returns_none_when_latest_is_complete(self, temp_config):
        month_dir = temp_config.paths.backup_root / "2024_01"
        month_dir.mkdir()
        (month_dir / ".month_done").touch()

        result = latest_incomplete_month_folder(temp_config)
        assert result is None

    def test_returns_latest_incomplete_folder(self, temp_config):
        month_dir = temp_config.paths.backup_root / "2024_03"
        month_dir.mkdir()
        # No .month_done placed — folder is incomplete.

        result = latest_incomplete_month_folder(temp_config)
        assert result == month_dir

    def test_picks_newest_when_multiple_folders(self, temp_config):
        older = temp_config.paths.backup_root / "2023_11"
        newer = temp_config.paths.backup_root / "2024_06"
        older.mkdir()
        newer.mkdir()
        # Neither folder has .month_done.

        result = latest_incomplete_month_folder(temp_config)
        assert result == newer

    def test_ignores_already_complete_latest_when_older_is_incomplete(self, temp_config):
        older = temp_config.paths.backup_root / "2023_12"
        newer = temp_config.paths.backup_root / "2024_01"
        older.mkdir()
        newer.mkdir()
        (newer / ".month_done").touch()  # Latest is complete.

        # Function returns None because the latest is already done.
        result = latest_incomplete_month_folder(temp_config)
        assert result is None


# ---------------------------------------------------------------------------
# reset_incomplete_latest_month
# ---------------------------------------------------------------------------

class TestResetIncompleteLatestMonth:
    def test_removes_incomplete_month_folder(self, temp_config):
        month_dir = temp_config.paths.backup_root / "2024_05"
        month_dir.mkdir()
        (month_dir / "study.zip").write_text("data")

        reset_incomplete_latest_month(temp_config)

        assert not month_dir.exists()

    def test_does_nothing_when_latest_is_complete(self, temp_config):
        month_dir = temp_config.paths.backup_root / "2024_05"
        month_dir.mkdir()
        (month_dir / ".month_done").touch()

        reset_incomplete_latest_month(temp_config)

        assert month_dir.exists()

    def test_does_nothing_when_no_month_folders(self, temp_config):
        # Should not raise when backup_root is empty.
        reset_incomplete_latest_month(temp_config)

    def test_logs_english_warning_on_removal(self, temp_config, caplog):
        month_dir = temp_config.paths.backup_root / "2024_07"
        month_dir.mkdir()

        with caplog.at_level(logging.WARNING, logger="horos_backup"):
            reset_incomplete_latest_month(temp_config)

        assert "Removing incomplete month folder" in caplog.text


# ---------------------------------------------------------------------------
# mark_month_done
# ---------------------------------------------------------------------------

class TestMarkMonthDone:
    def test_creates_month_done_marker(self, tmp_path):
        month_dir = tmp_path / "2024_08"
        month_dir.mkdir()

        mark_month_done(month_dir)

        assert (month_dir / ".month_done").exists()

    def test_creates_directory_when_missing(self, tmp_path):
        month_dir = tmp_path / "2024_09"
        # Do NOT create the directory — mark_month_done should create it.
        assert not month_dir.exists()

        mark_month_done(month_dir)

        assert month_dir.exists()
        assert (month_dir / ".month_done").exists()

    def test_logs_english_warning_on_failure(self, tmp_path, caplog):
        # Pass a path whose parent cannot be created (a file masquerading as dir).
        blocker = tmp_path / "blocker"
        blocker.write_text("I am a file, not a dir")
        month_dir = blocker / "2024_10"  # parent is a file — mkdir will fail

        with caplog.at_level(logging.WARNING, logger="horos_backup"):
            mark_month_done(month_dir)  # must not raise

        assert "Failed to mark .month_done" in caplog.text

    def test_is_idempotent(self, tmp_path):
        month_dir = tmp_path / "2024_11"
        month_dir.mkdir()

        mark_month_done(month_dir)
        mark_month_done(month_dir)  # Second call must not raise.

        assert (month_dir / ".month_done").exists()


# ---------------------------------------------------------------------------
# dump_fs_layout
# ---------------------------------------------------------------------------

class TestDumpFsLayout:
    def test_runs_without_raising(self, temp_config):
        # Must complete gracefully even when directories exist.
        dump_fs_layout(temp_config)

    def test_logs_debug_messages(self, temp_config, caplog):
        with caplog.at_level(logging.DEBUG, logger="horos_backup"):
            dump_fs_layout(temp_config)
        assert "FS layout check" in caplog.text

    def test_logs_failure_on_exception(self, tmp_path, caplog):
        # Construct a config whose horos_data_dir is a file, causing scandir to fail.
        paths = Paths(pacs_root=tmp_path / "PACS")
        paths.pacs_root.mkdir(parents=True, exist_ok=True)
        # Make database_dir a regular file so os.scandir() raises.
        paths.database_dir.parent.mkdir(parents=True, exist_ok=True)
        paths.database_dir.write_text("not a directory")
        config = BackupConfig(paths=paths)

        with caplog.at_level(logging.ERROR, logger="horos_backup"):
            dump_fs_layout(config)  # must not propagate the exception

        assert "dump_fs_layout failed" in caplog.text