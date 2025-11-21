from pathlib import Path

from horos_backup.fs_utils import count_files_early, ensure_volume_mounted, resolve_image_path


def test_count_files_early(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    for i in range(5):
        (root / f"f{i}").write_text("x")
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
    assert resolved == target


def test_resolve_image_path_relative_outside_db(temp_config):
    target = temp_config.paths.horos_data_dir / "relative" / "image.dcm"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("x")

    resolved = resolve_image_path("relative/image.dcm", None, 0, temp_config)
    assert resolved == target
