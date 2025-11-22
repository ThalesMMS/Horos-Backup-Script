#
# test_naming.py
# Horos Backup Script
#
# Checks name sanitization, collision handling, and truncation when generating ZIP filenames.
#
# Thales Matheus Mendonça Santos - November 2025
#
from horos_backup.naming import build_zip_path, sanitize_name


def test_sanitize_name():
    # Special characters become underscores and empty strings become UNKNOWN.
    assert sanitize_name("John Doe @#") == "John_Doe__"
    assert sanitize_name("") == "UNKNOWN"


def test_build_zip_path_collision_and_truncation(tmp_path, temp_config):
    month_dir = tmp_path / "2023_02"
    month_dir.mkdir()

    base_path = build_zip_path(
        month_dir,
        "Patient Name",
        "1980-05-01",
        "2023-02-03",
        "UID123",
        temp_config,
    )
    base_path.touch()

    second = build_zip_path(
        month_dir,
        "Patient Name",
        "1980-05-01",
        "2023-02-03",
        "UID123",
        temp_config,
    )
    # Collision should append a numeric suffix.
    assert second.name.endswith("_2.zip")

    long_name = "A" * 300
    truncated = build_zip_path(month_dir, long_name, "2000-01-01", "2023-02-03", "UID123", temp_config)
    # Generated names must honor the configured max length.
    assert len(truncated.stem) <= temp_config.settings.max_name_noext
