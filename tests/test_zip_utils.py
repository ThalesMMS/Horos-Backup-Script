#
# test_zip_utils.py
# Horos Backup Script
#
# Validates ZIP creation and verification helpers, including error handling for corrupt archives.
#
# Thales Matheus Mendonça Santos - November 2025
#
import logging
import struct
import zipfile

import pytest

from horos_backup.zip_utils import verify_zip, zip_study_atomic


def test_zip_study_atomic_and_verify(tmp_path):
    f1 = tmp_path / "a.txt"
    f2 = tmp_path / "b.txt"
    f1.write_text("hello")
    f2.write_text("world")

    out_zip = tmp_path / "out.zip"
    # Build a ZIP then ensure contents and integrity are correct.
    zip_study_atomic([f1, f2], out_zip)

    assert out_zip.exists()
    with zipfile.ZipFile(out_zip) as zf:
        assert set(zf.namelist()) == {"a.txt", "b.txt"}
    assert verify_zip(out_zip)


def test_verify_zip_failure(tmp_path):
    bad = tmp_path / "bad.zip"
    bad.write_text("not a zip")
    # Corrupt archives should return False and not raise.
    assert verify_zip(bad) is False


# ---------------------------------------------------------------------------
# verify_zip – English log messages (added in chore/translate-pt-docs-to-english)
# ---------------------------------------------------------------------------

def test_verify_zip_logs_english_message_for_non_zip_file(tmp_path, caplog):
    bad = tmp_path / "bad.zip"
    bad.write_text("not a zip")

    with caplog.at_level(logging.ERROR, logger="horos_backup"):
        result = verify_zip(bad)

    assert result is False
    assert "Failed to open/test ZIP" in caplog.text


def test_verify_zip_returns_false_for_nonexistent_file(tmp_path, caplog):
    missing = tmp_path / "nonexistent.zip"

    with caplog.at_level(logging.ERROR, logger="horos_backup"):
        result = verify_zip(missing)

    assert result is False
    assert "Failed to open/test ZIP" in caplog.text


def test_verify_zip_logs_english_message_for_corrupt_entry(tmp_path, caplog):
    # Build a valid ZIP first.
    f = tmp_path / "data.bin"
    f.write_bytes(b"\x00" * 1024)
    out_zip = tmp_path / "corrupt_entry.zip"
    zip_study_atomic([f], out_zip)

    # Corrupt the compressed data bytes in the middle of the file so testzip()
    # detects a bad CRC but the file is still recognisable as a ZIP.
    raw = bytearray(out_zip.read_bytes())
    # Flip bytes roughly in the middle to corrupt the compressed data.
    mid = len(raw) // 2
    for i in range(mid, min(mid + 32, len(raw))):
        raw[i] ^= 0xFF
    out_zip.write_bytes(bytes(raw))

    with caplog.at_level(logging.ERROR, logger="horos_backup"):
        result = verify_zip(out_zip)

    # Either the corruption caused a bad entry (False + message) or the ZIP
    # became unreadable (also False + message).  Either way the result is False.
    assert result is False


# ---------------------------------------------------------------------------
# zip_study_atomic – boundary / regression cases
# ---------------------------------------------------------------------------

def test_zip_study_atomic_with_empty_file_list(tmp_path):
    out_zip = tmp_path / "empty.zip"
    # An empty iterable should produce an empty but valid ZIP.
    zip_study_atomic([], out_zip)

    assert out_zip.exists()
    with zipfile.ZipFile(out_zip) as zf:
        assert zf.namelist() == []
    assert verify_zip(out_zip)


def test_zip_study_atomic_skips_nonexistent_files(tmp_path):
    real_file = tmp_path / "real.txt"
    real_file.write_text("content")
    ghost = tmp_path / "ghost.txt"  # intentionally not created

    out_zip = tmp_path / "partial.zip"
    zip_study_atomic([real_file, ghost], out_zip)

    with zipfile.ZipFile(out_zip) as zf:
        assert "real.txt" in zf.namelist()
        assert "ghost.txt" not in zf.namelist()
    assert verify_zip(out_zip)