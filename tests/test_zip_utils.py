#
# test_zip_utils.py
# Horos Backup Script
#
# Validates ZIP creation and verification helpers, including error handling for corrupt archives.
#
# Thales Matheus Mendonça Santos - November 2025
#
import zipfile

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
