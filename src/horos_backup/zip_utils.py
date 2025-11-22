#
# zip_utils.py
# Horos Backup Script
#
# Builds ZIPs atomically in a temp folder and verifies their integrity before marking an export successful.
#
# Thales Matheus Mendonça Santos - November 2025
#
"""ZIP helpers used when exporting studies."""
from __future__ import annotations

import logging
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Iterable, List, Optional


def zip_study_atomic(input_files: Iterable[Path], out_zip: Path):
    # Build the ZIP in a temp dir and rename atomically to avoid partial files.
    out_zip.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = tempfile.mkdtemp(prefix=".export_tmp_", dir=str(out_zip.parent))
    tmp_zip = Path(tmp_dir) / (out_zip.name + ".part")
    try:
        with zipfile.ZipFile(tmp_zip, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            for p in input_files:
                if Path(p).is_file():
                    zf.write(str(p), arcname=Path(p).name)
        tmp_zip.replace(out_zip)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def verify_zip(out_zip: Path, logger: Optional[logging.Logger] = None) -> bool:
    log = logger or logging.getLogger("horos_backup")
    try:
        # testzip() returns the first corrupt filename, or None if OK.
        with zipfile.ZipFile(out_zip, "r") as zf:
            bad = zf.testzip()
            if bad is not None:
                log.error("testzip() encontrou erro em %s: entrada problemática: %s", out_zip, bad)
                return False
            return True
    except Exception as e:
        log.error("Falha ao abrir/testar ZIP %s: %s", out_zip, e)
        return False


__all__ = ["zip_study_atomic", "verify_zip"]
