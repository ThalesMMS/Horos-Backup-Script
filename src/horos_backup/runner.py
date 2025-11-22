#
# runner.py
# Horos Backup Script
#
# Coordinates one full export cycle: validating guardrails, preparing the database snapshot, selecting studies, zipping files, and marking results.
#
# Thales Matheus Mendonça Santos - November 2025
#
"""Run a single backup/export cycle."""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Dict, Optional

from .config import BackupConfig, DEFAULT_CONFIG
from .dates import debug_dump_date, month_dir_for
from .db_snapshot import choose_db_path
from .fs_utils import (
    count_files_early,
    dump_fs_layout,
    ensure_dirs,
    ensure_volume_mounted,
    mark_month_done,
    reset_incomplete_latest_month,
    resolve_image_path,
)
from .issues import issues_log
from .locks import acquire_lock, release_lock
from .logging_setup import setup_logging
from .naming import build_zip_path
from .queries import QUERY_IMAGE_PATHS_BY_STUDY_PK, build_studies_query
from .state import mark_exported, state_connect
from .zip_utils import verify_zip, zip_study_atomic


def run_once(config: BackupConfig = DEFAULT_CONFIG, logger: Optional[logging.Logger] = None):
    log = logger or setup_logging(config)
    # Ensure the required folder structure and external drive are present.
    ensure_dirs(config)
    ensure_volume_mounted(config)

    # Prevent overlapping runs by acquiring a lock; skip if already running.
    lock_fh = acquire_lock(config.paths.lockfile_path)
    if lock_fh is None:
        log.info("Execução anterior ainda em curso; esta rodada será pulada.")
        return

    try:
        # Avoid hammering Horos during heavy imports by bailing out early.
        incoming_count = count_files_early(config.paths.incoming_dir, config.settings.incoming_max_files)
        log.info(
            "INCOMING.noindex: ~%d arquivos (limiar %d)", incoming_count, config.settings.incoming_max_files
        )
        if incoming_count > config.settings.incoming_max_files:
            log.warning("INCOMING.noindex acima do limiar; pulando exportação desta rodada.")
            issues_log(config, "INCOMING_OVER_LIMIT", "-", f"count={incoming_count}", {"limit": config.settings.incoming_max_files})
            return

        # Clean up the newest month folder if it was left half-written.
        reset_incomplete_latest_month(config, logger=log)
        dump_fs_layout(config, logger=log)

        try:
            # Create or reuse a consistent SQLite snapshot for read-only querying.
            db_path = choose_db_path(config, logger=log)
        except Exception:
            log.exception("Falha ao preparar snapshot do DB.")
            raise

        try:
            # Open the snapshot in query-only mode to match macOS Horos DB.
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            try:
                conn.execute("PRAGMA query_only=ON;")
            except Exception:
                pass
            cur = conn.cursor()
            log.debug("Conexão SQLite ok. sqlite_version=%s", sqlite3.sqlite_version)
        except Exception:
            log.exception("Erro ao abrir snapshot SQLite em modo leitura.")
            raise

        state_conn = None
        try:
            # State DB records exported studies so we never duplicate work.
            state_conn = state_connect(config)
            cur.execute("ATTACH DATABASE ? AS state", (f"file:{config.paths.state_db}?mode=ro",))

            try:
                # Quick stats: how many studies are eligible and already exported.
                cur.execute(
                    """
                    WITH cs AS (
                        SELECT DISTINCT s.ZSTUDY AS ZSTUDY
                        FROM ZSERIES s
                        WHERE TRIM(UPPER(COALESCE(s.ZMODALITY,''))) IN ('CT','MR')
                    )
                    SELECT
                        (SELECT COUNT(*) FROM cs) AS total_candidates,
                        (SELECT COUNT(DISTINCT s.ZSTUDY) FROM ZSERIES s WHERE TRIM(UPPER(COALESCE(s.ZMODALITY,'')))='CT') AS series_ct,
                        (SELECT COUNT(DISTINCT s.ZSTUDY) FROM ZSERIES s WHERE TRIM(UPPER(COALESCE(s.ZMODALITY,'')))='MR') AS series_mr,
                        (
                        SELECT COUNT(*)
                        FROM cs
                        JOIN ZSTUDY st ON st.Z_PK = cs.ZSTUDY
                        JOIN state.Exported ex ON ex.studyInstanceUID = st.ZSTUDYINSTANCEUID
                        ) AS exported_in_candidates
                """
                )
                row = cur.fetchone()
                total_candidates = int(row["total_candidates"])
                series_ct = int(row["series_ct"])
                series_mr = int(row["series_mr"])
                exported_in_candidates = int(row["exported_in_candidates"])
                pending = max(0, total_candidates - exported_in_candidates)

                log.info(
                    "Snapshot (series-level): CT(estudos)=%d; MR(estudos)=%d; CT∪MR(estudos únicos)=%d",
                    series_ct,
                    series_mr,
                    total_candidates,
                )
                log.info(
                    "Snapshot: candidatos(CT/MR)=%d; já exportados(dentro dos candidatos)=%d; pendentes ~%d",
                    total_candidates,
                    exported_in_candidates,
                    pending,
                )
            except Exception:
                log.exception("Falha ao calcular snapshot de candidatos/exportados")

            studies_query = build_studies_query(config)
            cur.execute(studies_query, (*config.settings.mods, config.settings.batch_size))
            studies = cur.fetchall()

            log.info(
                "Estudos selecionados para o lote: %d (ORDER_BY=%s, MODS=%s, BATCH_SIZE=%d)",
                len(studies),
                config.settings.order_by,
                config.settings.mods,
                config.settings.batch_size,
            )
            for i, r in enumerate(studies[:3]):
                keys = r.keys()
                zdate = r["studyDate"] if "studyDate" in keys else (r["dateAdded"] if "dateAdded" in keys else "")
                dob = r["dob"] if "dob" in keys else ""
                pname = r["patientName"] if "patientName" in keys else ""
                log.debug(
                    "Study[%d] PK=%s UID=%s DATE=%r DOB=%r NAME=%r",
                    i,
                    r["studyPK"],
                    r["studyUID"],
                    zdate,
                    dob,
                    pname,
                )

            if not studies:
                log.info("Nada a exportar neste ciclo.")
                return

            zips_by_month: Dict[Path, int] = {}

            for row in studies:
                study_pk = row["studyPK"]
                study_uid = row["studyUID"]
                rk = row.keys()
                # Fall back to dateAdded when the configured ordering requires it.
                study_ts = row["studyDate"] if "studyDate" in rk else (row["dateAdded"] if "dateAdded" in rk else "")
                dob_ts = row["dob"]
                patient_nm = row["patientName"]

                debug_dump_date("study_date", study_ts, logger=log)
                debug_dump_date("dob", dob_ts, logger=log)

                month_dir = month_dir_for(study_ts, config)
                log.debug("month_dir=%s", month_dir)

                out_zip = build_zip_path(month_dir, patient_nm, dob_ts, study_ts, study_uid, config)
                log.debug("ZIP destino: %s", out_zip)

                cur2 = conn.cursor()
                # Fetch all image paths for the current study.
                cur2.execute(QUERY_IMAGE_PATHS_BY_STUDY_PK, (study_pk,))
                rows_img = cur2.fetchall()

                files = []
                debug_checked = []

                for r in rows_img:
                    zpathstring = r["ZPATHSTRING"]
                    zpathnumber = r["ZPATHNUMBER"]
                    zstored_in = r["ZSTOREDINDATABASEFOLDER"]

                    # Resolve possible locations following Horos conventions.
                    p = resolve_image_path(zpathstring, zpathnumber, zstored_in, config)
                    exists = p.is_file()
                    if exists:
                        files.append(p)

                    try:
                        in_db_flag = int(zstored_in) == 1
                    except Exception:
                        in_db_flag = False

                    if len(debug_checked) < 5:
                        log.debug(
                            "IMG CAND: ZPATHSTRING=%r ZPATHNUMBER=%r ZINDB=%r -> resolved=%s exists=%s",
                            zpathstring,
                            zpathnumber,
                            zstored_in,
                            p,
                            exists,
                        )
                    if len(debug_checked) < 5:
                        if in_db_flag:
                            s_local = str(zpathstring or "").lstrip("/")
                            sub_local = (str(zpathnumber).strip() if zpathnumber is not None else "")
                            cand1 = (config.paths.database_dir / sub_local / s_local) if sub_local else (config.paths.database_dir / s_local)
                            cand2 = (config.paths.database_dir / s_local)
                            debug_checked.append((str(cand1), cand1.is_file()))
                            if str(cand2) != str(cand1) and len(debug_checked) < 5:
                                debug_checked.append((str(cand2), cand2.is_file()))
                        else:
                            debug_checked.append((str(p), exists))

                log.debug("ZIMAGE rows para study_pk=%s: %d; encontrados=%d", study_pk, len(rows_img), len(files))

                if not files:
                    # Record missing data instead of failing the entire batch.
                    log.warning("Estudo %s: nenhum arquivo encontrado. Marcando como NO_FILES.", study_uid)
                    issues_log(
                        config,
                        "NO_FILES",
                        study_uid,
                        "Nenhum arquivo válido encontrado",
                        {"study_pk": int(study_pk), "checked": debug_checked},
                    )
                    log.debug("Caminhos tentados (amostra): %s", debug_checked)
                    continue

                ok = False
                attempts = 0
                # Retry ZIP creation up to 3 times to dodge transient I/O issues.
                while attempts < 3 and not ok:
                    attempts += 1
                    try:
                        log.info("Exportando %s -> %s (tentativa %d)", study_uid, out_zip, attempts)
                        zip_study_atomic(files, out_zip)
                        if verify_zip(out_zip, logger=log):
                            try:
                                zip_size = os.stat(out_zip).st_size
                            except Exception:
                                zip_size = -1
                            log.info("OK: %s (arquivos=%d, tamanho=%d bytes)", out_zip.name, len(files), zip_size)
                            # Persist export metadata so future runs skip this study.
                            mark_exported(state_conn, study_uid, out_zip)
                            zips_by_month[month_dir] = zips_by_month.get(month_dir, 0) + 1
                            ok = True
                        else:
                            try:
                                out_zip.unlink(missing_ok=True)
                            except TypeError:
                                if out_zip.exists():
                                    out_zip.unlink()
                            time.sleep(1)
                    except Exception:
                        log.exception("Falha ao exportar %s", study_uid)
                        try:
                            if out_zip.exists():
                                out_zip.unlink()
                        except Exception:
                            pass
                        time.sleep(1)

                if not ok:
                    log.error("Estudo %s: falha após %d tentativas. Registrando em issues.csv.", study_uid, attempts)
                    issues_log(
                        config,
                        "ZIP_FAIL",
                        study_uid,
                        f"Falha após {attempts} tentativas",
                        {"zip_path": str(out_zip), "files": len(files)},
                    )

                time.sleep(config.settings.sleep_between_studies)

            for month_dir, cnt in zips_by_month.items():
                if cnt > 0:
                    # Mark months that received any ZIPs as complete for resilience.
                    mark_month_done(month_dir, logger=log)
        finally:
            try:
                conn.close()
            except Exception:
                pass
            if state_conn is not None:
                try:
                    state_conn.close()
                except Exception:
                    pass
    finally:
        release_lock(lock_fh)


__all__ = ["run_once"]
