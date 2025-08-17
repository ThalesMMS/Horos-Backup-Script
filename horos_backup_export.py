#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, sqlite3, time, shutil, zipfile, tempfile, fcntl, sys, csv, logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime

# ====================== CONFIG ======================

PACS_ROOT         = Path("/Volumes/PACS")

# Sentinela para garantir que o volume externo está montado
SENTINEL          = PACS_ROOT / ".pacs_sentinel"

# Diretórios do Horos no DISCO EXTERNO
HOROS_DATA_DIR    = PACS_ROOT / "Database" / "Horos Data"
HOROS_DB_ORIG     = HOROS_DATA_DIR / "Database.sql"
INCOMING_DIR      = HOROS_DATA_DIR / "INCOMING.noindex"

# Limiar de sobrecarga: contar arquivos em INCOMING.noindex (early-stop > 25k)
INCOMING_MAX_FILES = 25_000

# Backup/export no mesmo disco
BACKUP_ROOT       = PACS_ROOT / "Backup"
TMP_ROOT          = BACKUP_ROOT / ".tmp"
DBCOPY_DIR        = TMP_ROOT / "dbcopy"
DBCOPY_PATH       = DBCOPY_DIR / "Database_copy.sql"
STATE_DB          = BACKUP_ROOT / "export_state.sqlite"
LOCKFILE_PATH     = TMP_ROOT / ".run.lock"

# Logs (rotação por tamanho)
LOGS_DIR          = BACKUP_ROOT / "logs"
LOG_FILE          = LOGS_DIR / "horos_backup.log"
LOG_MAX_BYTES     = 100 * 1024 * 1024  # 100 MB
LOG_BACKUP_COUNT  = 10

# Critério de ordenação
# "study_date" => usa ZSTUDY.ZDATE
# "date_added" => usa ZSTUDY.ZDATEADDED
ORDER_BY          = "study_date"

# Parâmetros do lote
BATCH_SIZE        = 15
SLEEP_BETWEEN_STUDIES = 1  # segundos

# Modalidades alvo
MODS              = ("CT", "MR")

# Nome: limite sem extensão (preservar UID sempre)
MAX_NAME_NOEXT    = 128

# CSV de issues
ISSUES_CSV        = BACKUP_ROOT / "issues.csv"

# ====================================================

SANITIZE_RE = re.compile(r'[^0-9A-Za-z._-]+', re.UNICODE)

# ---------- Logging ----------

def setup_logging():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("horos_backup")
    logger.setLevel(logging.INFO)

    # Rotating file handler
    fh = RotatingFileHandler(str(LOG_FILE), maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    # Console (stdout) para visibilidade básica no launchd
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

log = setup_logging()

# ---------- Utilidades ----------

def ensure_dirs():
    BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    DBCOPY_DIR.mkdir(parents=True, exist_ok=True)

def ensure_volume_mounted():
    if not PACS_ROOT.exists() or not SENTINEL.exists():
        raise RuntimeError(f"Volume externo não montado OU sentinela ausente: {PACS_ROOT}\n"
                           f"Crie o arquivo sentinela: {SENTINEL}")

def sanitize_name(s: str) -> str:
    s = (s or "").strip().replace(" ", "_")
    s = SANITIZE_RE.sub("_", s)
    return (s[:128] or "UNKNOWN")

def parse_timestamp_to_parts(ts):
    """
    Tenta extrair YYYY, MM, DD de strings tipo '2023-07-19 12:34:56 +0000' ou '20230719'.
    Retorna (YYYY, MM, DD) ou (None, None, None).
    """
    if not ts:
        return (None, None, None)
    ts = str(ts)
    # Procurar padrão YYYY-MM-DD ou YYYYMMDD
    m = re.search(r'(\d{4})[-/]?(\d{2})[-/]?(\d{2})?', ts)
    if not m:
        return (None, None, None)
    y = m.group(1)
    mo = m.group(2)
    d = m.group(3) if m.lastindex and m.lastindex >= 3 else None
    return (y, mo, d)

def fmt_date_for_name(ts, fallback="UNKNOWN"):
    y, mo, d = parse_timestamp_to_parts(ts)
    if y and mo and d:
        return f"{y}-{mo}-{d}"
    elif y and mo:
        return f"{y}-{mo}-01"
    else:
        return fallback

def month_dir_for(ts) -> Path:
    y, mo, _ = parse_timestamp_to_parts(ts)
    if y and mo:
        return BACKUP_ROOT / f"{y}_{mo}"
    return BACKUP_ROOT / "UNKNOWN_DATE"

from typing import Optional
def latest_incomplete_month_folder() -> Optional[Path]:
    months = [p for p in BACKUP_ROOT.glob("[0-9][0-9][0-9][0-9]_[0-1][0-9]") if p.is_dir()]
    if not months:
        return None
    latest = sorted(months)[-1]
    if not (latest / ".month_done").exists():
        return latest
    return None

def reset_incomplete_latest_month():
    mf = latest_incomplete_month_folder()
    if mf and mf.exists():
        log.warning(f"Removendo mês incompleto: {mf}")
        shutil.rmtree(mf, ignore_errors=True)

def mark_month_done(month_dir: Path):
    try:
        (month_dir / ".month_done").touch()
    except Exception as e:
        log.warning(f"Falha ao marcar .month_done em {month_dir}: {e}")

from typing import List
def zip_study_atomic(input_files: List[Path], out_zip: Path):
    out_zip.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = tempfile.mkdtemp(prefix=".export_tmp_", dir=str(out_zip.parent))
    tmp_zip = Path(tmp_dir) / (out_zip.name + ".part")
    try:
        with zipfile.ZipFile(tmp_zip, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            for p in input_files:
                if p.is_file():
                    zf.write(str(p), arcname=p.name)
        # Movimento atômico (mesmo volume)
        tmp_zip.replace(out_zip)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

def verify_zip(out_zip: Path) -> bool:
    try:
        with zipfile.ZipFile(out_zip, "r") as zf:
            bad = zf.testzip()  # retorna o primeiro arquivo problemático ou None
            if bad is not None:
                log.error(f"testzip() encontrou erro em {out_zip}: entrada problemática: {bad}")
                return False
            return True
    except Exception as e:
        log.error(f"Falha ao abrir/testar ZIP {out_zip}: {e}")
        return False

# ---------- Estado (estudos já exportados) ----------

def state_connect():
    conn = sqlite3.connect(str(STATE_DB))
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Exported (
            studyInstanceUID TEXT PRIMARY KEY,
            when_exported     TEXT NOT NULL,
            zip_path          TEXT NOT NULL
        );
    """)
    conn.commit()
    return conn

def mark_exported(state_conn, study_uid: str, zip_path):
    cur = state_conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO Exported (studyInstanceUID, when_exported, zip_path) VALUES (?, datetime('now'), ?);",
        (study_uid, str(zip_path))
    )
    state_conn.commit()

# ---------- Cópia consistente do DB (API de backup do SQLite) ----------

def copy_horos_db_consistent() -> Path:
    if not HOROS_DB_ORIG.exists():
        raise FileNotFoundError(f"DB do Horos não encontrado: {HOROS_DB_ORIG}")

    if DBCOPY_PATH.exists():
        try: DBCOPY_PATH.unlink()
        except Exception: pass

    src = sqlite3.connect(f"file:{HOROS_DB_ORIG}?mode=ro", uri=True)
    try:
        src.execute("PRAGMA query_only=ON;")
    except Exception:
        pass

    dst = sqlite3.connect(str(DBCOPY_PATH))
    try:
        src.backup(dst)
        dst.commit()
    finally:
        dst.close()
        src.close()

    return DBCOPY_PATH

# ---------- SQL adaptado ao seu schema ----------

# Seleciona estudos CT/MR ainda não exportados.
# - ZSERIES.ZSTUDY -> FK para ZSTUDY.Z_PK
# - Modalidade por série (mais confiável)
# - Campos usados:
#   * st.Z_PK (chave interna do estudo)  -> para buscar imagens
#   * st.ZSTUDYINSTANCEUID               -> UID único do estudo
#   * st.ZDATE                           -> data do estudo (TIMESTAMP string)
#   * st.ZDATEOFBIRTH                    -> data de nascimento
#   * st.ZNAME                           -> nome do paciente
QUERY_STUDIES_BY_STUDYDATE = f"""
    WITH Mods AS (
        SELECT DISTINCT s.ZSTUDY
        FROM ZSERIES s
        WHERE UPPER(COALESCE(s.ZMODALITY,'')) IN ({",".join("?"*len(MODS))})
    )
    SELECT
        st.Z_PK                   AS studyPK,
        st.ZSTUDYINSTANCEUID      AS studyUID,
        COALESCE(st.ZDATE,'')     AS studyDate,
        COALESCE(st.ZDATEOFBIRTH,'') AS dob,
        COALESCE(st.ZNAME,'')     AS patientName
    FROM ZSTUDY st
    JOIN Mods m ON m.ZSTUDY = st.Z_PK
    LEFT JOIN state.Exported ex ON ex.studyInstanceUID = st.ZSTUDYINSTANCEUID
    WHERE ex.studyInstanceUID IS NULL
    ORDER BY st.ZDATE ASC, st.ZSTUDYINSTANCEUID ASC
    LIMIT ?;
"""

QUERY_STUDIES_BY_DATEADDED = f"""
    WITH Mods AS (
        SELECT DISTINCT s.ZSTUDY
        FROM ZSERIES s
        WHERE UPPER(COALESCE(s.ZMODALITY,'')) IN ({",".join("?"*len(MODS))})
    )
    SELECT
        st.Z_PK                      AS studyPK,
        st.ZSTUDYINSTANCEUID         AS studyUID,
        COALESCE(st.ZDATEADDED,'')   AS dateAdded,
        COALESCE(st.ZDATE,'')        AS studyDate,
        COALESCE(st.ZDATEOFBIRTH,'') AS dob,
        COALESCE(st.ZNAME,'')        AS patientName
    FROM ZSTUDY st
    JOIN Mods m ON m.ZSTUDY = st.Z_PK
    LEFT JOIN state.Exported ex ON ex.studyInstanceUID = st.ZSTUDYINSTANCEUID
    WHERE ex.studyInstanceUID IS NULL
    ORDER BY st.ZDATEADDED ASC, st.ZSTUDYINSTANCEUID ASC
    LIMIT ?;
"""

QUERY_IMAGE_PATHS_BY_STUDY_PK = """
    SELECT DISTINCT i.ZPATHSTRING
    FROM ZSERIES s
    JOIN ZIMAGE  i ON i.ZSERIES = s.Z_PK
    WHERE s.ZSTUDY = ?
      AND i.ZPATHSTRING IS NOT NULL
      AND i.ZPATHSTRING <> '';
"""

# ---------- Lock de execução (evita sobreposição) ----------

def acquire_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    f = open(lock_path, "w")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        f.truncate(0)
        f.write(str(os.getpid()))
        f.flush()
        return f
    except BlockingIOError:
        f.close()
        return None

def release_lock(fh):
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        fh.close()
    except Exception:
        pass

# ---------- Contador de arquivos (com early-stop) ----------

def count_files_early(root: Path, stop_after: int) -> int:
    """
    Conta arquivos de forma iterativa e para assim que exceder stop_after.
    Não soma bytes (mais rápido em muitos arquivos pequenos).
    """
    if not root.exists():
        return 0
    count = 0
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for entry in it:
                    try:
                        if entry.is_file(follow_symlinks=False):
                            count += 1
                            if count > stop_after:
                                return count
                        elif entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
                    except FileNotFoundError:
                        pass
        except FileNotFoundError:
            pass
    return count

# ---------- issues.csv ----------

from typing import Optional, Dict
def issues_log(kind: str, study_uid: str, detail: str = "", extra: Optional[Dict] = None):
    """
    Escreve uma linha em issues.csv: timestamp, kind, study_uid, detail, extras(json)
    """
    ISSUES_CSV.parent.mkdir(parents=True, exist_ok=True)
    new_file = not ISSUES_CSV.exists()
    with open(ISSUES_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["timestamp", "kind", "study_uid", "detail", "extra"])
        w.writerow([datetime.now().isoformat(timespec="seconds"), kind, study_uid, detail,
                    (extra and str(extra)) or ""])

# ---------- Construção de nome do ZIP ----------

def build_zip_path(month_dir: Path, patient_name: str, dob_ts, study_ts, study_uid: str) -> Path:
    """
    Gera nome único sob as regras:
    - Base: Patient_DOB_StudyDate_UID.zip
    - UID deve ser preservado INTEIRO.
    - Limite 128 chars (sem extensão): truncar apenas a parte anterior ao UID.
    - Se colisão mesmo assim: acrescenta _2, _3, ...
    """
    patient = sanitize_name(patient_name)
    dob = fmt_date_for_name(dob_ts, fallback="UNKNOWN")
    sdate = fmt_date_for_name(study_ts, fallback="UNKNOWN")
    uid = sanitize_name(study_uid)

    prefix = f"{patient}_{dob}_{sdate}"
    # Garantir UID preservado
    base_noext = f"{prefix}_{uid}"
    if len(base_noext) > MAX_NAME_NOEXT:
        allow_prefix = max(1, MAX_NAME_NOEXT - (len(uid) + 1))  # 1 para o "_"
        prefix = prefix[:allow_prefix].rstrip("_")
        base_noext = f"{prefix}_{uid}"

    candidate = month_dir / (base_noext + ".zip")
    if not candidate.exists():
        return candidate

    # Desambiguação se ainda colidir
    n = 2
    while True:
        cand = month_dir / (f"{base_noext}_{n}.zip")
        if not cand.exists():
            return cand
        n += 1

# ---------- Execução de uma rodada ----------

def run_once():
    ensure_dirs()
    ensure_volume_mounted()

    # 0) Lock
    lock_fh = acquire_lock(LOCKFILE_PATH)
    if lock_fh is None:
        log.info("Execução anterior ainda em curso; esta rodada será pulada.")
        return

    try:
        # 1) Checagem de sobrecarga (contagem de arquivos em INCOMING)
        incoming_count = count_files_early(INCOMING_DIR, INCOMING_MAX_FILES)
        log.info(f"INCOMING.noindex: ~{incoming_count} arquivos (limiar {INCOMING_MAX_FILES})")
        if incoming_count > INCOMING_MAX_FILES:
            log.warning("INCOMING.noindex acima do limiar; pulando exportação desta rodada.")
            issues_log("INCOMING_OVER_LIMIT", "-", f"count={incoming_count}", {"limit": INCOMING_MAX_FILES})
            return

        # 2) Retomada: apagar último mês incompleto
        reset_incomplete_latest_month()

        # 3) Cópia consistente do DB
        db_copy = copy_horos_db_consistent()
        log.info(f"DB copiado para: {db_copy}")

        # 4) Abre DB copiado em modo leitura
        conn = sqlite3.connect(f"file:{db_copy}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA query_only=ON;")
        except Exception:
            pass
        cur = conn.cursor()

        # 5) Banco de estado e attach
        state_conn = state_connect()
        cur.execute("ATTACH DATABASE ? AS state", (str(STATE_DB),))

        # 6) Seleção do lote
        if ORDER_BY == "date_added":
            cur.execute(QUERY_STUDIES_BY_DATEADDED, (*MODS, BATCH_SIZE))
        else:
            cur.execute(QUERY_STUDIES_BY_STUDYDATE, (*MODS, BATCH_SIZE))
        studies = cur.fetchall()

        if not studies:
            log.info("Nada a exportar neste ciclo.")
            conn.close()
            state_conn.close()
            return

        from typing import Set
        touched_months: Set[Path] = set()
        # ou simplesmente:
        # touched_months = set()

        for row in studies:
            study_pk   = row["studyPK"]
            study_uid  = row["studyUID"]
            study_ts   = row["studyDate"] if "studyDate" in row.keys() else ""
            dob_ts     = row["dob"]
            patient_nm = row["patientName"]

            month_dir  = month_dir_for(study_ts)
            touched_months.add(month_dir)

            out_zip = build_zip_path(month_dir, patient_nm, dob_ts, study_ts, study_uid)

            # Pega caminhos de imagens para o estudo (por Z_PK)
            cur2 = conn.cursor()
            cur2.execute(QUERY_IMAGE_PATHS_BY_STUDY_PK, (study_pk,))
            raw_paths = [r[0] for r in cur2.fetchall() if r[0]]

            # Constrói Paths absolutos
            files = []
            for rp in raw_paths:
                p = Path(rp)
                if not p.is_absolute():
                    p = HOROS_DATA_DIR / rp
                if p.is_file():
                    files.append(p)

            if not files:
                log.warning(f"Estudo {study_uid}: nenhum arquivo encontrado. Marcando como NO_FILES.")
                issues_log("NO_FILES", study_uid, "Nenhum arquivo válido encontrado",
                           {"study_pk": int(study_pk)})
                mark_exported(state_conn, study_uid, "NO_FILES")
                continue

            # Exporta com tentativa + verificação
            ok = False
            attempts = 0
            while attempts < 3 and not ok:
                attempts += 1
                try:
                    log.info(f"Exportando {study_uid} -> {out_zip} (tentativa {attempts})")
                    zip_study_atomic(files, out_zip)
                    if verify_zip(out_zip):
                        mark_exported(state_conn, study_uid, out_zip)
                        ok = True
                    else:
                        # ZIP inconsistente; remove e tenta novamente
                        try:
                            out_zip.unlink(missing_ok=True)  # py>=3.8
                        except TypeError:
                            # Para py<3.8
                            if out_zip.exists():
                                out_zip.unlink()
                        time.sleep(1)
                except Exception as e:
                    log.error(f"Falha ao exportar {study_uid}: {e}")
                    # remove eventual .zip parcial e tenta de novo
                    try:
                        if out_zip.exists():
                            out_zip.unlink()
                    except Exception:
                        pass
                    time.sleep(1)

            if not ok:
                log.error(f"Estudo {study_uid}: falha após {attempts} tentativas. Registrando em issues.csv.")
                issues_log("ZIP_FAIL", study_uid, f"Falha após {attempts} tentativas",
                           {"zip_path": str(out_zip), "files": len(files)})
                # não marca como exportado; tentará novamente no próximo ciclo

            time.sleep(SLEEP_BETWEEN_STUDIES)

        # 7) Marcar meses tocados como concluídos
        for m in touched_months:
            mark_month_done(m)

        conn.close()
        state_conn.close()

    finally:
        release_lock(lock_fh)

if __name__ == "__main__":
    try:
        run_once()
    except Exception as e:
        log.error(f"[FATAL] {e}")
        sys.exit(1)