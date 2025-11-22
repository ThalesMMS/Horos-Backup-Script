#
# queries.py
# Horos Backup Script
#
# Stores the SQL fragments used to fetch studies and their images from the Horos database, adapting to the configured ordering.
#
# Thales Matheus Mendonça Santos - November 2025
#
"""SQL statements shared by the pipeline."""

from .config import BackupConfig


QUERY_IMAGE_PATHS_BY_STUDY_PK = """
    SELECT DISTINCT
        i.ZPATHSTRING,
        i.ZPATHNUMBER,
        i.ZSTOREDINDATABASEFOLDER
    FROM ZSERIES s
    JOIN ZIMAGE  i ON i.ZSERIES = s.Z_PK
    WHERE s.ZSTUDY = ?
      AND i.ZPATHSTRING IS NOT NULL
      AND i.ZPATHSTRING <> '';
"""


def build_studies_query(config: BackupConfig) -> str:
    mods_placeholders = ",".join("?" * len(config.settings.mods))

    query_by_studydate = f"""
        WITH CandidateStudies AS (
          -- Build a set of studies that have at least one CT/MR series.
          SELECT
            st.Z_PK                   AS studyPK,
            st.ZSTUDYINSTANCEUID      AS studyUID,
            COALESCE(st.ZDATE,'')     AS studyDate,
            COALESCE(st.ZDATEOFBIRTH,'') AS dob,
            COALESCE(st.ZNAME,'')     AS patientName
          FROM ZSTUDY st
          WHERE EXISTS (
            SELECT 1
            FROM ZSERIES s
            WHERE s.ZSTUDY = st.Z_PK
              AND TRIM(UPPER(COALESCE(s.ZMODALITY,''))) IN ({mods_placeholders})
          )
        )
        SELECT cs.*
        FROM CandidateStudies cs
        LEFT JOIN state.Exported ex ON ex.studyInstanceUID = cs.studyUID
        WHERE ex.studyInstanceUID IS NULL
        -- Order deterministically so repeated runs pick up the same studies.
        ORDER BY cs.studyDate ASC, cs.studyUID ASC
        LIMIT ?;
    """

    query_by_dateadded = f"""
        WITH CandidateStudies AS (
          -- Alternative ordering: process newest imports first.
          SELECT
            st.Z_PK                      AS studyPK,
            st.ZSTUDYINSTANCEUID         AS studyUID,
            COALESCE(st.ZDATEADDED,'')   AS dateAdded,
            COALESCE(st.ZDATE,'')        AS studyDate,
            COALESCE(st.ZDATEOFBIRTH,'') AS dob,
            COALESCE(st.ZNAME,'')        AS patientName
          FROM ZSTUDY st
          WHERE EXISTS (
            SELECT 1
            FROM ZSERIES s
            WHERE s.ZSTUDY = st.Z_PK
              AND TRIM(UPPER(COALESCE(s.ZMODALITY,''))) IN ({mods_placeholders})
          )
        )
        SELECT cs.*
        FROM CandidateStudies cs
        LEFT JOIN state.Exported ex ON ex.studyInstanceUID = cs.studyUID
        WHERE ex.studyInstanceUID IS NULL
        ORDER BY cs.dateAdded ASC, cs.studyUID ASC
        LIMIT ?;
    """

    return query_by_dateadded if config.settings.order_by == "date_added" else query_by_studydate


__all__ = ["QUERY_IMAGE_PATHS_BY_STUDY_PK", "build_studies_query"]
