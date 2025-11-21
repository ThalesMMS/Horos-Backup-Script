# Horos → Automated CT/MR Backups (ZIP with resume support)

The project automates bulk backup of CT and MR DICOM studies from the Horos database on macOS. It produces a ZIP per study, grouped into monthly folders, and it can resume safely after crashes or power failures. The automation was built for large archives (2+ TB, 50k–130k studies) running on older Macs where manual exports are impractical.

> Looking for the original Portuguese documentation? See `docs/README.pt.md`.

## Repository layout

- `src/horos_backup_export.py` — main backup script.
- `src/horos_backup/` — modularised backup logic (config, logging, helpers).
- `launchd/com.horos.backup.plist` — LaunchAgent definition used for scheduling.
- `docs/` — auxiliary material (Portuguese guide, helper SQL queries, assistant notes).

## Key capabilities

- Filters by modality and only exports CT/MR studies (`MODS = ("CT", "MR")`).
- Processes studies in small batches (`BATCH_SIZE = 15`) every LaunchAgent run.
- Builds a ZIP per study with the pattern `Patient_DOB_StudyDate_UID.zip`.
- Writes ZIPs into `YYYY_MM/` folders and marks completed months via `.month_done`.
- Uses an on-disk state database to avoid re-exporting the same study twice.
- Validates every ZIP with `testzip()` and retries up to three times before logging an issue.
- Pauses automatically when `INCOMING.noindex` has more than 25k files to avoid interfering with Horos imports.

## Runtime workflow

1. Ensure the external volume `/Volumes/PACS` is mounted and contains the sentinel file `.pacs_sentinel`.  
2. Acquire an exclusive lock (`.tmp/.run.lock`) to avoid overlapping runs.  
3. Abort the cycle when `INCOMING.noindex` exceeds `INCOMING_MAX_FILES`.  
4. Remove the newest month folder when it lacks the marker `.month_done` (guarantees clean resumptions).  
5. Create or reuse a consistent snapshot of `Database.sql` before querying.  
6. Query the Horos database for the next batch of unexported CT/MR studies (stable order by study date or date added).  
7. Resolve each study’s files, generate a ZIP atomically (`.part` rename), run `testzip()`, and record success.  
8. Mark every touched month as done once at least one ZIP was created for it.

### Backup layout produced on the PACS volume

```
/Volumes/PACS
├── Database/Horos Data/
│   ├── Database.sql
│   └── INCOMING.noindex/
└── Backup/
    ├── horos_backup_export.py
    ├── export_state.sqlite
    ├── issues.csv
    ├── logs/horos_backup.log
    ├── 2021_01/
    │   ├── Patient1_1980-05-03_2021-01-10_UID123.zip
    │   └── .month_done
    └── .tmp/
        ├── dbcopy/Database_copy.sql
        └── .run.lock
```

## Safeguards you get out of the box

- **Sentinel check** — refuses to run without `/Volumes/PACS/.pacs_sentinel`, which avoids writing to the internal SSD by mistake.  
- **Launch lock** — file locking ensures a new run waits if the previous one takes longer than expected.  
- **Import friendly** — skips runs during heavy Horos imports (`INCOMING_MAX_FILES = 25_000`).  
- **Atomic ZIP creation** — writes to a temporary file and renames only when complete.  
- **Integrity validation** — every ZIP is verified; failures are retried and logged to `issues.csv`.  
- **Deterministic naming** — preserves the full Study UID and keeps filenames under 128 characters.  
- **Stateful exports** — `export_state.sqlite` prevents duplicate work.

## Requirements

- macOS with the built-in `launchd`.  
- `/usr/bin/python3` (CPython 3.8 or newer, no external dependencies).  
- Horos data residing at `/Volumes/PACS/Database/Horos Data/`.

## Installation

1. **Create the sentinel on the external volume**
   ```bash
   touch "/Volumes/PACS/.pacs_sentinel"
   ```
2. **Copy the automation files**
 ```bash
 cp src/horos_backup_export.py "/Volumes/PACS/Backup/"
 cp -R src/horos_backup "/Volumes/PACS/Backup/horos_backup"
  cp launchd/com.horos.backup.plist ~/Library/LaunchAgents/
  ```
3. **Grant execute permission to the script**
   ```bash
   chmod +x "/Volumes/PACS/Backup/horos_backup_export.py"
   ```
4. **Load the LaunchAgent**
   ```bash
   launchctl load ~/Library/LaunchAgents/com.horos.backup.plist
   ```
5. **Run immediately (optional)**
   ```bash
   launchctl start com.horos.backup
   ```

## Operations and monitoring

- Tail the rotated log (100 MB × 10):
  ```bash
  tail -f "/Volumes/PACS/Backup/logs/horos_backup.log"
  ```
- Inspect `launchd` outputs when debugging:
  ```bash
  tail -f /tmp/horos_backup_export.out /tmp/horos_backup_export.err
  ```
- Track issues such as `NO_FILES`, `ZIP_FAIL`, or `INCOMING_OVER_LIMIT` in:
  ```
  /Volumes/PACS/Backup/issues.csv
  ```

## Useful script parameters

- `MODS = ("CT", "MR")` — modalities to export.  
- `BATCH_SIZE = 15` — studies processed per run.  
- `SLEEP_BETWEEN_STUDIES = 1` — throttling between ZIPs (seconds).  
- `ORDER_BY = "study_date"` — switch to `"date_added"` to process newest imports first.  
- `INCOMING_MAX_FILES = 25_000` — guardrail while Horos is importing.  
- `USE_DB_COPY = True` — creates a fresh SQLite snapshot each run to pick up new studies safely.  
- `MAX_NAME_NOEXT = 128` — maximum filename length (without extension).  
- `LOG_MAX_BYTES = 100 * 1024 * 1024` and `LOG_BACKUP_COUNT = 10` — log rotation policy.

> To change the ordering, set `ORDER_BY = "date_added"` inside `src/horos_backup_export.py`. The SQL query will then sort by `ZSTUDY.ZDATEADDED` followed by the Study UID.

## Quick manual run

Useful for smoke tests between scheduled runs.

```bash
/usr/bin/python3 "/Volumes/PACS/Backup/horos_backup_export.py"
```

Temporarily set `BATCH_SIZE = 3` inside the script if you only want to exercise a few studies.

## Development and tests

The automation is modularised under `src/horos_backup/`. Run the unit tests locally with:

```bash
pytest
```

## Troubleshooting

- **“Sentinel missing” fatal error** — create the sentinel file on the correct external volume.  
- **`INCOMING_OVER_LIMIT` keeps showing up** — Horos is importing; wait for the folder to fall below the limit.  
- **`NO_FILES` entries in `issues.csv`** — the database references are stale; check the study paths on disk.  
- **`ZIP_FAIL` entries** — the ZIP remained invalid after three attempts; inspect disk I/O and permissions.  
- **Switching to `date_added` order** — update `ORDER_BY` as noted above.

## Privacy and safety reminders

Exported ZIP filenames contain patient names and dates. Make sure data sharing complies with your organisation’s privacy policies. Encryption at rest is not part of this automation; enable APFS encryption if required.

## Credits

This automation delivers reliable DICOM backups with minimal operator effort while respecting Horos’ import workload. Contributions and feedback are welcome.

## License

Released under the [MIT License](LICENSE).
