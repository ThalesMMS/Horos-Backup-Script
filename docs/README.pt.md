# Horos → Automated CT/MR Study Backups to ZIP (with resume support)

> Note: the legacy filename `README.pt.md` is intentionally preserved for compatibility, but the content below is now in English.

## 📖 Introduction and context

This project automates the **backup of DICOM studies** stored in Horos (a DICOM viewer for macOS), exporting them as **`.zip` files** organized by **month** (`YYYY_MM`).  
It was designed for **high-volume** scenarios (tens of thousands of studies; for example 2 TB / 50k–130k exams), where **manual exporting** is impractical and an **older Mac** (for example a 2014 Mac mini) needs to be **spared unnecessary load**.

**Problems that motivated this solution**:
- Manual export is slow and error-prone.
- Horos may be **importing** studies every 30 minutes; we do not want to compete for I/O or freeze the interface.
- We need **crash-safe resumption** (power loss, freezes, unexpected interruptions).
- We want **monthly organization** and **one ZIP per study**, with useful names (Patient, DOB, Study Date, UID).

---

## 🧭 What this project does

- Exports **CT/MR only**, **15 studies per run**, **every 10 minutes** (configurable).
- Generates **one ZIP per study**, named: `Patient_DOB_StudyDate_UID.zip`.
- Organizes ZIP files into `YYYY_MM` folders (for example `2021_03/`).
- Guarantees **resume support**: if the process stops mid-run, it resumes exactly where it left off.
- Avoids interfering with Horos imports: if `INCOMING.noindex` has **more than 25,000 files**, it **skips** the cycle.
- Verifies ZIP **integrity** with `testzip()`, retries zipping up to 3 times, and if the problem persists, records it in `issues.csv`.
- Ensures you **never write** to the internal SSD by mistake (it uses a **sentinel file** on the external volume).
- Logs everything with **rotation** (100 MB × 10).

---

## 🏗️ Workflow summary

1. Checks whether `/Volumes/PACS` is **mounted** (using the `.pacs_sentinel` sentinel).  
2. Ensures there is **no other run** currently in progress (file lock).
3. Checks the file count in `INCOMING.noindex`; if it is **greater than 25k**, it **skips** this cycle.
4. Removes the **most recent monthly folder** if it is **incomplete** (missing `.month_done`).
5. Creates a **consistent copy** of `Database.sql` (using SQLite’s **backup** API).
6. Selects the **15 oldest studies** (CT/MR) **not yet exported** (stable ordering by date + UID).
7. For each study: gathers the `.dcm` files, creates an **atomic ZIP** (`.part` → rename), runs **`testzip()`**, and records the study as exported.
8. Marks the **touched months** as completed (`.month_done`).

---

## 📂 Backup structure
```text
/Volumes/PACS
├── Database/
│   └── Horos Data/
│       ├── Database.sql
│       └── INCOMING.noindex/
└── Backup/
    ├── horos_backup_export.py
    ├── export_state.sqlite
    ├── issues.csv
    ├── logs/
    │   └── horos_backup.log
    ├── 2021_01/
    │   ├── Patient1_1980-05-03_2021-01-10_UID123.zip
    │   └── .month_done
    ├── 2021_02/
    │   ├── ...
    └── .tmp/
        ├── dbcopy/Database_copy.sql
        └── .run.lock
```

---

## 🔒 Rules and safeguards
- **Sentinel volume**: requires `/Volumes/PACS/.pacs_sentinel`. Without it, the script **aborts** (to avoid writing to the internal SSD).
- **Execution lock**: prevents overlapping runs (if one run lasts longer than 10 minutes, the next one **waits**).
- **INCOMING.noindex**: if there are **more than 25,000 files**, the run is **skipped** (Horos is likely reimporting).
- **Monthly resume rule**: if the newest `YYYY_MM` folder does **not** contain `.month_done`, it is **deleted** and rebuilt.
- **Atomic ZIP creation**: writes a `.part` file and only then renames it to `.zip` (avoids exposing corrupted ZIPs).
- **Integrity check**: `testzip()` runs after every export; up to **3 attempts** are made before logging `ZIP_FAIL`.
- **Unique names**: preserves the full **UID**; truncates names to **128** characters; if there is a collision, suffixes like `_2`, `_3`, etc. are used.
- **State tracking**: `export_state.sqlite` stores exported `studyUID`s (so they are not exported again).

---

## ✅ Requirements
- macOS (with the system-provided **launchd**).
- **Python 3.8+** at `/usr/bin/python3` (no external dependencies).
- Horos with its database located at `/Volumes/PACS/Database/Horos Data/`.

---

## 🚀 Installation

1) **Create the sentinel on the PACS volume**
```bash
touch "/Volumes/PACS/.pacs_sentinel"
```

2) **Copy the files**
```text
/Volumes/PACS/Backup/horos_backup_export.py
~/Library/LaunchAgents/com.horos.backup.plist
```

3) **Grant execute permission**
```bash
chmod +x "/Volumes/PACS/Backup/horos_backup_export.py"
```

4) **Load the LaunchAgent**
```bash
launchctl load ~/Library/LaunchAgents/com.horos.backup.plist
```

5) **Run immediately (optional)**
```bash
launchctl start com.horos.backup
```

---

## 🛠️ Operations and monitoring

**Rotated logs (100 MB × 10):**
```bash
tail -f "/Volumes/PACS/Backup/logs/horos_backup.log"
```

**launchd logs:**
```bash
tail -f /tmp/horos_backup_export.out /tmp/horos_backup_export.err
```

**Issues (events such as `NO_FILES`, `ZIP_FAIL`, `INCOMING_OVER_LIMIT`):**  
`/Volumes/PACS/Backup/issues.csv`

---

## 🔧 Useful parameters (in the script)

- **Modalities**: `MODS = ("CT", "MR")`  
- **Batch size**: `BATCH_SIZE = 15`  
- **Delay between studies**: `SLEEP_BETWEEN_STUDIES = 1` (seconds)  
- **Ordering**: `ORDER_BY = "study_date"` (or `"date_added"`)  
- **INCOMING threshold**: `INCOMING_MAX_FILES = 25_000`  
- **Maximum filename length**: `MAX_NAME_NOEXT = 128`  
- **Logs**: `LOG_MAX_BYTES = 100 * 1024 * 1024`, `LOG_BACKUP_COUNT = 10`

> **Note**: if you switch `ORDER_BY` to `"date_added"`, sorting changes to `ZSTUDY.ZDATEADDED ASC, ZSTUDY.ZSTUDYINSTANCEUID ASC`.

---

## 🧪 Quick test (single cycle only)

> Useful to validate the setup without waiting for the scheduler.

```bash
/usr/bin/python3 "/Volumes/PACS/Backup/horos_backup_export.py"
```

If you want a smaller test batch, open the script and temporarily set `BATCH_SIZE = 3`.

---

## ❓ Troubleshooting

**Stopped with “sentinel missing”**  
Create `/Volumes/PACS/.pacs_sentinel` on the correct external volume.

**Nothing exported; log shows `INCOMING_OVER_LIMIT`**  
Horos is likely reimporting; wait until `INCOMING.noindex` drops below 25k files.

**`NO_FILES` in `issues.csv`**  
The study is orphaned (paths were not found). Check data integrity and Horos storage paths.

**`ZIP_FAIL` in `issues.csv`**  
The export still failed after 3 attempts and `testzip()`. Check disk I/O and permissions.

**I want to switch to `date_added`**  
Edit `ORDER_BY = "date_added"` and save the file.

---

## 📝 Security and privacy notes
- Filenames include the **patient name** and **dates**. Review internal policies before sharing ZIP files outside a controlled environment.
- Encryption at rest is not enabled by default (the focus here is performance). If needed, consider encrypting the APFS volume.

---

## ✅ Conclusion

This automation addresses the need for **reliable backups** of large DICOM repositories in Horos,  
with **low operator effort**, **failure resilience**, and **respect for the PACS import environment**.
