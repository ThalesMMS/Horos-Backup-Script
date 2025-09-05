# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based automated backup system for Horos DICOM viewer on macOS. It exports CT/MR medical imaging studies from a Horos database to organized ZIP files, designed for high-volume scenarios (50,000+ studies, 2+ TB).

## Key Commands

### Testing
```bash
# Test single execution (processes 15 studies)
/usr/bin/python3 "/Volumes/PACS/Backup/horos_backup_export.py"

# View logs
tail -f "/Volumes/PACS/Backup/logs/horos_backup.log"

# Check launchd logs
tail -f /tmp/horos_backup_export.out /tmp/horos_backup_export.err
```

### LaunchAgent Management
```bash
# Load scheduled task
launchctl load ~/Library/LaunchAgents/com.horos.backup.plist

# Unload scheduled task
launchctl unload ~/Library/LaunchAgents/com.horos.backup.plist

# Run immediately
launchctl start com.horos.backup
```

### Database Inspection
```bash
# Check study count
sqlite3 "/Volumes/PACS/Backup/.tmp/dbcopy/Database_copy.sql" \
  "SELECT COUNT(DISTINCT s.ZSTUDY) FROM ZSERIES s WHERE TRIM(UPPER(COALESCE(s.ZMODALITY,''))) IN ('CT','MR');"

# Check export state
sqlite3 "/Volumes/PACS/Backup/export_state.sqlite" \
  "SELECT COUNT(*) FROM exported_studies;"
```

## Architecture

### Core Components

1. **horos_backup_export.py** - Main script with single-execution logic
   - Uses SQLite to query Horos database (Core Data format)
   - Implements atomic ZIP creation with `.part` → rename pattern
   - Maintains export state in separate SQLite database
   - File locking prevents concurrent executions
   - Automatic month folder management with `.month_done` markers

2. **com.horos.backup.plist** - LaunchAgent for scheduled execution (every 10 minutes)

### Database Schema (Horos)

Key tables in the Horos database:
- `ZSTUDY` - Study-level metadata (patient, dates, UIDs)
- `ZSERIES` - Series within studies (modality info)
- `ZIMAGE` - Individual DICOM files (paths)

File path resolution:
- Files stored as: `DATABASE.noindex/{ZSTOREDIN}/{first 4 chars of filename}/{filename}`
- Alternative: Direct path in `ZPATHSTRING` field

### Safety Mechanisms

- **Volume sentinel**: Requires `/Volumes/PACS/.pacs_sentinel` file to prevent writing to wrong disk
- **INCOMING check**: Skips execution if >25,000 files in `INCOMING.noindex` (Horos importing)
- **Lock file**: Prevents overlapping executions via `fcntl` file locking
- **Atomic operations**: ZIP files written as `.part` then renamed
- **Month resumption**: Removes incomplete month folders on restart

### File Organization

```
/Volumes/PACS/
├── Database/Horos Data/        # Horos database location
│   ├── Database.sql            # Core Data SQLite file
│   ├── DATABASE.noindex/       # DICOM file storage
│   └── INCOMING.noindex/       # Import queue
└── Backup/
    ├── horos_backup_export.py  # Main script
    ├── export_state.sqlite     # Tracking database
    ├── issues.csv              # Error log
    ├── logs/                   # Rotating logs (100MB x 10)
    ├── YYYY_MM/                # Monthly folders with ZIPs
    └── .tmp/                   # Temporary files and locks
```

## Important Configuration

Key parameters in `horos_backup_export.py`:

- `BATCH_SIZE = 15` - Studies per execution
- `INCOMING_MAX_FILES = 25_000` - Skip threshold for import queue
- `ORDER_BY = "study_date"` - Can be "study_date" or "date_added"
- `MODS = ("CT", "MR")` - Target modalities
- `USE_DB_COPY = False` - Set True for consistent DB snapshot (slower)
- `MAX_NAME_NOEXT = 128` - ZIP filename length limit

## Critical Notes

- Script requires Python 3.8+ at `/usr/bin/python3` (standard macOS)
- No external Python dependencies (uses only standard library)
- Designed for Mac mini 2014 with limited resources - intentionally gentle I/O pattern
- ZIP integrity verified with `testzip()` after creation (3 retry attempts)
- Study selection uses complex SQL to find CT/MR studies across multiple fields
- Apple Core Data timestamps use epoch of 2001-01-01 (not Unix epoch)