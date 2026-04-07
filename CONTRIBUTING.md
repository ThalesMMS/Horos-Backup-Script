# Contributing

Thanks for helping improve Horos-Backup-Script.

This repository is intentionally small and operationally focused. Please keep contributions practical, easy to review, and aligned with the project goal: reliable bulk backup of Horos CT/MR studies on macOS.

## Before you open an issue

- Read the main [README](README.md) and [SUPPORT.md](SUPPORT.md) first.
- Search existing issues and pull requests for duplicates.
- Never post patient-identifying data, real study UIDs, database dumps, or screenshots that expose PHI.

## Good first contribution types

- Documentation fixes and clarifications
- Safer defaults or validation improvements
- Tests for existing behavior
- Logging or error-message improvements
- Small bug fixes that do not change the project scope

Please avoid unrelated feature work in drive-by PRs.

## Development notes

Requirements are intentionally light:

- macOS-oriented operational target
- Python 3.8+
- no external runtime dependencies

Run tests locally:

```bash
pytest
```

## Pull request checklist

When opening a PR, please:

1. Keep the change set focused.
2. Explain the operational problem being solved.
3. Mention how you tested it.
4. Call out any behavior changes that affect existing backups.
5. Confirm that no patient data was added to the repository, tests, screenshots, or logs.

## Scope and safety expectations

- This project is backup automation, not a general PACS migration tool.
- Backward-compatible operational fixes are preferred over broad rewrites.
- If your change affects filenames, ordering, locking, or retry logic, describe the expected impact clearly.
- For security-sensitive findings, use the process in [SECURITY.md](SECURITY.md).

## Communication

- Use issues for reproducible bugs and concrete improvement proposals.
- Use pull requests for proposed code or documentation changes.
- Use [SUPPORT.md](SUPPORT.md) for the recommended help path and FAQ.
