# Support

## How to get help

- **Bug report:** open a bug issue with clear reproduction steps.
- **Usage / setup help:** open a support issue and include your macOS version, Horos context, and sanitized log excerpts.
- **Security concern:** follow [SECURITY.md](SECURITY.md) and avoid posting sensitive details publicly.

## Before opening an issue

- Read [README.md](README.md), especially the installation, operations, and troubleshooting sections.
- Remove any patient-identifying data from logs, paths, screenshots, and example filenames.
- State whether you are running against a real Horos database or a sanitized test environment.

## Short FAQ

### Does this work outside macOS?

Not as-is. The automation is built around Horos on macOS and `launchd`.

### Does it encrypt the exported ZIP files?

No. Encryption at rest is not part of the current automation.

### Can I use modalities other than CT/MR?

Yes, but that is a local configuration change. Review `MODS` in the script and validate the workflow carefully before using it in production.

### Can I post real patient examples in issues?

No. Use sanitized examples only.

### Is this a full PACS migration tool?

No. It is a focused backup/export automation for Horos-managed studies.
