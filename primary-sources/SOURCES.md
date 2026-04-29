# Primary Sources

This directory holds extracts from the authoritative FINRA CAT specifications. Every value in `cat_im_event_types.csv` and `cais_enumerations.csv` is sourced from these PDFs and pinned by SHA-256 hash.

## Spec PDFs

| Spec | Version | Effective Date | SHA-256 |
|------|---------|----------------|---------|
| CAT Reporting Technical Specifications for Industry Members | 4.1.0r15 | March 6, 2026 | `f2147aea55f048aa9c85b3d53633eaa573d9889b50e7f08decb2d242b035a625` |
| Full CAIS Technical Specifications | 2.2.0r4 | August 14, 2025 | `42e3e007d369960e1bbadaf62e1bdb56ec2bda17994f9bf271696824a880c17b` |

The PDFs themselves are intentionally NOT committed to git (they are FINRA-distributed copyrighted material). Implementers must obtain them from https://www.catnmsplan.com/specifications/im, place them in this directory, and verify the SHA-256 matches before running `guardrails/validate_event_taxonomy.py`.

## Files

| File | Source | Contents |
|------|--------|----------|
| `cat_im_event_types.csv` | CAT spec Tables 15, 60, 61 | 99 reportable event types with section, name, message-type code, category, phase, description |
| `cais_enumerations.csv` | CAIS spec Sections 4.1, 5.1.3 | Reference enumerations for fdidType, accountType, fdidEndReason, addrType, customerType, customerRole, fileType, submissionAction |

## Provenance

Each row in `cat_im_event_types.csv` corresponds to a section number from the CAT spec TOC. The 99 rows break down as:

- 39 equity events (Section 4)
- 35 simple option events (Section 5.1)
- 25 multi-leg option events (Section 5.2)

`cais_enumerations.csv` covers the structural enumerations that drive the CAIS Data File payload. CAIS does not have "events" in the CAT-event-taxonomy sense; the equivalent operational dimensions are file submissions and record-state transitions, captured here via the `fileType` and `submissionAction` columns. The `submissionAction` values are derived from the operational sections of the spec (3.1 FDID replacement, 3.2 TID replacement, 3.3 Mass Transfer, 3.7 Full Data Replacement, 3.8 Periodic Refresh, 6.6.2 Repair, 6.6.4 Proactive Updates).

## How to update

When FINRA publishes a new spec version:

1. Download the new CLEAN PDF.
2. Replace the old PDF in this directory.
3. Update the SHA-256 in this README and in `guardrails/spec_pins.json`.
4. Walk every section that lists event types or enumerations; update the CSVs to match.
5. Run `guardrails/validate_event_taxonomy.py` and resolve any drift.
6. Update the version reference in `README.md`, the wiki, and `dlt_ref_cat_taxonomy.py`.
7. Record the change in `CHANGELOG.md` with the spec version, date, and a summary of added/removed/renamed codes.
