# Changelog

All notable changes to the data model are documented here. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased] - Remediation against CAT IM v4.1.0r15 and CAIS v2.2.0r4

### Added

- `primary-sources/` directory holding pinned spec PDFs and verified extracts:
  - `cat_im_event_types.csv` - 99 CAT reportable event types extracted from CAT IM v4.1.0r15 Tables 15, 60, 61
  - `cais_enumerations.csv` - CAIS structural enumerations extracted from CAIS v2.2.0r4 Sections 4.1 and 5.1.3
  - `SOURCES.md` - provenance and how-to-update guide
- `guardrails/` directory:
  - `validate_event_taxonomy.py` - automated drift-detection between CSV and pinned PDF
  - `spec_pins.json` - SHA-256 pins for each spec version
  - `VERIFICATION_PROTOCOL.md` - mandatory protocol for adding or modifying reference data
  - `pre-commit` - git hook that runs the validator
  - `github-actions-validator.yml` - CI workflow
- `ddl/multileg/` - 25 multi-leg option events (CAT IM Section 5.2) modelled across all four dialects:
  - `01_multileg_silver_delta.sql` - Silver hubs / links / satellites
  - `02_multileg_gold_delta.sql` - Gold facts (`fact_multileg_option_events`, `fact_multileg_option_legs`) and `dim_multileg_strategy`
  - `03_multileg_silver_hive.sql` - Hive mirror
  - `04_multileg_fabric_warehouse.sql` - Fabric Warehouse (T-SQL) mirror
- `ddl/cais/` - CAIS Silver and Gold:
  - `01_cais_silver_delta.sql` - DV2 hubs (FDID, Customer, Large Trader), links, satellites with proper SCD2 of FDID/Customer state and addresses
  - `02_cais_gold_delta.sql` - `fact_cais_fdid`, `fact_cais_customer`, `fact_cais_submission`, `fact_cais_inconsistency`, `dim_cais_account_type`, `dim_cais_fdid_type`
- `dlt-pipelines/dlt_fact_multileg_option_events.py` - DLT pipeline with `expect_all_or_fail` quality gates including code-set validation against the verified 25 multi-leg codes
- `reference-data/dlt_ref_cat_taxonomy.py` - rewritten to load from CSV with row-count and version-pin enforcement
- `reference-data/dlt_ref_cais_taxonomy.py` - new CAIS reference loader
- `docs/ROOT_CAUSE_AUDIT.md` - forensic analysis of how 24 fabricated event codes shipped in v2.0.0

### Removed

- 24 fabricated event codes from `ref_cat_event_type` (`CAIS_A`, `CAIS_C`, `CAIS_R`, `EOT`, `MEAU`, `MEAX`, `MEIN`, `MEMA`, `MEMA_CORR`, `MEOR_EXT`, `MEOTQ`, `MEOX`, `MEPM`, `MEPZ`, `MERO`, `MEVE`, `MLOT`, `MOCX`, `MOOTS`, `MOOX`, `MOQS`, `MORA`, `MORE`, `MORR`). None of these codes appear in any FINRA CAT specification.
- `audit/validate_report.md` and `audit/validate_entity_matrix.csv` - self-signed audit artefacts that referenced the fabricated codes.
- "All 50 CAT events covered" claim throughout README, wiki, and reference data. The actual count is 99.

### Fixed

- Spec version reference bumped from `v4.1.0r9` to `v4.1.0r15`.
- `MEQS` description corrected. Real spec definition: "Quote Status" (Section 4.10.8). Was previously labeled "Quote Sent in response to RFQ".
- CAIS events removed from the order-event reference table; CAIS now lives in its own taxonomy and DDL because it is file-based, not event-based.

### Coverage delta

| Metric | Before | After |
|--------|-------:|------:|
| CAT codes in reference table | 49 | 99 |
| Fabricated codes | 24 | 0 |
| Real CAT codes covered | 25 / 99 (25%) | 99 / 99 (100%) |
| Multi-leg event coverage | 0 / 25 | 25 / 25 |
| CAIS coverage | (incorrectly typed as 3 events) | full record-and-submission model |
| Validator running in CI | No | Yes |
