# Changelog

All notable changes to the data model are documented here. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased] - Tier 5: Multi-leg Hive Gold + order-event field reconciliation

### Added

- `ddl/multileg/06_multileg_gold_hive.sql` - Hive variant of `fact_multileg_option_events`, `fact_multileg_option_legs`, `dim_multileg_strategy`, and `vw_multileg_option_lifecycle`. Multi-leg now spans all four dialects (Delta + Hive + Fabric Lakehouse + Fabric Warehouse).
- 55 newly verified order-event field mappings sourced from CAT IM spec Sections 4.1, 4.3, 4.11.1, and 4.13. Each row cites the specific table-row in the spec where the field is defined.

### Changed

- `ddl/gold/06_cat_field_mapping.csv` grew from 27 verified rows to **82 verified rows**, covering MENO (47 fields), MEOR (28 fields), MEOT (35 fields), MEPA (26 fields), MEAA (28 fields), and the cross-event header fields (`actionType`, `errorROEID`, `firmROEID`, `type`, `CATReporterIMID`, `eventTimestamp`, `manualFlag`, `electronicDupFlag`, `electronicTimestamp`).
- `ddl/gold/06b_cat_field_mapping_unverified_candidates.csv` shrank from 35 rows to 3 rows. The 32 promoted entries are now in the verified file; the 3 remaining are quote-event mappings whose submission-file-type assignment needs review in a separate quote-event pass.

### Why

Tier 4 split the fabrication-laden field mapping into verified and unverified buckets. Tier 5 closes the gap by walking the spec field-spec tables (Tables 16, 19, 51, 52) and promoting every real field to the verified file with a citation to its spec section.



### Changed

- `ddl/gold/06_cat_field_mapping.csv` reduced to 27 verified field mappings (from 82). Each row now carries a `verification_status` column and a description that cites the spec section the field is defined in.
- The 35 mappings whose CAT JSON field names do not appear in the spec PDFs (`cumQty`, `executionPrice`, `solicitedFlag`, `representativeID`, `accountID`, `quoteSide`, `quoteStatus`, `allocationMethod`, etc.) moved to `ddl/gold/06b_cat_field_mapping_unverified_candidates.csv` with explanatory notes.
- Malformed `gold_table` cells like `"dim_party (via party_sk join)"` rewritten as proper table names.

### Added

- `validate_field_specifications.py` now searches both CAT IM and CAIS spec PDFs (was CAT IM only), so CAIS field names like `firmDesignatedID` and `customerRecordID` resolve correctly.
- `06_cat_field_mapping.csv` gained a `verification_status` column.

### Why

Tier 3.3 (the field-spec validator) surfaced that 35 of 62 entries (~57%) referenced field names not present in any FINRA spec. Same fabrication pattern as the v2.0.0 event-code list, just one layer deeper. Splitting the file into verified vs unverified-candidates makes the boundary explicit: implementers consume the verified file; the candidates file is a backlog awaiting spec reconciliation against per-event Field Specification tables.

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
