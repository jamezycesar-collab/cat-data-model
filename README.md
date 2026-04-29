# CAT Data Model

A data model for the U.S. broker-dealer trade lifecycle, aligned to SEC Rule 613 / FINRA CAT reporting (both Industry Member Tech Specs and CAIS Tech Specs). DDL ships in four dialects: Azure Databricks (Delta), Apache Hive, and Microsoft Fabric (Lakehouse + Warehouse, T-SQL).

The model carries about 93 entities across thirteen subject areas covering Party, Instrument, Venue, Agreement, Pre-Trade, Order Request, Order Stage, Execution, Allocation, Post-Trade, Position, Operations/Regulatory, Multi-Leg Option Order, and CAIS. Reference tables enforce most enumerations via CHECK and FOREIGN KEY constraints. See `docs/entity_catalog_expanded.md` for the full list.

## Spec coverage

| Spec | Version | Effective |
|------|---------|-----------|
| FINRA CAT IM Technical Specifications | 4.1.0r15 | 2026-03-06 |
| FINRA CAT CAIS Technical Specifications | 2.2.0r4 | 2025-08-14 |

The model covers all 99 reportable event types defined in CAT IM v4.1.0r15 (39 equity + 35 simple option + 25 multi-leg) plus the FDID, Customer, Address, and Large Trader records defined in CAIS v2.2.0r4. Reference data is extracted from the spec PDFs and verified by `guardrails/validate_event_taxonomy.py` on every commit.

## Layout

- `primary-sources/` - pinned spec PDFs and verified extracts (the source of truth)
- `ddl/` - physical DDL, one file per dialect, plus split-by-subject-area copies
- `ddl/dv2/` - Data Vault 2.0 Silver
- `ddl/gold/` - star schema
- `ddl/multileg/` - multi-leg option order Silver and Gold (Section 5.2 of CAT IM)
- `ddl/cais/` - CAIS Silver and Gold
- `dlt-pipelines/` - Delta Live Tables notebooks for dims, facts, and quality gates
- `ref-data-pipelines/` - ingestion for ISO, FIX, ISDA, and CAT/CAIS taxonomy refs
- `diagrams/` - Mermaid and draw.io diagrams
- `docs/` - entity catalog, relationship matrix, state machines, root-cause audit
- `guardrails/` - validator, pre-commit hook, GitHub Actions config, verification protocol
- `audit/` - reconciliation CSVs

## Getting started

`diagrams/mermaid/full_model_er.mmd` is the entry point. For Databricks / Delta, open `ddl/CAT_PreTrade_DDL_DeltaLake.sql` plus the multi-leg and CAIS additions in `ddl/multileg/01_multileg_silver_delta.sql`, `ddl/multileg/02_multileg_gold_delta.sql`, `ddl/cais/01_cais_silver_delta.sql`, `ddl/cais/02_cais_gold_delta.sql`.

`primary-sources/SOURCES.md` documents the spec PDFs and how to update them. `guardrails/VERIFICATION_PROTOCOL.md` is mandatory reading before adding or modifying reference data.

## Running the validator

```bash
pip install pypdf
python3 guardrails/validate_event_taxonomy.py
```

Expected output: `PASS - reference data matches primary sources.`

## License

Apache License, Version 2.0. See `LICENSE`, `NOTICE`, and `THIRD_PARTY_LICENSES.md`.

## Regulatory context

The model supports:

- SEC Rule 613 (17 CFR 242.613), Consolidated Audit Trail
- FINRA Rule 6800 series, CAT reporting
- FINRA CAT IM Technical Specifications v4.1.0r15
- FINRA CAT CAIS Technical Specifications v2.2.0r4
- BCBS 239 risk-data aggregation
- MiFID II / MiFIR and Dodd-Frank Title VII cross-border references
- ISDA Master / CSA, GMRA, and GMSLA agreement references

It isn't a substitute for regulatory advice. Implementers are responsible for validating conformance against the current specification.
