# CAT Data Model

A data model for the U.S. broker-dealer trade lifecycle, aligned to SEC Rule 613 / FINRA CAT reporting. DDL ships in four dialects: Azure Databricks (Delta), Apache Hive, and Microsoft Fabric (Lakehouse + Warehouse, T-SQL).

The model grew out of an earlier 29-entity pre-trade compliance schema. It now carries about 93 entities across twelve subject areas covering Party, Instrument, Venue, Agreement, Pre-Trade, Order Request, Order Stage, Execution, Allocation, Post-Trade, Position, and Operations/Regulatory. Reference tables enforce most enumerations via CHECK and FOREIGN KEY constraints. See `docs/entity_catalog_expanded.md` for the full list.

## Layout

- `ddl/` - physical DDL, one file per dialect, plus split-by-subject-area copies
- `ddl/dv2/` - Data Vault 2.0 Silver (hubs, links, satellites, PITs, bridges, BV)
- `ddl/gold/` - star schema (8 SCD2 dims, 4 CAT-aligned facts, submission generator)
- `dlt-pipelines/` - Delta Live Tables notebooks for dims, facts, and quality gates
- `ref-data-pipelines/` - ingestion for ISO, FIX, ISDA, and CAT taxonomy refs, plus runbooks
- `diagrams/` - Mermaid and draw.io ER, sequence, state, and flowchart diagrams
- `docs/` - entity catalog, relationship matrix, state machines, dictionary
- `reference/` - schema summary and business dictionary
- `audit/` - reconciliation CSVs between DDL and docs

## Getting started

Open `diagrams/mermaid/full_model_er.mmd` for the 93-entity overview. Subject-area diagrams (`party_model_er.mmd`, `trade_lifecycle_er.mmd`, and friends) zoom in.

For Databricks / Delta, open `ddl/CAT_PreTrade_DDL_DeltaLake.sql`. For Fabric, use the `Fabric_Lakehouse` or `Fabric_Warehouse` variant. The split-file versions under `ddl/expanded-model/` are easier for PR review.

`ddl/dv2/dv2_mapping.md` walks the transactional to DV2 mapping table by table. `ddl/gold/05_etl_silver_to_gold.md` covers DV2 to Gold. `ddl/gold/07_submission_generator.py` produces the FINRA CAT JSON submission with BZip2 compression.

`dlt-pipelines/` has the DLT workflows. `ref-data-pipelines/manual-procedures/` has monthly, quarterly, annual, and ad-hoc runbooks for reference-data maintenance.

## License

Apache License, Version 2.0. See `LICENSE`, `NOTICE`, and `THIRD_PARTY_LICENSES.md`.

## Regulatory context

The model supports:

- SEC Rule 613 (17 CFR 242.613), Consolidated Audit Trail
- FINRA Rule 6800 series, CAT reporting
- FINRA CAT Industry Member Technical Specifications v4.1.0r9
- BCBS 239 risk-data aggregation
- MiFID II / MiFIR and Dodd-Frank Title VII cross-border references
- ISDA Master / CSA, GMRA, and GMSLA agreement references

It isn't a substitute for regulatory advice. Implementers are responsible for validating conformance against the current specification.

## Contributing

Open an issue or PR.
