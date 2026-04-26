# Repository file manifest

File inventory for the expanded CAT data model. Organized by directory.

## Root

- `README.md` - overview
- `LICENSE` - Apache License, Version 2.0 (full text)
- `NOTICE` - third-party attribution notices
- `THIRD_PARTY_LICENSES.md` - external-source attribution
- `MANIFEST.md` - this file
- `.gitignore`

## ddl/

Physical DDL, one file per dialect:

- `CAT_PreTrade_DDL_DeltaLake.sql` - Databricks / Delta Lake
- `CAT_PreTrade_DDL_Hive.sql` - Apache Hive
- `CAT_PreTrade_DDL_Fabric_Lakehouse.sql` - Microsoft Fabric Lakehouse
- `CAT_PreTrade_DDL_Fabric_Warehouse.sql` - Microsoft Fabric Warehouse (T-SQL)
- `fabric-migration-notes.md` - migration notes for Fabric
- `_delta_to_fabric.py` - Delta -> Fabric conversion helper

Split-by-subject-area DDL (easier to review) lives under:

- `ddl/expanded-model/` - Delta versions, one file per subject area
- `ddl/expanded-model-hive/` - Hive mirror

### ddl/dv2/ - Data Vault 2.0 Silver

- `dv2_hubs.sql` - 20 hubs
- `dv2_links.sql` - 32 links
- `dv2_satellites.sql` - 52 satellites
- `dv2_reference.sql` - 19 reference tables
- `dv2_pit_tables.sql` - 16 point-in-time tables
- `dv2_bridge_tables.sql` - 8 bridge tables
- `dv2_business_vault.sql` - 11 business-vault views
- `dv2_mapping.md` - transactional -> DV2 mapping
- `dv2_ddl_fabric_lakehouse.sql` - Fabric Lakehouse variant
- `dv2_ddl_fabric_warehouse.sql` - Fabric Warehouse (T-SQL) variant

### ddl/gold/ - star schema

- `01_dim_tables.sql` - 8 SCD2 dimensions
- `02_fact_tables.sql` - 4 CAT-aligned facts
- `03_operational_tables.sql` - submission tracking + feedback
- `04_consumption_views.sql` - BI and regulator views
- `05_etl_silver_to_gold.md` - DV2 -> Gold transformation logic
- `06_cat_field_mapping.csv` - Gold column to CAT JSON field mapping
- `07_submission_generator.py` - JSON + BZip2 + SFTP submission generator
- `08_fabric_warehouse_variant.sql` - T-SQL Fabric Warehouse variant

## dlt-pipelines/ - Delta Live Tables

- `README.md`
- `config/pipeline_config.json`
- `dim_pipelines/` - 8 dimension pipelines
- `fact_pipelines/` - 4 fact pipelines
- `tests/` - SCD2 invariants, fact quality gates, referential-integrity checks

## ref-data-pipelines/ - reference-data ingestion

- `README.md`
- `configs/pipeline_config.json`
- `configs/source_registry.json`
- `configs/validation_rules.json`
- `ingestion/dlt_ref_cat_taxonomy.py` - FINRA CAT 50-event taxonomy
- `ingestion/dlt_ref_iso_codes.py` - ISO 3166-1 / 4217 / 10383
- `ingestion/dlt_ref_isda_conventions.py` - ISDA day-count conventions
- `ingestion/dlt_ref_fix_enums.py` - FIX tag 40/59/21 enums
- `ingestion/dlt_ref_firm_taxonomy.py` - firm desk and risk-framework taxonomies
- `ingestion/dlt_ref_industry.py` - industry/tax/credit reference data
- `manual-procedures/01_monthly_iso_mic_refresh.md`
- `manual-procedures/02_cat_taxonomy_version_upgrade.md`
- `manual-procedures/03_credit_rating_quarterly_refresh.md`
- `manual-procedures/04_tax_treaty_annual_refresh.md`

## diagrams/

- `README.md`
- `mermaid/` - ER, sequence, state, class, and flowchart diagrams
- `drawio/` - subject-area ER diagrams plus a generator script

## docs/

- `CAT_PreTrade_Canonical_Model.pptx`
- `CAT_PreTrade_Conceptual_Model.pptx`
- `CAT_PreTrade_Logical_Model.pptx`
- `CAT_PreTrade_Data_Model.docx`
- `entity_catalog_expanded.md` - full entity catalog
- `relationship_matrix.csv` - entity-to-entity relationship matrix
- `state_machines.md` - lifecycle state machines
- `source_inventory.md` - source-file inventory
- `fix_tag_mappings.csv` - FIX tag -> column mapping

## reference/

- `CAT_PreTrade_Business_Dictionary.xlsx`
- `CAT_PreTrade_Schema_Summary.txt`

## audit/

- `entity_reconciliation.csv` - per-entity reconciliation between docs and DDL
- `attribute_reconciliation.csv` - per-attribute reconciliation
- `naming_violations.csv` - naming-convention check output
