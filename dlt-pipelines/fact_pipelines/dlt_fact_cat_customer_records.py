"""DEPRECATED. CAIS is file-based, not event-based.

This pipeline previously assumed three CAIS event codes (CAIS_C, CAIS_A, CAIS_R)
that do not exist in any FINRA specification. CAIS reporting uses paired
CAIS Data File + Transformed Identifiers File submissions containing FDID
Records and Customer Records.

The replacement model lives in:
  - ddl/cais/01_cais_silver_delta.sql
  - ddl/cais/02_cais_gold_delta.sql
  - reference-data/dlt_ref_cais_taxonomy.py (when implemented)

The new Gold tables are:
  - fact_cais_fdid           (snapshot-grain row per FDID)
  - fact_cais_customer       (snapshot-grain row per Customer)
  - fact_cais_submission     (per-file submission registry)
  - fact_cais_inconsistency  (material inconsistency tracking)

Until the new CAIS DLT pipelines are implemented, this file is retained as a
deprecation marker. Do not run.
"""
import dlt

# Deprecation guard: refuse to run.
raise RuntimeError(
    "dlt_fact_cat_customer_records.py is deprecated. CAIS uses paired-file "
    "submissions, not events. See ddl/cais/ for the replacement model."
)
