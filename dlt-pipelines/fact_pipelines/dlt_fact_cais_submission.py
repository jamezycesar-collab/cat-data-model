# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 cat-data-model contributors
"""DLT pipeline: fact_cais_submission.

Per-file submission registry covering both CAIS Data Files and Transformed
Identifiers Files. Source: CAIS Tech Specs v2.2.0r4 Section 5 (Submission
Process) and Section 6 (Feedback and Corrections).

Each row represents one submission attempt - whether accepted or rejected.
The accepted/error/rejection/inconsistency counts come from the FINRA CAT
feedback files matched on submission_filename.
"""
import dlt
from pyspark.sql import functions as F


SILVER_DB = spark.conf.get("silver_database", "silver")


@dlt.view(name="v_cais_submission_curated")
def v_cais_submission_curated():
    sub = spark.table(f"{SILVER_DB}.link_cais_submission")
    return sub.select(
        F.col("submission_filename"),
        F.col("load_dts").alias("submission_dts"),
        F.col("cat_submitter_id"),
        F.col("cat_reporter_crd"),
        F.col("file_type"),
        F.col("submission_action"),
        F.col("paired_filename"),
        # The accepted / error / rejection / inconsistency counts are
        # populated by a downstream feedback-ingestion job that joins
        # the FINRA-returned Feedback File on submission_filename.
        # Default to nulls until that job runs.
        F.lit(None).cast("int").alias("fdid_record_count"),
        F.lit(None).cast("int").alias("natural_person_record_count"),
        F.lit(None).cast("int").alias("legal_entity_record_count"),
        F.lit(False).alias("accepted"),
        F.lit(0).alias("error_count"),
        F.lit(0).alias("rejection_count"),
        F.lit(0).alias("inconsistency_count"),
        F.col("record_source").alias("source_file"),
        F.col("dv2_source_hk"),
    )


@dlt.expect_all_or_fail({
    "non_null_filename":     "submission_filename IS NOT NULL",
    "non_null_submitter_id": "cat_submitter_id IS NOT NULL",
    "non_null_reporter_crd": "cat_reporter_crd IS NOT NULL AND cat_reporter_crd > 0",
    "valid_file_type":
        "file_type IN ('CAIS_DATA_FILE', 'TRANSFORMED_IDENTIFIERS_FILE')",
    "valid_submission_action":
        "submission_action IN ('NEW', 'UPDATE', 'REFRESH', 'FIRM_CORRECTION', "
        "'REPAIR', 'REPLACEMENT', 'MASS_TRANSFER')",
})
@dlt.table(
    name="fact_cais_submission",
    comment="Per-submission registry across CAIS Data and Transformed Identifiers Files.",
    table_properties={
        "quality": "gold",
        "subject_area": "gold_fact_cais",
        "spec_version": "CAIS_v2.2.0r4",
    },
)
def fact_cais_submission():
    return dlt.read("v_cais_submission_curated")
