# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 cat-data-model contributors
"""DLT pipeline: fact_cais_customer.

Snapshot-grain fact, one row per Customer per as-of date. Source:
CAIS Tech Specs v2.2.0r4 Section 5.1.3.2.

PII protection: this fact does NOT carry the personally-identifying
fields. The TID (transformed identifier) values stay in the paired
Transformed Identifiers File and are joined via cais_customer_hk only
when authorised.
"""
import dlt
from pyspark.sql import functions as F


SILVER_DB = spark.conf.get("silver_database", "silver")


@dlt.view(name="v_cais_customer_curated")
def v_cais_customer_curated():
    hub = spark.table(f"{SILVER_DB}.hub_cais_customer")
    sat = (
        spark.table(f"{SILVER_DB}.sat_cais_customer_state")
        .filter("load_end_dts IS NULL")
    )
    fdid_cust = (
        spark.table(f"{SILVER_DB}.link_cais_fdid_customer")
        .filter("role_end_date IS NULL")
    )

    fdid_count = (
        fdid_cust.groupBy("cais_customer_hk")
        .agg(F.count("*").alias("fdid_count"))
    )

    return (
        hub.join(sat, "cais_customer_hk")
        .join(fdid_count, "cais_customer_hk", "left")
        .select(
            F.current_date().alias("as_of_date"),
            F.col("cais_customer_bk_recordID").alias("customer_record_id"),
            F.col("cais_customer_bk_catReporterCRD").alias("cat_reporter_crd"),
            F.col("customer_type"),
            F.coalesce(F.col("fdid_count"), F.lit(0)).alias("fdid_count"),
            F.lit(True).alias("has_pii_in_tid"),
            F.col("record_source").alias("source_file"),
            F.col("dv2_source_hk"),
        )
    )


@dlt.expect_all_or_fail({
    "non_null_customer_record_id": "customer_record_id IS NOT NULL",
    "non_null_cat_reporter_crd":   "cat_reporter_crd IS NOT NULL AND cat_reporter_crd > 0",
    "valid_customer_type":         "customer_type IN ('NATURAL_PERSON', 'LEGAL_ENTITY')",
    "must_have_fdid":              "fdid_count >= 1",
})
@dlt.table(
    name="fact_cais_customer",
    comment="CAIS Customer snapshot fact. PII held in paired Transformed Identifiers File.",
    partition_cols=["as_of_date"],
    table_properties={
        "quality": "gold",
        "subject_area": "gold_fact_cais",
        "spec_version": "CAIS_v2.2.0r4",
        "pii_classification": "NON_PII",
    },
)
def fact_cais_customer():
    return dlt.read("v_cais_customer_curated")
