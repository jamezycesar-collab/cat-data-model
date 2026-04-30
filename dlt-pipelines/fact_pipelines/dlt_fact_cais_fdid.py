# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 cat-data-model contributors
"""DLT pipeline: fact_cais_fdid.

Snapshot-grain fact, one row per FDID per as-of date. Reads from the
Silver CAIS hubs and satellites added in the v4.1.0r15 remediation
(ddl/cais/01_cais_silver_delta.sql) and writes to the Gold table
(ddl/cais/02_cais_gold_delta.sql).

Source: FINRA CAT CAIS Tech Specs v2.2.0r4 Section 4.1.
"""
import dlt
from pyspark.sql import functions as F


SILVER_DB = spark.conf.get("silver_database", "silver")
GOLD_DB = spark.conf.get("gold_database", "gold")


@dlt.view(
    name="v_cais_fdid_curated",
    comment="Joins hub_cais_fdid + sat_cais_fdid_state (current open) for snapshot generation.",
)
def v_cais_fdid_curated():
    hub = spark.table(f"{SILVER_DB}.hub_cais_fdid")
    sat = (
        spark.table(f"{SILVER_DB}.sat_cais_fdid_state")
        .filter("load_end_dts IS NULL")
    )
    fdid_cust = spark.table(f"{SILVER_DB}.link_cais_fdid_customer")
    fdid_lt = spark.table(f"{SILVER_DB}.link_cais_fdid_large_trader")

    customer_count = (
        fdid_cust.filter("role_end_date IS NULL")
        .groupBy("cais_fdid_hk")
        .agg(F.count("*").alias("customer_count"))
    )
    has_trd = (
        fdid_cust.filter("role_end_date IS NULL AND customer_role = 'TRDHOLDER'")
        .select("cais_fdid_hk")
        .distinct()
        .withColumn("has_active_trdholder", F.lit(True))
    )
    has_lt = (
        fdid_lt.filter("ltid_end_date IS NULL")
        .select("cais_fdid_hk")
        .distinct()
        .withColumn("has_active_ltid", F.lit(True))
    )

    return (
        hub.join(sat, "cais_fdid_hk")
        .join(customer_count, "cais_fdid_hk", "left")
        .join(has_trd, "cais_fdid_hk", "left")
        .join(has_lt, "cais_fdid_hk", "left")
        .select(
            F.current_date().alias("as_of_date"),
            F.col("cais_fdid_bk_firmDesignatedID").alias("firm_designated_id"),
            F.col("cais_fdid_bk_catReporterCRD").alias("cat_reporter_crd"),
            F.col("correspondent_crd"),
            F.col("fdid_type"),
            F.col("fdid_date"),
            F.col("fdid_end_date"),
            F.col("fdid_end_reason"),
            F.col("branch_office_crd"),
            F.coalesce(F.col("customer_count"), F.lit(0)).alias("customer_count"),
            F.coalesce(F.col("has_active_trdholder"), F.lit(False)).alias(
                "has_active_trdholder"
            ),
            F.coalesce(F.col("has_active_ltid"), F.lit(False)).alias("has_active_ltid"),
            F.col("record_source").alias("source_file"),
            F.col("dv2_source_hk"),
        )
    )


@dlt.expect_all_or_fail({
    "non_null_firm_designated_id": "firm_designated_id IS NOT NULL",
    "non_null_cat_reporter_crd":   "cat_reporter_crd IS NOT NULL AND cat_reporter_crd > 0",
    "valid_fdid_type":
        "fdid_type IN ('ACCOUNT', 'RELATIONSHIP', 'ENTITYID')",
    "valid_end_reason":
        "fdid_end_reason IS NULL OR fdid_end_reason IN "
        "('CORRECTION','ENDED','INACTIVE','REPLACED','OTHER','TRANSFER')",
    "trdholder_required":
        "has_active_trdholder = TRUE OR fdid_end_date IS NOT NULL",
})
@dlt.table(
    name="fact_cais_fdid",
    comment="CAIS FDID snapshot fact - per-day point-in-time state of each FDID.",
    partition_cols=["as_of_date"],
    table_properties={
        "quality": "gold",
        "subject_area": "gold_fact_cais",
        "spec_version": "CAIS_v2.2.0r4",
    },
)
def fact_cais_fdid():
    return dlt.read("v_cais_fdid_curated")
