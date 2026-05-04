# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 cat-data-model contributors
"""DLT pipeline: fact_option_allocations.

Loads simple-option post-trade allocation events (MOPA, MOAA) from
Silver into Gold. The Allocations submission file type carries these.
"""
import dlt
from pyspark.sql import functions as F


SILVER_DB = spark.conf.get("silver_database", "silver")

ALLOCATION_EVENT_CODES = ["MOPA", "MOAA"]


@dlt.view(name="v_option_allocations_curated")
def v_option_allocations_curated():
    link = spark.table(f"{SILVER_DB}.link_option_event").filter(
        F.col("event_code").isin(ALLOCATION_EVENT_CODES)
    )
    sat_event = spark.table(f"{SILVER_DB}.sat_option_event_state").filter(
        "load_end_dts IS NULL"
    )
    return (
        link.join(sat_event, "option_event_hk", "left")
            .select(
                F.col("event_timestamp").alias("event_dts"),
                F.to_date(F.col("event_timestamp")).alias("event_date"),
                F.col("event_code").alias("cat_event_code"),
                F.col("event_qty").alias("quantity"),
                F.col("event_price").alias("price"),
                F.col("cancel_flag"),
                F.col("cancel_timestamp"),
                F.col("occ_clearing_member_id"),
                F.col("dv2_source_hk"),
                F.col("record_source").alias("source_file"),
            )
    )


@dlt.expect_all_or_fail({
    "valid_event_code":   "cat_event_code IN ('MOPA','MOAA')",
    "non_null_event_dts": "event_dts IS NOT NULL",
    "non_null_quantity":  "quantity IS NOT NULL AND quantity > 0",
})
@dlt.table(
    name="fact_option_allocations",
    comment="Simple-option post-trade allocations (MOPA, MOAA) "
            "from Section 5.1.13 of CAT IM Tech Specs v4.1.0r15.",
    partition_cols=["event_date"],
)
def fact_option_allocations():
    return dlt.read("v_option_allocations_curated")
