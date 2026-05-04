# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 cat-data-model contributors
"""DLT pipeline: fact_option_executions.

Loads simple-option trade and fulfillment events (MOOT, MOOF, MOOFS, MOFA)
from Silver into Gold.
"""
import dlt
from pyspark.sql import functions as F


SILVER_DB = spark.conf.get("silver_database", "silver")

EXECUTION_EVENT_CODES = ["MOOT", "MOOF", "MOOFS", "MOFA"]


@dlt.view(name="v_option_executions_curated")
def v_option_executions_curated():
    link = spark.table(f"{SILVER_DB}.link_option_event").filter(
        F.col("event_code").isin(EXECUTION_EVENT_CODES)
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
                F.col("capacity"),
                F.col("market_center_id"),
                F.col("side_details_ind"),
                F.col("clearing_firm"),
                F.col("fulfillment_id"),
                F.col("fulfillment_link_type"),
                F.col("cancel_flag"),
                F.col("cancel_timestamp"),
                F.col("dv2_source_hk"),
                F.col("record_source").alias("source_file"),
            )
    )


@dlt.expect_all_or_fail({
    "valid_event_code":   "cat_event_code IN ('MOOT','MOOF','MOOFS','MOFA')",
    "non_null_event_dts": "event_dts IS NOT NULL",
    "non_null_quantity":  "quantity IS NOT NULL AND quantity > 0",
    "non_null_price":     "price IS NOT NULL AND price > 0",
})
@dlt.table(
    name="fact_option_executions",
    comment="Per-execution row for MOOT / MOOF / MOOFS / MOFA "
            "(Section 5.1.11 - 5.1.14 of CAT IM Tech Specs v4.1.0r15).",
    partition_cols=["event_date"],
)
def fact_option_executions():
    return dlt.read("v_option_executions_curated")
