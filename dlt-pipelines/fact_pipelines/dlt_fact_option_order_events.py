# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 cat-data-model contributors
"""DLT pipeline: fact_option_order_events.

Loads simple-option order/quote/lifecycle events (Section 5.1, excluding
trades and allocations) from Silver into Gold. The trade/fulfillment
events go to fact_option_executions; the post-trade allocation events
go to fact_option_allocations.
"""
import dlt
from pyspark.sql import functions as F


SILVER_DB = spark.conf.get("silver_database", "silver")


# Event codes that route to fact_option_order_events.
ORDER_EVENT_CODES = [
    "MONO", "MONOS",
    "MOOR", "MOMR", "MOCR", "MOORS", "MOMRS", "MOCRS", "MOOA",
    "MOIR", "MOIM", "MOIC", "MOIMR", "MOICR",
    "MOCO", "MOCOM", "MOCOC",
    "MOOM", "MOOMS", "MOOMR", "MOOJ", "MOOC", "MOOCR",
    "MONQ", "MORQ", "MOQR", "MOQC", "MOQM",
    "MOOE",
]


@dlt.view(
    name="v_option_order_events_curated",
    comment="Joins link_option_event with sat_option_event_state and "
            "sat_option_order_state for the order-flow event subset.",
)
def v_option_order_events_curated():
    link = spark.table(f"{SILVER_DB}.link_option_event").filter(
        F.col("event_code").isin(ORDER_EVENT_CODES)
    )
    sat_event = spark.table(f"{SILVER_DB}.sat_option_event_state").filter(
        "load_end_dts IS NULL"
    )
    sat_order = spark.table(f"{SILVER_DB}.sat_option_order_state").filter(
        "load_end_dts IS NULL"
    )
    return (
        link.join(sat_event, "option_event_hk", "left")
            .join(sat_order, "option_order_hk", "left")
            .select(
                F.col("event_timestamp").alias("event_dts"),
                F.to_date(F.col("event_timestamp")).alias("event_date"),
                F.col("event_code").alias("cat_event_code"),
                F.col("option_order_hk").alias("cat_order_id"),
                # Per-event attributes from sat_option_event_state
                F.col("event_qty").alias("quantity_event"),
                F.col("event_price").alias("price_event"),
                F.col("cancel_qty"),
                F.col("initiator"),
                # Header attributes from sat_option_order_state
                F.col("side"),
                F.col("limit_price").alias("price"),
                F.col("order_quantity").alias("quantity"),
                F.col("leaves_quantity").alias("leaves_qty"),
                F.col("net_price"),
                F.col("time_in_force"),
                F.col("trading_session"),
                F.col("open_close_indicator"),
                F.col("handling_instructions"),
                F.col("dv2_source_hk"),
                F.col("record_source").alias("source_file"),
            )
    )


@dlt.expect_all_or_fail({
    "valid_event_code":     "cat_event_code IS NOT NULL",
    "non_null_event_dts":   "event_dts IS NOT NULL",
    "valid_side":           "side IS NULL OR side IN ('BUY','SELL','SELLSHORT','SELLSHORTEXEMPT')",
})
@dlt.table(
    name="fact_option_order_events",
    comment="Header-grain row per simple-option order/quote/lifecycle event "
            "(Section 5.1 of CAT IM Tech Specs v4.1.0r15, excl. exec/alloc).",
    partition_cols=["event_date"],
)
def fact_option_order_events():
    return dlt.read("v_option_order_events_curated")
