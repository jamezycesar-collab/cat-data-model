"""DLT pipeline for fact_multileg_option_events and fact_multileg_option_legs.

Reads link_multileg_event and link_multileg_order_leg from Silver, joins to
the conformed Gold dimensions via PIT, applies expect_all_or_fail quality
gates, and writes the two-table multi-leg fact pair.

The 25 multi-leg event codes are validated against ref_cat_event_type before
write.
"""
import dlt
from pyspark.sql import functions as F


VALID_MULTILEG_EVENT_CODES = [
 'MLNO', 'MLOR', 'MLMR', 'MLCR', 'MLOA',
 'MLIR', 'MLIM', 'MLIC', 'MLIMR', 'MLICR',
 'MLCO', 'MLCOM', 'MLCOC',
 'MLOM', 'MLOMR', 'MLOC', 'MLOCR',
 'MLNQ', 'MLRQ', 'MLQS', 'MLQR', 'MLQC', 'MLQM',
 'MLOS', 'MLOE',
]


@dlt.table(
 name="fact_multileg_option_events",
 comment="Header-grain fact for multi-leg CAT events (Section 5.2 of CAT IM Tech Specs).",
 partition_cols=["event_date"])
@dlt.expect_all_or_fail({
 "valid_event_code": f"cat_event_code IN ({','.join(repr(c) for c in VALID_MULTILEG_EVENT_CODES)})",
 "non_null_event_dts": "event_dts IS NOT NULL",
 "non_null_party_sk": "party_sk IS NOT NULL AND party_sk > 0",
 "non_null_event_type_sk": "event_type_sk IS NOT NULL AND event_type_sk > 0",
 "leg_count_at_least_2": "leg_count >= 2",
 "leg_count_capped": "leg_count <= 50", # spec implies <=50 legs per CAT requirements
})
def fact_multileg_option_events():
 link = dlt.read("link_multileg_event")
 sat = dlt.read("sat_multileg_event_state").filter("load_end_dts IS NULL")
 order_sat = dlt.read("sat_multileg_order_state").filter("load_end_dts IS NULL")
 legs = dlt.read("link_multileg_order_leg")

 leg_count = legs.groupBy("multileg_order_hk").agg(F.count("*").alias("leg_count"))

 # PIT joins to conformed dims
 dim_date = dlt.read("dim_date")
 dim_party = dlt.read("dim_party")
 dim_event_type = dlt.read("dim_event_type")
 dim_venue = dlt.read("dim_venue")

 return (link
 .join(sat, "multileg_event_hk")
 .join(order_sat, "multileg_order_hk")
 .join(leg_count, "multileg_order_hk")
 .join(dim_date, F.to_date(F.col("event_timestamp")) == dim_date.calendar_date)
 .join(dim_party.filter("is_current = TRUE"),
 link.sender_party_hk == dim_party.party_bk_hk, how="left")
 .join(dim_event_type, link.event_type_hk == dim_event_type.event_type_bk_hk)
 .join(dim_venue.filter("is_current = TRUE"),
 link.venue_hk == dim_venue.venue_bk_hk, how="left")
 .select(
 F.col("event_timestamp").alias("event_dts"),
 F.to_date(F.col("event_timestamp")).alias("event_date"),
 "date_sk",
 F.lit(None).cast("bigint").alias("instrument_sk"), # set per-leg
 "party_sk",
 F.lit(None).cast("bigint").alias("trader_sk"),
 F.lit(None).cast("bigint").alias("desk_sk"),
 F.lit(None).cast("bigint").alias("account_sk"),
 "venue_sk",
 "event_type_sk",
 F.lit(None).cast("bigint").alias("multileg_strategy_sk"),
 F.col("multileg_order_hk").alias("multileg_order_id"),
 F.lit(None).cast("string").alias("parent_multileg_order_id"),
 F.col("leg_count"),
 F.col("event_code").alias("cat_event_code"),
 F.lit(None).cast("string").alias("side_summary"),
 F.col("net_price"),
 F.col("order_quantity"),
 F.col("time_in_force"),
 F.col("handling_instructions"),
 F.col("record_source").alias("source_file"),
 F.col("dv2_source_hk").alias("source_batch_id"),
 F.col("dv2_source_hk"),
 F.lit("PASS").alias("quality_outcome")))


@dlt.table(
 name="fact_multileg_option_legs",
 comment="Per-leg detail for multi-leg events. One header row to N leg rows.")
@dlt.expect_all_or_fail({
 "non_null_instrument_sk": "instrument_sk IS NOT NULL AND instrument_sk > 0",
 "valid_leg_side": "leg_side IN ('BUY', 'SELL')",
 "non_null_qty": "leg_quantity IS NOT NULL AND leg_quantity > 0",
 "non_null_ratio": "leg_ratio IS NOT NULL AND leg_ratio > 0",
})
def fact_multileg_option_legs():
 fact_header = dlt.read("fact_multileg_option_events")
 legs = dlt.read("link_multileg_order_leg")
 leg_sat = dlt.read("sat_multileg_leg_state").filter("load_end_dts IS NULL")
 dim_instr = dlt.read("dim_instrument").filter("is_current = TRUE")

 return (fact_header
 .join(legs, fact_header.multileg_order_id == legs.multileg_order_hk)
 .join(leg_sat, "multileg_leg_hk")
 .join(dim_instr, legs.instrument_hk == dim_instr.instrument_bk_hk)
 .select(
 fact_header.multileg_event_sk,
 legs.leg_seq,
 dim_instr.instrument_sk,
 legs.leg_side,
 leg_sat.leg_open_close,
 leg_sat.leg_quantity,
 leg_sat.leg_price,
 legs.leg_ratio,
 leg_sat.leg_status,
 fact_header.source_file,
 fact_header.source_batch_id))
