# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 cat-pretrade-data-model contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""DLT fact pipeline for CAT order events. Reads the relevant Silver links and sats, joins the needed dims via PIT, applies quality gates, writes fact_cat_order_events."""
import dlt
from pyspark.sql import functions as F

SILVER_DB = spark.conf.get("silver_database", "silver")
GOLD_DB = spark.conf.get("gold_database", "gold")

# ---------------------------------------------------------------------------
# Source view - curate CAT-shaped rows from Silver link + sat
# ---------------------------------------------------------------------------
@dlt.view(
 name="v_order_events_curated",
 comment="Curated Silver view producing CAT-event-shaped rows for the fact",
)
def v_order_events_curated():
 lnk = spark.readStream.table(f"{SILVER_DB}.link_order_event")
 sat = spark.table(f"{SILVER_DB}.sat_order_event_details")

 # Streaming join against the event satellite (same grain, 1:1)
 return (
 lnk.alias("l").join(
 sat.alias("s"),
 F.col("l.event_link_hk") == F.col("s.event_link_hk"),
).select(
 F.col("s.firm_roe_id").alias("firm_roe_id"),
 F.col("s.cat_order_id").alias("cat_order_id"),
 F.col("s.cat_event_code").alias("cat_event_code"),
 F.col("l.party_id_bk").alias("party_id_bk"),
 F.col("l.instrument_id_bk").alias("instrument_id_bk"),
 F.col("l.venue_id_bk").alias("venue_id_bk"),
 F.col("l.account_id_bk").alias("account_id_bk"),
 F.col("l.desk_id_bk").alias("desk_id_bk"),
 F.col("l.trader_id_bk").alias("trader_id_bk"),
 F.col("l.counterparty_party_id_bk").alias("counterparty_party_id_bk"),
 F.col("s.event_timestamp").alias("event_timestamp"),
 F.to_date("s.event_timestamp").alias("event_date"),
 F.col("s.event_nanos").alias("event_nanos"),
 F.col("s.received_timestamp").alias("received_timestamp"),
 F.col("s.route_timestamp").alias("route_timestamp"),
 F.col("s.side").alias("side"),
 F.col("s.order_type").alias("order_type"),
 F.col("s.order_capacity").alias("order_capacity"),
 F.col("s.time_in_force").alias("time_in_force"),
 F.col("s.quantity").alias("quantity"),
 F.col("s.leaves_quantity").alias("leaves_quantity"),
 F.col("s.cumulative_filled_quantity").alias("cumulative_filled_quantity"),
 F.col("s.price").alias("price"),
 F.col("s.execution_price").alias("execution_price"),
 F.col("s.execution_quantity").alias("execution_quantity"),
 F.col("s.currency_code").alias("currency_code"),
 F.col("s.handling_instructions").alias("handling_instructions"),
 F.col("s.special_handling_codes").alias("special_handling_codes"),
 F.col("s.display_instruction").alias("display_instruction"),
 F.col("s.iso_flag").alias("iso_flag"),
 F.col("s.short_sale_exempt_flag").alias("short_sale_exempt_flag"),
 F.col("s.solicited_flag").alias("solicited_flag"),
 F.col("s.directed_flag").alias("directed_flag"),
 F.col("s.parent_order_id").alias("parent_order_id"),
 F.col("s.routed_to_venue_mic").alias("routed_to_venue_mic"),
 F.col("s.routed_to_firm_imid").alias("routed_to_firm_imid"),
 F.col("s.original_event_ref").alias("original_event_ref"),
 F.col("s.correction_reason").alias("correction_reason"),
 F.col("s.cancel_reason").alias("cancel_reason"),
 F.col("s.rejection_reason").alias("rejection_reason"),
 F.col("s.source_system").alias("source_system"),
 F.col("l.event_link_hk").alias("dv2_source_hk"),
 F.current_timestamp().alias("load_date"),
 F.lit(f"{SILVER_DB}.link_order_event").alias("record_source"),
)
)

# ---------------------------------------------------------------------------
# DQ gates (HARD FAIL - halt pipeline if any row violates)
# ---------------------------------------------------------------------------
@dlt.expect_all_or_fail(
 {
 "firm_roe_id_required": "firm_roe_id IS NOT NULL",
 "dv2_source_hk_required": "dv2_source_hk IS NOT NULL",
 "cat_event_code_required": "cat_event_code IS NOT NULL",
 "event_timestamp_required": "event_timestamp IS NOT NULL",
 "event_date_required": "event_date IS NOT NULL",
 "cat_event_code_format": "cat_event_code RLIKE '^[A-Z]{3,10}(_[A-Z])?$'",
 "side_enum": "side IS NULL OR side IN ('BUY','SELL','SELL_SHORT','SELL_SHORT_EXEMPT')",
 }
)
@dlt.view(
 name="v_order_events_validated",
 comment="Order-event stream after hard-fail DQ gates",
)
def v_order_events_validated():
 return dlt.read_stream("v_order_events_curated")

# ---------------------------------------------------------------------------
# Join to Gold dims to resolve surrogate keys (point-in-time via effective
# date window - BETWEEN effective_start_date AND effective_end_date)
# ---------------------------------------------------------------------------
@dlt.view(name="v_order_events_enriched")
def v_order_events_enriched():
 e = dlt.read_stream("v_order_events_validated")

 dim_party = spark.table(f"{GOLD_DB}.dim_party")
 dim_instrument = spark.table(f"{GOLD_DB}.dim_instrument")
 dim_venue = spark.table(f"{GOLD_DB}.dim_venue")
 dim_account = spark.table(f"{GOLD_DB}.dim_account")
 dim_desk = spark.table(f"{GOLD_DB}.dim_desk")
 dim_trader = spark.table(f"{GOLD_DB}.dim_trader")

 return (
 e.alias("e").join(
 dim_party.alias("p"),
 (F.col("e.party_id_bk") == F.col("p.party_id_bk"))
 & (F.col("e.event_date").between(F.col("p.effective_start_date"), F.col("p.effective_end_date"))),
 "left",
).join(
 dim_instrument.alias("i"),
 (F.col("e.instrument_id_bk") == F.col("i.instrument_id_bk"))
 & (F.col("e.event_date").between(F.col("i.effective_start_date"), F.col("i.effective_end_date"))),
 "left",
).join(
 dim_venue.alias("v"),
 (F.col("e.venue_id_bk") == F.col("v.venue_id_bk"))
 & (F.col("e.event_date").between(F.col("v.effective_start_date"), F.col("v.effective_end_date"))),
 "left",
).join(
 dim_account.alias("a"),
 (F.col("e.account_id_bk") == F.col("a.account_id_bk"))
 & (F.col("e.event_date").between(F.col("a.effective_start_date"), F.col("a.effective_end_date"))),
 "left",
).join(
 dim_desk.alias("d"),
 (F.col("e.desk_id_bk") == F.col("d.desk_id_bk"))
 & (F.col("e.event_date").between(F.col("d.effective_start_date"), F.col("d.effective_end_date"))),
 "left",
).join(
 dim_trader.alias("t"),
 (F.col("e.trader_id_bk") == F.col("t.trader_id_bk"))
 & (F.col("e.event_date").between(F.col("t.effective_start_date"), F.col("t.effective_end_date"))),
 "left",
).join(
 dim_party.alias("cp"),
 (F.col("e.counterparty_party_id_bk") == F.col("cp.party_id_bk"))
 & (F.col("e.event_date").between(F.col("cp.effective_start_date"), F.col("cp.effective_end_date"))),
 "left",
).select(
 "e.firm_roe_id", "e.cat_order_id", "e.cat_event_code",
 F.col("p.dim_party_sk").alias("dim_party_sk"),
 F.col("i.dim_instrument_sk").alias("dim_instrument_sk"),
 F.col("v.dim_venue_sk").alias("dim_venue_sk"),
 F.col("a.dim_account_sk").alias("dim_account_sk"),
 F.col("d.dim_desk_sk").alias("dim_desk_sk"),
 F.col("t.dim_trader_sk").alias("dim_trader_sk"),
 F.col("cp.dim_party_sk").alias("counterparty_party_sk"),
 "e.event_date", "e.event_timestamp", "e.event_nanos",
 "e.received_timestamp", "e.route_timestamp",
 "e.side", "e.order_type", "e.order_capacity", "e.time_in_force",
 "e.quantity", "e.leaves_quantity", "e.cumulative_filled_quantity",
 "e.price", "e.execution_price", "e.execution_quantity",
 "e.currency_code", "e.handling_instructions", "e.special_handling_codes",
 "e.display_instruction", "e.iso_flag", "e.short_sale_exempt_flag",
 "e.solicited_flag", "e.directed_flag", "e.parent_order_id",
 "e.routed_to_venue_mic", "e.routed_to_firm_imid",
 "e.original_event_ref", "e.correction_reason",
 "e.cancel_reason", "e.rejection_reason",
 "e.dv2_source_hk", "e.source_system", "e.load_date", "e.record_source",
)
)

# ---------------------------------------------------------------------------
# Target - append-only streaming table. CAT corrections are NEW events.
# ---------------------------------------------------------------------------
@dlt.expect_all_or_fail(
 {
 "sk_party_resolved": "dim_party_sk IS NOT NULL",
 "sk_instrument_resolved": "dim_instrument_sk IS NOT NULL",
 }
)
@dlt.table(
 name="fact_cat_order_events",
 comment="CAT order-event grain fact - append-only, partitioned by event_date",
 partition_cols=["event_date"],
 table_properties={
 "delta.enableChangeDataFeed": "true",
 "delta.autoOptimize.optimizeWrite": "true",
 "delta.autoOptimize.autoCompact": "true",
 "subject_area": "gold_fact",
 "cat_submission_file_type": "OrderEvents",
 "grain": "one row per CAT order event",
 },
)
def fact_cat_order_events():
 return dlt.read_stream("v_order_events_enriched")
