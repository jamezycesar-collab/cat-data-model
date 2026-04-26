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

"""DLT fact pipeline for CAT quote events (MEQR, MEQS). Reads the quote Silver link and sat, joins conformed dims, applies quality gates, writes fact_cat_quotes."""
import dlt
from pyspark.sql import functions as F

SILVER_DB = spark.conf.get("silver_database", "silver")
GOLD_DB = spark.conf.get("gold_database", "gold")

@dlt.view(name="v_quotes_curated")
def v_quotes_curated:
 lnk = spark.readStream.table(f"{SILVER_DB}.link_quote")
 sat = spark.table(f"{SILVER_DB}.sat_quote_details")

 return (
 lnk.alias("l").join(sat.alias("s"), F.col("l.quote_link_hk") == F.col("s.quote_link_hk")).select(
 F.col("s.firm_roe_id").alias("firm_roe_id"),
 F.col("s.cat_quote_id").alias("cat_quote_id"),
 F.col("s.cat_rfq_id").alias("cat_rfq_id"),
 F.col("s.cat_event_code").alias("cat_event_code"),
 F.col("l.party_id_bk").alias("party_id_bk"),
 F.col("l.counterparty_party_id_bk").alias("counterparty_party_id_bk"),
 F.col("l.instrument_id_bk").alias("instrument_id_bk"),
 F.col("l.venue_id_bk").alias("venue_id_bk"),
 F.col("l.trader_id_bk").alias("trader_id_bk"),
 F.col("s.quote_side").alias("quote_side"),
 F.col("s.bid_price").alias("bid_price"),
 F.col("s.ask_price").alias("ask_price"),
 F.col("s.bid_size").alias("bid_size"),
 F.col("s.ask_size").alias("ask_size"),
 F.col("s.currency_code").alias("currency_code"),
 F.col("s.quote_status").alias("quote_status"),
 F.col("s.quote_type").alias("quote_type"),
 F.col("s.min_quote_life_ms").alias("min_quote_life_ms"),
 F.col("s.quote_expiry_timestamp").alias("quote_expiry_timestamp"),
 F.col("s.rfq_expiry_timestamp").alias("rfq_expiry_timestamp"),
 F.col("s.quote_timestamp").alias("quote_timestamp"),
 F.to_date("s.quote_timestamp").alias("event_date"),
 F.col("s.quote_nanos").alias("quote_nanos"),
 F.col("s.source_system").alias("source_system"),
 F.col("l.quote_link_hk").alias("dv2_source_hk"),
 F.current_timestamp.alias("load_date"),
 F.lit(f"{SILVER_DB}.link_quote").alias("record_source"),
)
)

@dlt.expect_all_or_fail(
 {
 "firm_roe_id_required": "firm_roe_id IS NOT NULL",
 "dv2_source_hk_required": "dv2_source_hk IS NOT NULL",
 "cat_event_code_required": "cat_event_code IS NOT NULL",
 "quote_timestamp_required":"quote_timestamp IS NOT NULL",
 "quote_side_enum": "quote_side IS NULL OR quote_side IN ('BID','OFFER','TWO_WAY')",
 "quote_type_enum": "quote_type IS NULL OR quote_type IN ('INDICATIVE','FIRM','TRADEABLE')",
 "bid_ask_consistent": "NOT (quote_side = 'TWO_WAY' AND (bid_price IS NULL OR ask_price IS NULL))",
 }
)
@dlt.view(name="v_quotes_validated")
def v_quotes_validated:
 return dlt.read_stream("v_quotes_curated")

@dlt.view(name="v_quotes_enriched")
def v_quotes_enriched:
 q = dlt.read_stream("v_quotes_validated")
 dim_party = spark.table(f"{GOLD_DB}.dim_party")
 dim_instrument = spark.table(f"{GOLD_DB}.dim_instrument")
 dim_venue = spark.table(f"{GOLD_DB}.dim_venue")
 dim_trader = spark.table(f"{GOLD_DB}.dim_trader")

 return (
 q.alias("q").join(
 dim_party.alias("p"),
 (F.col("q.party_id_bk") == F.col("p.party_id_bk"))
 & (F.col("q.event_date").between(F.col("p.effective_start_date"), F.col("p.effective_end_date"))),
 "left",
).join(
 dim_instrument.alias("i"),
 (F.col("q.instrument_id_bk") == F.col("i.instrument_id_bk"))
 & (F.col("q.event_date").between(F.col("i.effective_start_date"), F.col("i.effective_end_date"))),
 "left",
).join(
 dim_venue.alias("v"),
 (F.col("q.venue_id_bk") == F.col("v.venue_id_bk"))
 & (F.col("q.event_date").between(F.col("v.effective_start_date"), F.col("v.effective_end_date"))),
 "left",
).join(
 dim_trader.alias("t"),
 (F.col("q.trader_id_bk") == F.col("t.trader_id_bk"))
 & (F.col("q.event_date").between(F.col("t.effective_start_date"), F.col("t.effective_end_date"))),
 "left",
).join(
 dim_party.alias("cp"),
 (F.col("q.counterparty_party_id_bk") == F.col("cp.party_id_bk"))
 & (F.col("q.event_date").between(F.col("cp.effective_start_date"), F.col("cp.effective_end_date"))),
 "left",
).select(
 "q.firm_roe_id", "q.cat_quote_id", "q.cat_rfq_id", "q.cat_event_code",
 F.col("p.dim_party_sk").alias("dim_party_sk"),
 F.col("cp.dim_party_sk").alias("counterparty_party_sk"),
 F.col("i.dim_instrument_sk").alias("dim_instrument_sk"),
 F.col("v.dim_venue_sk").alias("dim_venue_sk"),
 F.col("t.dim_trader_sk").alias("dim_trader_sk"),
 "q.quote_side", "q.bid_price", "q.ask_price", "q.bid_size", "q.ask_size",
 "q.currency_code", "q.quote_status", "q.quote_type", "q.min_quote_life_ms",
 "q.quote_expiry_timestamp", "q.rfq_expiry_timestamp",
 "q.quote_timestamp", "q.quote_nanos", "q.event_date",
 "q.dv2_source_hk", "q.source_system", "q.load_date", "q.record_source",
)
)

@dlt.expect_all_or_fail(
 {
 "sk_party_resolved": "dim_party_sk IS NOT NULL",
 "sk_instrument_resolved": "dim_instrument_sk IS NOT NULL",
 }
)
@dlt.table(
 name="fact_cat_quotes",
 comment="CAT quote-event grain fact",
 partition_cols=["event_date"],
 table_properties={
 "delta.enableChangeDataFeed": "true",
 "delta.autoOptimize.optimizeWrite": "true",
 "subject_area": "gold_fact",
 "cat_submission_file_type": "QuoteEvents",
 "grain": "one row per quote event",
 },
)
def fact_cat_quotes:
 return dlt.read_stream("v_quotes_enriched")
