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

"""DLT fact pipeline for CAT allocations. Builds fact_cat_allocations from the allocation Silver link and its sats, with PIT joins to the conformed dims."""
import dlt
from pyspark.sql import functions as F

SILVER_DB = spark.conf.get("silver_database", "silver")
GOLD_DB = spark.conf.get("gold_database", "gold")

@dlt.view(name="v_allocations_curated")
def v_allocations_curated:
 lnk = spark.readStream.table(f"{SILVER_DB}.link_allocation")
 sat = spark.table(f"{SILVER_DB}.sat_allocation_details")

 return (
 lnk.alias("l").join(sat.alias("s"), F.col("l.allocation_link_hk") == F.col("s.allocation_link_hk")).select(
 F.col("s.firm_roe_id").alias("firm_roe_id"),
 F.col("s.cat_order_id").alias("cat_order_id"),
 F.col("s.cat_allocation_id").alias("cat_allocation_id"),
 F.col("s.cat_event_code").alias("cat_event_code"),
 F.col("l.party_id_bk").alias("party_id_bk"),
 F.col("l.instrument_id_bk").alias("instrument_id_bk"),
 F.col("l.account_id_bk").alias("account_id_bk"),
 F.col("l.desk_id_bk").alias("desk_id_bk"),
 F.col("s.parent_execution_id").alias("parent_execution_id"),
 F.col("s.allocation_method").alias("allocation_method"),
 F.col("s.allocation_status").alias("allocation_status"),
 F.col("s.allocated_quantity").alias("allocated_quantity"),
 F.col("s.allocated_price").alias("allocated_price"),
 F.col("s.allocation_pct").alias("allocation_pct"),
 F.col("s.gross_amount").alias("gross_amount"),
 F.col("s.commission").alias("commission"),
 F.col("s.commission_currency").alias("commission_currency"),
 F.col("s.net_amount").alias("net_amount"),
 F.col("s.settlement_date").alias("settlement_date"),
 F.col("s.settlement_currency").alias("settlement_currency"),
 F.col("s.affirmation_timestamp").alias("affirmation_timestamp"),
 F.col("s.allocation_timestamp").alias("allocation_timestamp"),
 F.to_date("s.allocation_timestamp").alias("event_date"),
 F.col("s.allocation_nanos").alias("allocation_nanos"),
 F.col("s.source_system").alias("source_system"),
 F.col("l.allocation_link_hk").alias("dv2_source_hk"),
 F.current_timestamp.alias("load_date"),
 F.lit(f"{SILVER_DB}.link_allocation").alias("record_source"),
)
)

@dlt.expect_all_or_fail(
 {
 "firm_roe_id_required": "firm_roe_id IS NOT NULL",
 "dv2_source_hk_required": "dv2_source_hk IS NOT NULL",
 "cat_event_code_required": "cat_event_code IS NOT NULL",
 "allocation_timestamp_required": "allocation_timestamp IS NOT NULL",
 "allocation_method_enum":
 "allocation_method IS NULL OR allocation_method IN ('PRO_RATA','MANUAL','FIFO','LIFO','AVG_PRICE')",
 "allocated_quantity_nonneg": "allocated_quantity IS NULL OR allocated_quantity >= 0",
 }
)
@dlt.view(name="v_allocations_validated")
def v_allocations_validated:
 return dlt.read_stream("v_allocations_curated")

@dlt.view(name="v_allocations_enriched")
def v_allocations_enriched:
 a = dlt.read_stream("v_allocations_validated")
 dim_party = spark.table(f"{GOLD_DB}.dim_party")
 dim_instrument = spark.table(f"{GOLD_DB}.dim_instrument")
 dim_account = spark.table(f"{GOLD_DB}.dim_account")
 dim_desk = spark.table(f"{GOLD_DB}.dim_desk")

 return (
 a.alias("a").join(
 dim_party.alias("p"),
 (F.col("a.party_id_bk") == F.col("p.party_id_bk"))
 & (F.col("a.event_date").between(F.col("p.effective_start_date"), F.col("p.effective_end_date"))),
 "left",
).join(
 dim_instrument.alias("i"),
 (F.col("a.instrument_id_bk") == F.col("i.instrument_id_bk"))
 & (F.col("a.event_date").between(F.col("i.effective_start_date"), F.col("i.effective_end_date"))),
 "left",
).join(
 dim_account.alias("ac"),
 (F.col("a.account_id_bk") == F.col("ac.account_id_bk"))
 & (F.col("a.event_date").between(F.col("ac.effective_start_date"), F.col("ac.effective_end_date"))),
 "left",
).join(
 dim_desk.alias("d"),
 (F.col("a.desk_id_bk") == F.col("d.desk_id_bk"))
 & (F.col("a.event_date").between(F.col("d.effective_start_date"), F.col("d.effective_end_date"))),
 "left",
).select(
 "a.firm_roe_id", "a.cat_order_id", "a.cat_allocation_id", "a.cat_event_code",
 F.col("p.dim_party_sk").alias("dim_party_sk"),
 F.col("i.dim_instrument_sk").alias("dim_instrument_sk"),
 F.col("ac.dim_account_sk").alias("dim_account_sk"),
 F.col("d.dim_desk_sk").alias("dim_desk_sk"),
 "a.parent_execution_id", "a.allocation_method", "a.allocation_status",
 "a.allocated_quantity", "a.allocated_price", "a.allocation_pct",
 "a.gross_amount", "a.commission", "a.commission_currency", "a.net_amount",
 "a.settlement_date", "a.settlement_currency",
 "a.affirmation_timestamp", "a.allocation_timestamp", "a.allocation_nanos",
 "a.event_date",
 "a.dv2_source_hk", "a.source_system", "a.load_date", "a.record_source",
)
)

@dlt.expect_all_or_fail(
 {
 "sk_party_resolved": "dim_party_sk IS NOT NULL",
 "sk_account_resolved": "dim_account_sk IS NOT NULL",
 "sk_instrument_resolved": "dim_instrument_sk IS NOT NULL",
 }
)
@dlt.table(
 name="fact_cat_allocations",
 comment="CAT allocation-event grain fact",
 partition_cols=["event_date"],
 table_properties={
 "delta.enableChangeDataFeed": "true",
 "delta.autoOptimize.optimizeWrite": "true",
 "subject_area": "gold_fact",
 "cat_submission_file_type": "Allocations",
 "grain": "one row per allocation event",
 },
)
def fact_cat_allocations:
 return dlt.read_stream("v_allocations_enriched")
