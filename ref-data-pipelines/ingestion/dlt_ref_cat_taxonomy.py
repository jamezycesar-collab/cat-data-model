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

"""Ingest FINRA CAT event taxonomy (50 events) into the ref_cat_event_type reference table."""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
 StructType, StructField, StringType, DateType,
)

SILVER_DB = spark.conf.get("silver_database", "silver")
GOLD_DB = spark.conf.get("gold_database", "gold")

CAT_SPEC_VERSION = "v4.1.0r9"
CAT_SPEC_URL = "https://www.catnmsplan.com/technical-specifications"

# ---------------------------------------------------------------------------
# FINRA CAT 50-event canonical seed
# (code, name, description, category, lifecycle_stage, phase, submission_file_type)
# ---------------------------------------------------------------------------
CAT_EVENTS = [
 # ───────────────────── Order Request (pre-order, phase 2d) ───────────────
 ("MEIR", "New Order Internal Route Request", "Request to route an order internally before it becomes a live order", "ORDER_REQUEST", "PRE_TRADE", "2d", "OrderEvents"),
 ("MOIR", "New Order Intra-firm Route Request", "Request for an intra-firm route of a prospective order", "ORDER_REQUEST", "PRE_TRADE", "2d", "OrderEvents"),

 # ───────────────────── Quote / RFQ (phase 2c) ────────────────────────────
 ("MEQR", "New Quote Request", "Customer or proprietary RFQ issued to one or more liquidity providers","QUOTE", "PRE_TRADE", "2c", "QuoteEvents"),
 ("MEQS", "Quote Sent in response to RFQ", "Firm's quote response to an inbound RFQ", "QUOTE", "PRE_TRADE", "2c", "QuoteEvents"),
 ("MOQR", "New Options Quote Request", "Listed-options RFQ", "QUOTE", "PRE_TRADE", "2c", "QuoteEvents"),
 ("MOQS", "Options Quote Sent", "Listed-options quote response", "QUOTE", "PRE_TRADE", "2c", "QuoteEvents"),

 # ───────────────────── New Orders (phase 2a/2b) ──────────────────────────
 ("MENO", "New Order Event", "Manual entry of a new order received by the firm", "ORDER", "ORDER", "2a", "OrderEvents"),
 ("MONO", "New Order (Options)", "New listed-options order event", "ORDER", "ORDER", "2b", "OrderEvents"),
 ("MECO", "Combined Order Event", "Combined order tied to multiple originating orders", "COMBINED_ORDER", "ORDER", "2a", "OrderEvents"),
 ("MERO", "Representative Order Event", "Representative order opened by the firm for OTC principal activity", "REPRESENTATIVE_ORDER", "ORDER", "2a", "OrderEvents"),

 # ───────────────────── Routing (phase 2a/2b) ─────────────────────────────
 ("MEOR", "Order Route Event", "Firm routes an order to another firm or venue", "ROUTING", "ORDER", "2a", "OrderEvents"),
 ("MOOR", "Options Order Route Event", "Listed-options route event", "ROUTING", "ORDER", "2b", "OrderEvents"),
 ("MEOA", "Order Accepted Event", "Firm accepts an inbound routed order", "ROUTING", "ORDER", "2a", "OrderEvents"),
 ("MEOR_EXT", "Order Route Event (External)", "Cross-firm route to a non-member exchange", "ROUTING", "ORDER", "2a", "OrderEvents"),

 # ───────────────────── Modifications (phase 2a/2b) ───────────────────────
 ("MEOM", "Order Modified Event", "Customer or firm modifies an open order", "MODIFICATION", "ORDER", "2a", "OrderEvents"),
 ("MEOC", "Order Cancelled Event", "Order cancellation event", "MODIFICATION", "ORDER", "2a", "OrderEvents"),
 ("MOOC", "Options Order Cancelled Event", "Listed-options cancellation", "MODIFICATION", "ORDER", "2b", "OrderEvents"),
 ("MEOJ", "Order Rejected Event", "Order rejected by the receiving firm or venue", "MODIFICATION", "ORDER", "2a", "OrderEvents"),
 ("MOOJ", "Options Order Rejected", "Listed-options rejection", "MODIFICATION", "ORDER", "2b", "OrderEvents"),
 ("MEOX", "Order Expired Event", "Order expires (GTD / session end)", "MODIFICATION", "ORDER", "2a", "OrderEvents"),
 ("MOOX", "Options Order Expired", "Listed-options expiry", "MODIFICATION", "ORDER", "2b", "OrderEvents"),

 # ───────────────────── Executions (phase 2a/2b) ──────────────────────────
 ("MEOT", "Order Trade Event", "Order execution / trade event", "EXECUTION", "EXECUTION", "2a", "OrderEvents"),
 ("MOOT", "Options Order Trade Event", "Listed-options execution", "EXECUTION", "EXECUTION", "2b", "OrderEvents"),
 ("MEOTQ","Order Trade Event (Q)", "Trade-report-facility reported execution", "EXECUTION", "EXECUTION", "2a", "OrderEvents"),
 ("MEOTS","Order Trade Event (Subsequent)", "Subsequent execution against remainder of the order", "EXECUTION", "EXECUTION", "2a", "OrderEvents"),
 ("MOOTS","Options Trade Event (Subsequent)", "Listed-options subsequent fill", "EXECUTION", "EXECUTION", "2b", "OrderEvents"),
 ("MEOF", "Order Fulfillment Event", "Order fully filled / terminated fulfillment", "EXECUTION", "EXECUTION", "2a", "OrderEvents"),
 ("MOOF", "Options Order Fulfillment", "Listed-options fulfillment event", "EXECUTION", "EXECUTION", "2b", "OrderEvents"),
 ("EOT", "Exchange Order Trade", "Exchange reporting for an order-book trade", "EXECUTION", "EXECUTION", "2a", "OrderEvents"),

 # ───────────────────── Manual events (phase 2d) ──────────────────────────
 ("MEMA", "Manual Order Modification", "Manual mod recorded by trader (MEMA)", "MANUAL", "ORDER", "2d", "OrderEvents"),
 ("MLOT", "Locked-in Trade", "Locked-in execution reported outside standard flow", "MANUAL", "EXECUTION", "2d", "OrderEvents"),
 ("MEIN", "Manual Order Internal Event", "Internal manual order state change", "MANUAL", "ORDER", "2d", "OrderEvents"),
 ("MEMA_CORR", "Manual Order Correction", "Correction to a manual order event", "MANUAL", "ORDER", "2d", "OrderEvents"),

 # ───────────────────── Options extensions (phase 2e) ─────────────────────
 ("MORE", "Options Route Event", "Listed-options routing event", "ROUTING", "ORDER", "2e", "OptionsExtensions"),
 ("MORR", "Options Route Rejected", "Listed-options route rejection", "ROUTING", "ORDER", "2e", "OptionsExtensions"),
 ("MORA", "Options Route Accepted", "Listed-options route acceptance", "ROUTING", "ORDER", "2e", "OptionsExtensions"),
 ("MOCO", "Options Complex Order Event", "Complex multi-leg options order event", "COMBINED_ORDER", "ORDER", "2e", "OptionsExtensions"),
 ("MOCX", "Options Complex Order Expiry", "Complex options order expiry", "MODIFICATION", "ORDER", "2e", "OptionsExtensions"),

 # ───────────────────── Allocations (phase 2c) ────────────────────────────
 ("MEPA", "Post-Execution Allocation", "Allocation of a block execution to underlying accounts", "ALLOCATION", "POST_TRADE","2c", "Allocations"),
 ("MEAA", "Allocation Accepted", "Receiving firm accepts an allocation", "ALLOCATION", "POST_TRADE","2c", "Allocations"),
 ("MOFA", "OTC Option Allocation", "OTC options allocation event", "ALLOCATION", "POST_TRADE","2c", "Allocations"),
 ("MEPM", "Post-Execution Allocation Modification", "Modification of a previously submitted allocation", "ALLOCATION", "POST_TRADE","2c", "Allocations"),
 ("MEPZ", "Post-Execution Allocation Cancellation", "Cancellation of an allocation", "ALLOCATION", "POST_TRADE","2c", "Allocations"),
 ("MEAX", "Allocation Rejected", "Receiving firm rejects an allocation", "ALLOCATION", "POST_TRADE","2c", "Allocations"),

 # ───────────────────── CAIS (customer/account) ───────────────────────────
 ("CAIS_C", "CAIS Customer Record", "Customer record - daily snapshot (inserts/updates/deletes)", "MANUAL", "POST_TRADE","2a", "CAIS"),
 ("CAIS_A", "CAIS Account Record", "Account record - daily snapshot", "MANUAL", "POST_TRADE","2a", "CAIS"),
 ("CAIS_R", "CAIS Trading Relationship", "Trading relationship record (natural / non-natural)", "MANUAL", "POST_TRADE","2a", "CAIS"),

 # ───────────────────── Reserved / audit ──────────────────────────────────
 ("MEAU", "Order Audit Trail Event", "Audit-trail reserved event for unclassified state changes", "MANUAL", "ORDER", "2a", "OrderEvents"),
 ("MEVE", "Venue Event", "Venue-side event related to order lifecycle", "ROUTING", "ORDER", "2a", "OrderEvents"),
]

SCHEMA = StructType([
 StructField("cat_event_type_code", StringType, False),
 StructField("cat_event_type_name", StringType, False),
 StructField("cat_event_type_description", StringType, True),
 StructField("category", StringType, True),
 StructField("lifecycle_stage", StringType, True),
 StructField("phase", StringType, True),
 StructField("submission_file_type", StringType, True),
])

@dlt.view(name="v_cat_event_type_seed")
def v_cat_event_type_seed:
 df = spark.createDataFrame(CAT_EVENTS, schema=SCHEMA)
 return (
 df.withColumn("effective_start_date", F.current_date).withColumn("effective_end_date", F.lit("9999-12-31").cast(DateType)).withColumn("is_active", F.lit(True)).withColumn("source_authority", F.lit("FINRA_CAT")).withColumn("source_version", F.lit(CAT_SPEC_VERSION)).withColumn("last_updated_date", F.current_date).withColumn("record_source", F.lit(CAT_SPEC_URL)).withColumn("load_date", F.current_timestamp).withColumn("cdc_sequence", F.col("load_date").cast("long")).withColumn("cdc_operation", F.lit("UPSERT")).withColumn(
 "row_hash",
 F.sha2(
 F.concat_ws("|",
 F.col("cat_event_type_code"),
 F.col("cat_event_type_name"),
 F.coalesce(F.col("category"), F.lit("")),
 F.coalesce(F.col("lifecycle_stage"), F.lit("")),
 F.coalesce(F.col("phase"), F.lit("")),
 F.coalesce(F.col("submission_file_type"), F.lit("")),
),
 256,
),
)
)

@dlt.expect_all_or_fail({
 "code_required": "cat_event_type_code IS NOT NULL",
 "name_required": "cat_event_type_name IS NOT NULL",
 "code_alpha_3_10": "cat_event_type_code RLIKE '^[A-Z_]{3,12}$'",
 "category_enum": "category IN ('ORDER_REQUEST','ORDER','ROUTING','MODIFICATION','EXECUTION','ALLOCATION','QUOTE','COMBINED_ORDER','REPRESENTATIVE_ORDER','MANUAL')",
 "phase_enum": "phase IN ('2a','2b','2c','2d','2e')",
 "submission_file_type_enum": "submission_file_type IN ('OrderEvents','QuoteEvents','Allocations','CAIS','OptionsExtensions')",
})
@dlt.view(name="v_cat_event_type_validated")
def v_cat_event_type_validated:
 return dlt.read("v_cat_event_type_seed")

dlt.create_streaming_table(
 name="ref_cat_event_type",
 comment="FINRA CAT 50-event taxonomy - manual seed per FINRA release",
 table_properties={
 "delta.enableChangeDataFeed": "true",
 "subject_area": "reference",
 "scd_type": "2",
 "cat_spec_version": CAT_SPEC_VERSION,
 },
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_cat_event_type",
 source="v_cat_event_type_validated",
 keys=["cat_event_type_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 track_history_column_list=[
 "cat_event_type_name", "cat_event_type_description",
 "category", "lifecycle_stage", "phase",
 "submission_file_type", "row_hash",
 ],
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)
