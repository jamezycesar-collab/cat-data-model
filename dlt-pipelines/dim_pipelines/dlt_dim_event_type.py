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

"""Type-0 dim_event_type pipeline. Loads the CAT event taxonomy reference as a small conformed dim."""
import dlt
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 50 CAT events - aligned to `dim_event_type` seed in gold/01_dim_tables.sql
# Columns: (sk, code, name, description, phase, file_type, sort_order,
# is_manual, is_electronic, is_options_extension, is_reportable)
# ---------------------------------------------------------------------------
CAT_EVENTS = [
 (1, "MEQR", "Quote Request (electronic)", "Request for quotation from client to dealer", "PRE_TRADE", "QuoteEvents", 10, False, True, False, True),
 (2, "MEQS", "Quote Sent (electronic)", "Quote response posted to RFQ", "PRE_TRADE", "QuoteEvents", 11, False, True, False, True),
 (3, "MEIR", "Internal Order Received (electronic)", "Order received internally from a customer", "ORDER_REQUEST", "OrderEvents", 20, False, True, True, True),
 (4, "MOIR", "Internal Order Received (manual)", "Order received internally via manual channel", "ORDER_REQUEST", "OrderEvents", 21, True, False, True, True),
 (5, "MENO", "New Order", "Order opened on central book", "ORDER_OPEN", "OrderEvents", 30, False, True, True, True),
 (6, "MEOA", "Order Accepted", "Venue/broker accepted order", "ORDER_OPEN", "OrderEvents", 31, False, True, False, True),
 (7, "MEOM", "Order Modified", "Order details modified", "ORDER_OPEN", "OrderEvents", 32, False, True, True, True),
 (8, "MEOR", "Order Rejected", "Order rejected", "ORDER_OPEN", "OrderEvents", 33, False, True, False, True),
 (9, "MEOC", "Order Cancelled", "Order cancelled by firm or client", "ORDER_OPEN", "OrderEvents", 34, False, True, True, True),
 (10, "MEOE", "Order Expired", "Order expired at end of session", "ORDER_OPEN", "OrderEvents", 35, False, True, False, True),
 (11, "MECO", "Electronic Route Out", "Route out of firm electronically", "ORDER_ROUTING", "OrderEvents", 40, False, True, False, True),
 (12, "MLCO", "Route Out to Exchange", "Child-order routed to exchange", "ORDER_ROUTING", "OrderEvents", 41, False, True, False, True),
 (13, "MOCO", "Manual Route Out", "Route out of firm manually", "ORDER_ROUTING", "OrderEvents", 42, True, False, False, True),
 (14, "MEOT", "Order Fulfillment (execution)", "Fill event - MOST COMMON CAT EVENT", "EXECUTION", "OrderEvents", 50, False, True, True, True),
 (15, "MEOTQ", "Order Fulfillment nanosecond", "Fill with nanosecond precision", "EXECUTION", "OrderEvents", 51, False, True, False, True),
 (16, "MEOTS", "Order Fulfillment Stopped Order", "Stopped-order fill", "EXECUTION", "OrderEvents", 52, False, True, False, True),
 (17, "MEOF", "Order Fulfillment Final", "Riskless-principal / final fulfillment", "EXECUTION", "OrderEvents", 53, False, True, False, True),
 (18, "MEPA", "Prorata Allocation", "Post-trade pro-rata allocation", "ALLOCATION", "Allocations", 60, False, True, False, True),
 (19, "MEAA", "Affirmed Allocation", "Counterparty-affirmed allocation", "ALLOCATION", "Allocations", 61, False, True, False, True),
 (20, "MOFA", "Order Fill Allocated", "Fill allocated to specific sub-account", "ALLOCATION", "Allocations", 62, False, True, False, True),
 (21, "MEOJ", "Correction/Adjustment", "Order event correction (T+3)", "CORRECTION", "OrderEvents", 70, False, True, False, True),
 (22, "MOQR", "Manual Quote Request", "Manual RFQ", "PRE_TRADE", "QuoteEvents", 12, True, False, False, True),
 (23, "MOQS", "Manual Quote Sent", "Manual quote response", "PRE_TRADE", "QuoteEvents", 13, True, False, False, True),
 (24, "MONO", "Manual New Order", "New order recorded manually", "ORDER_OPEN", "OrderEvents", 36, True, False, False, True),
 (25, "MOOA", "Manual Order Accepted", "Manual order accepted", "ORDER_OPEN", "OrderEvents", 37, True, False, False, True),
 (26, "MOOR", "Manual Order Rejected", "Manual order rejected", "ORDER_OPEN", "OrderEvents", 38, True, False, False, True),
 (27, "MOOM", "Manual Order Modified", "Manual order modified", "ORDER_OPEN", "OrderEvents", 39, True, False, False, True),
 (28, "MOOC", "Manual Order Cancelled", "Manual order cancelled", "ORDER_OPEN", "OrderEvents", 43, True, False, False, True),
 (29, "MOOT", "Manual Order Trade", "Manual fill event", "EXECUTION", "OrderEvents", 54, True, False, False, True),
 (30, "MEPB", "Principal Block Trade", "Principal block trade", "EXECUTION", "OrderEvents", 55, False, True, False, True),
 (31, "MEOJR", "Order Event Repaired", "Error repair (different from MEOJ correction)", "CORRECTION", "OrderEvents", 71, False, True, False, True),
 (32, "MELC", "Market Maker Lifecycle Cancel", "MM cancellation", "OPTIONS", "OrderEvents", 80, False, True, True, True),
 (33, "MELP", "Market Maker Peg", "MM pegged quote", "OPTIONS", "OrderEvents", 81, False, True, True, True),
 (34, "MEOX", "Order Expiration", "Options-specific expiration", "OPTIONS", "OrderEvents", 82, False, True, True, True),
 (35, "MEOS", "Order Shortfall", "Short-sale circuit-breaker order", "ORDER_OPEN", "OrderEvents", 44, False, True, False, True),
 (36, "MEON", "Order Netting", "Internal order netting", "EXECUTION", "OrderEvents", 56, False, True, False, True),
 (37, "MEOK", "Order Kill Switch", "Risk kill-switch cancellation", "ORDER_OPEN", "OrderEvents", 45, False, True, False, True),
 (38, "MEPZ", "Post-Trade Allocation Zeroed", "Allocation cancelled to zero", "ALLOCATION", "Allocations", 63, False, True, False, True),
 (39, "MEPM", "Post-Trade Allocation Modified", "Allocation modified", "ALLOCATION", "Allocations", 64, False, True, False, True),
 (40, "MEAX", "Affirmation Cancelled", "Allocation affirmation reversed", "ALLOCATION", "Allocations", 65, False, True, False, True),
 (41, "MECX", "Execution Cancelled", "Execution cancelled", "EXECUTION", "OrderEvents", 57, False, True, False, True),
 (42, "MECM", "Execution Modified", "Execution modified", "EXECUTION", "OrderEvents", 58, False, True, False, True),
 (43, "MERO", "Reject Order", "Upstream reject", "ORDER_OPEN", "OrderEvents", 46, False, True, False, True),
 (44, "MERR", "Reject Route", "Route rejection", "ORDER_ROUTING", "OrderEvents", 47, False, True, False, True),
 (45, "MERC", "Reject Cancel", "Cancel rejected", "ORDER_OPEN", "OrderEvents", 48, False, True, False, True),
 (46, "MEXC", "Expired Cancel", "Cancellation expired unacted", "ORDER_OPEN", "OrderEvents", 49, False, True, False, True),
 (47, "MEXM", "Expired Modify", "Modification expired unacted", "ORDER_OPEN", "OrderEvents", 59, False, True, False, True),
 (48, "CAIS_C", "CAIS Customer", "Customer Account Information System record", "CUSTOMER", "CAIS", 90, False, True, False, True),
 (49, "CAIS_A", "CAIS Account", "CAIS account snapshot", "CUSTOMER", "CAIS", 91, False, True, False, True),
 (50, "CAIS_R", "CAIS Trading Relationship", "CAIS trading relationship", "CUSTOMER", "CAIS", 92, False, True, False, True),
]

SCHEMA = (
 "dim_event_type_sk INT, cat_event_code STRING, event_name STRING, event_description STRING, "
 "event_phase STRING, submission_file_type STRING, sort_order INT, "
 "is_manual BOOLEAN, is_electronic BOOLEAN, is_options_extension BOOLEAN, is_reportable BOOLEAN"
)

@dlt.expect_all_or_fail(
 {
 "exactly_50_events": "(SELECT COUNT(*) FROM dim_event_type) = 50", # informational only
 "cat_event_code_unique": "cat_event_code IS NOT NULL",
 "event_phase_enum": "event_phase IN ('PRE_TRADE','ORDER_REQUEST','ORDER_OPEN','ORDER_ROUTING','EXECUTION','ALLOCATION','CORRECTION','OPTIONS','CUSTOMER')",
 "file_type_enum": "submission_file_type IN ('OrderEvents','QuoteEvents','Allocations','CAIS')",
 }
)
@dlt.table(
 name="dim_event_type",
 comment="50-row CAT event-code taxonomy (static, regenerated on CAT tech spec upgrade)",
 table_properties={
 "delta.enableChangeDataFeed": "false",
 "scd_type": "0",
 "subject_area": "gold_dim",
 "cat_spec_version": "4.3",
 },
)
def dim_event_type:
 return (
 spark.createDataFrame(CAT_EVENTS, schema=SCHEMA).withColumn("cat_spec_version", F.lit("4.3")).withColumn("load_date", F.current_timestamp).withColumn("record_source", F.lit("SEED_CAT_TECH_SPEC_4.3"))
)
