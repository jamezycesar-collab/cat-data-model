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

"""Ingest FIX protocol enumerations (order type, time-in-force, handling instructions) into their reference tables."""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
 StructType, StructField, StringType, BooleanType, DateType
)

FIX_VERSION = "FIX 4.4 SP2"
FIX_URL = "https://www.fixtrading.org/standards/fix-4-4/"

# ===========================================================================
# Shared helpers
# ===========================================================================
def _audit(df, authority: str, subpath: str):
 return (
 df.withColumn("effective_start_date", F.current_date).withColumn("effective_end_date", F.lit("9999-12-31").cast(DateType)).withColumn("is_active", F.lit(True)).withColumn("source_authority", F.lit(authority)).withColumn("source_version", F.lit(FIX_VERSION)).withColumn("last_updated_date", F.current_date).withColumn("record_source", F.lit(f"{FIX_URL}#{subpath}")).withColumn("load_date", F.current_timestamp).withColumn("cdc_sequence", F.col("load_date").cast("long")).withColumn("cdc_operation", F.lit("UPSERT"))
)

# ===========================================================================
# 1. ref_order_type - FIX tag 40 OrdType
# ===========================================================================
ORDER_TYPES = [
 # (code, name, description, category, fix_value, requires_price, requires_stop, is_algo)
 ("MARKET", "Market", "Buy/sell at best available price", "SIMPLE", "1", False, False, False),
 ("LIMIT", "Limit", "Buy at or below / sell at or above a specified limit", "SIMPLE", "2", True, False, False),
 ("STOP", "Stop", "Becomes a market order when stop trigger is hit", "CONDITIONAL", "3", False, True, False),
 ("STOP_LIMIT", "Stop Limit", "Becomes a limit order when stop trigger is hit", "CONDITIONAL", "4", True, True, False),
 ("MARKET_ON_CLOSE", "Market On Close", "Executes at closing-auction price", "SIMPLE", "5", False, False, False),
 ("WITH_OR_WITHOUT", "With or Without", "Limit order executable with or without quote", "CONDITIONAL", "6", True, False, False),
 ("LIMIT_OR_BETTER", "Limit or Better", "Limit order with permission to improve", "SIMPLE", "7", True, False, False),
 ("LIMIT_WITH_OR_WITHOUT", "Limit With or Without", "Limit OR executable w/o a quote", "CONDITIONAL", "8", True, False, False),
 ("ON_BASIS", "On Basis", "Tied to the basis market", "CONDITIONAL", "9", True, False, False),
 ("ON_CLOSE", "On Close", "Execute at close", "SIMPLE", "A", False, False, False),
 ("LIMIT_ON_CLOSE", "Limit On Close", "Limit at closing auction", "SIMPLE", "B", True, False, False),
 ("FOREX_MARKET", "Forex Market", "FX market order", "SIMPLE", "C", False, False, False),
 ("PREVIOUSLY_QUOTED", "Previously Quoted", "Execute against previously quoted price", "PEGGED", "D", True, False, False),
 ("PREVIOUSLY_INDICATED", "Previously Indicated", "Execute against previously indicated IOI", "PEGGED", "E", True, False, False),
 ("FOREX_LIMIT", "Forex Limit", "FX limit order", "SIMPLE", "F", True, False, False),
 ("FOREX_SWAP", "Forex Swap", "FX swap", "SIMPLE", "G", True, False, False),
 ("FOREX_PREVIOUSLY_QUOTED", "Forex Previously Quoted", "FX previously quoted", "PEGGED", "H", True, False, False),
 ("FUNARI", "Funari", "Japanese GTC-with-market-on-close conversion", "CONDITIONAL", "I", True, False, False),
 ("MARKET_IF_TOUCHED", "Market If Touched", "Becomes market when touched", "CONDITIONAL", "J", False, True, False),
 ("MARKET_WITH_LEFTOVER_LIMIT", "Market with Leftover as Limit","Market for what can fill, rest becomes limit", "CONDITIONAL", "K", True, False, False),
 ("PREVIOUS_FUND_VALUATION", "Previous Fund Valuation", "Fund trade at previous NAV", "SIMPLE", "L", True, False, False),
 ("NEXT_FUND_VALUATION", "Next Fund Valuation", "Fund trade at next NAV", "SIMPLE", "M", True, False, False),
 ("PEGGED", "Pegged", "Peg to best bid / offer", "PEGGED", "P", False, False, False),
 ("COUNTER_ORDER_SELECTION", "Counter Order Selection", "Counter order selection", "DISCRETIONARY", "Q", True, False, False),
 ("ALGO", "Algo (firm-defined)", "Parent algorithmic order (VWAP, TWAP, IS, etc.)", "ALGO", "Z", True, False, True),
]

ORDER_TYPE_SCHEMA = StructType([
 StructField("order_type_code", StringType, False),
 StructField("order_type_name", StringType, False),
 StructField("order_type_description", StringType, True),
 StructField("category", StringType, True),
 StructField("fix_tag_40_value", StringType, True),
 StructField("requires_price", BooleanType, True),
 StructField("requires_stop_price", BooleanType, True),
 StructField("is_algo_order", BooleanType, True),
])

@dlt.view(name="v_order_type_seed")
def v_order_type_seed:
 df = spark.createDataFrame(ORDER_TYPES, schema=ORDER_TYPE_SCHEMA)
 return (
 df.transform(lambda d: _audit(d, "FIX_PROTOCOL", "tag40")).withColumn("row_hash",
 F.sha2(F.concat_ws("|",
 F.col("order_type_code"),
 F.col("order_type_name"),
 F.coalesce(F.col("category"), F.lit("")),
 F.coalesce(F.col("fix_tag_40_value"), F.lit("")),
 F.col("requires_price").cast("string"),
 F.col("requires_stop_price").cast("string"),
 F.col("is_algo_order").cast("string"),
), 256))
)

@dlt.expect_all_or_fail({
 "code_not_null": "order_type_code IS NOT NULL",
 "name_not_null": "order_type_name IS NOT NULL",
 "fix_tag_40_valid": "fix_tag_40_value IS NULL OR fix_tag_40_value RLIKE '^[A-Z0-9]{1,3}$'",
 "category_enum": "category IS NULL OR category IN ('SIMPLE','CONDITIONAL','PEGGED','ALGO','DISCRETIONARY')",
})
@dlt.view(name="v_order_type_validated")
def v_order_type_validated:
 return dlt.read("v_order_type_seed")

dlt.create_streaming_table(
 name="ref_order_type",
 comment="FIX tag 40 OrdType - seeded per FIX release",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_order_type",
 source="v_order_type_validated",
 keys=["order_type_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)

# ===========================================================================
# 2. ref_time_in_force - FIX tag 59 TimeInForce
# ===========================================================================
TIF_VALUES = [
 # (code, name, description, category, fix_value, requires_expire, auto_cancels_intraday)
 ("DAY", "Day", "Valid for the trading day; cancels at session close", "SESSION", "0", False, True),
 ("GTC", "Good Till Cancel", "Open until the client cancels", "OPEN_ENDED", "1", False, False),
 ("OPG", "At the Opening", "Executes only at market open; otherwise cancels", "SESSION", "2", False, True),
 ("IOC", "Immediate or Cancel", "Execute immediately any part that can fill; cancel rest", "SESSION", "3", False, True),
 ("FOK", "Fill or Kill", "Fill entirely or cancel", "SESSION", "4", False, True),
 ("GTX", "Good Till Crossing", "Good until end of the crossing session", "SESSION", "5", False, True),
 ("GTD", "Good Till Date", "Good until an explicit expire date (ExpireDate / tag 432)", "CONDITIONAL", "6", True, False),
 ("CLO", "At the Close", "Executes only at closing auction", "SESSION", "7", False, True),
]

TIF_SCHEMA = StructType([
 StructField("tif_code", StringType, False),
 StructField("tif_name", StringType, False),
 StructField("tif_description", StringType, True),
 StructField("category", StringType, True),
 StructField("fix_tag_59_value", StringType, True),
 StructField("requires_expire_date", BooleanType, True),
 StructField("auto_cancels_intraday", BooleanType, True),
])

@dlt.view(name="v_tif_seed")
def v_tif_seed:
 df = spark.createDataFrame(TIF_VALUES, schema=TIF_SCHEMA)
 return (
 df.transform(lambda d: _audit(d, "FIX_PROTOCOL", "tag59")).withColumn("row_hash",
 F.sha2(F.concat_ws("|",
 F.col("tif_code"), F.col("tif_name"),
 F.coalesce(F.col("category"), F.lit("")),
 F.coalesce(F.col("fix_tag_59_value"), F.lit("")),
 F.col("requires_expire_date").cast("string"),
 F.col("auto_cancels_intraday").cast("string"),
), 256))
)

@dlt.expect_all_or_fail({
 "tif_code_upper": "tif_code RLIKE '^[A-Z_]{2,20}$'",
 "fix_tag_59_numeric": "fix_tag_59_value RLIKE '^[0-9]{1,2}$'",
})
@dlt.view(name="v_tif_validated")
def v_tif_validated:
 return dlt.read("v_tif_seed")

dlt.create_streaming_table(
 name="ref_time_in_force",
 comment="FIX tag 59 TimeInForce - 8 canonical values",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_time_in_force",
 source="v_tif_validated",
 keys=["tif_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)

# ===========================================================================
# 3. ref_handling_instruction - FIX tag 21 HandlInst
# ===========================================================================
HANDLING = [
 # (code, name, description, category, fix_value, manual_intervention, triggers_moot)
 ("AUTOMATED_PRIVATE", "Automated, Private", "Automated execution order - private, no broker intervention", "AUTOMATED", "1", False, False),
 ("AUTOMATED_PUBLIC", "Automated, Public", "Automated execution order - public, broker intervention OK", "AUTOMATED", "2", False, False),
 ("MANUAL", "Manual", "Manual order - broker intervention required; CAT triggers MOOT/MOFA/MEMA", "MANUAL", "3", True, True),
]

HANDLING_SCHEMA = StructType([
 StructField("handling_instruction_code", StringType, False),
 StructField("handling_instruction_name", StringType, False),
 StructField("handling_instruction_description", StringType, True),
 StructField("category", StringType, True),
 StructField("fix_tag_21_value", StringType, True),
 StructField("requires_manual_intervention", BooleanType, True),
 StructField("triggers_cat_moot", BooleanType, True),
])

@dlt.view(name="v_handling_seed")
def v_handling_seed:
 df = spark.createDataFrame(HANDLING, schema=HANDLING_SCHEMA)
 return (
 df.transform(lambda d: _audit(d, "FIX_PROTOCOL", "tag21")).withColumn("row_hash",
 F.sha2(F.concat_ws("|",
 F.col("handling_instruction_code"),
 F.col("handling_instruction_name"),
 F.coalesce(F.col("category"), F.lit("")),
 F.coalesce(F.col("fix_tag_21_value"), F.lit("")),
 F.col("requires_manual_intervention").cast("string"),
 F.col("triggers_cat_moot").cast("string"),
), 256))
)

@dlt.expect_all_or_fail({
 "handling_upper": "handling_instruction_code RLIKE '^[A-Z_]{3,40}$'",
 "fix_tag_21_enum": "fix_tag_21_value IN ('1','2','3')",
 "category_enum": "category IN ('AUTOMATED','MANUAL','WORKED')",
})
@dlt.view(name="v_handling_validated")
def v_handling_validated:
 return dlt.read("v_handling_seed")

dlt.create_streaming_table(
 name="ref_handling_instruction",
 comment="FIX tag 21 HandlInst - 3 canonical values",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_handling_instruction",
 source="v_handling_validated",
 keys=["handling_instruction_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)
