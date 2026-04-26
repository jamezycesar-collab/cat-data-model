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

"""SCD2 dim_trader pipeline. Trader natural key and employment/registration attributes via apply_changes."""
import dlt
from pyspark.sql import functions as F

SILVER_DB = spark.conf.get("silver_database", "silver")

@dlt.view(name="v_trader_cdc")
def v_trader_cdc:
 hub = spark.table(f"{SILVER_DB}.hub_trader")
 pit = spark.table(f"{SILVER_DB}.pit_trader")
 sd = spark.table(f"{SILVER_DB}.sat_trader_details")
 sr = spark.table(f"{SILVER_DB}.sat_trader_registration")

 return (
 pit.alias("pit").join(hub.alias("h"), F.col("pit.trader_hk") == F.col("h.trader_hk")).join(sd.alias("sd"), F.col("pit.sat_trader_details_pk") == F.col("sd.sat_trader_details_pk")).join(sr.alias("sr"), F.col("pit.sat_trader_registration_pk") == F.col("sr.sat_trader_registration_pk"), "left").select(
 F.col("h.trader_id_bk").alias("trader_id_bk"),
 F.col("sr.cat_trader_id").alias("cat_trader_id"),
 F.col("sd.given_name_hash").alias("given_name_hash"),
 F.col("sd.family_name_hash").alias("family_name_hash"),
 F.col("sd.desk_id_bk").alias("desk_id_bk"),
 F.col("sd.active_flag").alias("active_flag"),
 F.col("pit.load_date_effective").alias("effective_start_date"),
 F.col("pit.cdc_operation").alias("cdc_operation"),
 F.col("pit.cdc_sequence").alias("cdc_sequence"),
 F.col("h.trader_hk").alias("dv2_source_hk"),
).withColumn(
 "row_hash",
 F.sha2(
 F.concat_ws("|",
 F.coalesce(F.col("cat_trader_id"), F.lit("")),
 F.coalesce(F.col("given_name_hash"), F.lit("")),
 F.coalesce(F.col("family_name_hash"), F.lit("")),
 F.coalesce(F.col("desk_id_bk"), F.lit("")),
 F.coalesce(F.col("active_flag").cast("string"), F.lit("")),
),
 256,
),
).withColumn("record_source", F.lit(f"{SILVER_DB}.hub_trader")).withColumn("load_date", F.current_timestamp)
)

dlt.create_streaming_table(
 name="dim_trader",
 comment="Gold SCD2 trader dimension - PII hashed",
 table_properties={
 "delta.enableChangeDataFeed": "true",
 "scd_type": "2",
 "subject_area": "gold_dim",
 "pii_classification": "HASHED",
 },
 partition_cols=["is_current"],
)

@dlt.expect_all_or_fail(
 {
 "trader_id_bk_not_null": "trader_id_bk IS NOT NULL",
 "row_hash_sha256_len": "length(row_hash) = 64",
 "given_name_pii_hashed":
 "given_name_hash IS NULL OR length(given_name_hash) = 64",
 "family_name_pii_hashed":
 "family_name_hash IS NULL OR length(family_name_hash) = 64",
 }
)
@dlt.view(name="v_trader_cdc_validated")
def v_trader_cdc_validated:
 return dlt.read_stream("v_trader_cdc")

dlt.apply_changes(
 target="dim_trader",
 source="v_trader_cdc_validated",
 keys=["trader_id_bk"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 track_history_column_list=[
 "cat_trader_id", "given_name_hash", "family_name_hash",
 "desk_id_bk", "active_flag", "row_hash", "dv2_source_hk",
 ],
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)
