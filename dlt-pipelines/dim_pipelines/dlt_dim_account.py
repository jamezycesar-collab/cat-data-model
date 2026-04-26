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

"""SCD2 dim_account pipeline. Account attributes (CAIS, customer type, AP) via apply_changes."""
import dlt
from pyspark.sql import functions as F

SILVER_DB = spark.conf.get("silver_database", "silver")

@dlt.view(name="v_account_cdc")
def v_account_cdc:
 hub = spark.table(f"{SILVER_DB}.hub_account")
 pit = spark.table(f"{SILVER_DB}.pit_account")
 sd = spark.table(f"{SILVER_DB}.sat_account_details")
 sc = spark.table(f"{SILVER_DB}.sat_account_custodian")

 return (
 pit.alias("pit").join(hub.alias("h"), F.col("pit.account_hk") == F.col("h.account_hk")).join(sd.alias("sd"), F.col("pit.sat_account_details_pk") == F.col("sd.sat_account_details_pk")).join(sc.alias("sc"), F.col("pit.sat_account_custodian_pk") == F.col("sc.sat_account_custodian_pk"), "left").select(
 F.col("h.account_id_bk").alias("account_id_bk"),
 F.col("sd.account_type").alias("account_type"),
 F.col("sd.account_status").alias("account_status"),
 F.col("sc.custodian_id").alias("custodian_id"),
 F.col("sc.clearing_member_id").alias("clearing_member_id"),
 F.col("sd.cat_large_trader_flag").alias("cat_large_trader_flag"),
 F.col("pit.load_date_effective").alias("effective_start_date"),
 F.col("pit.cdc_operation").alias("cdc_operation"),
 F.col("pit.cdc_sequence").alias("cdc_sequence"),
 F.col("h.account_hk").alias("dv2_source_hk"),
).withColumn(
 "row_hash",
 F.sha2(
 F.concat_ws("|",
 F.coalesce(F.col("account_type"), F.lit("")),
 F.coalesce(F.col("account_status"), F.lit("")),
 F.coalesce(F.col("custodian_id"), F.lit("")),
 F.coalesce(F.col("clearing_member_id"), F.lit("")),
 F.coalesce(F.col("cat_large_trader_flag").cast("string"), F.lit("")),
),
 256,
),
).withColumn("record_source", F.lit(f"{SILVER_DB}.hub_account")).withColumn("load_date", F.current_timestamp)
)

dlt.create_streaming_table(
 name="dim_account",
 comment="Gold SCD2 account dimension",
 table_properties={
 "delta.enableChangeDataFeed": "true",
 "scd_type": "2",
 "subject_area": "gold_dim",
 },
 partition_cols=["is_current"],
)

@dlt.expect_all_or_fail(
 {
 "account_id_bk_not_null": "account_id_bk IS NOT NULL",
 "row_hash_sha256_len": "length(row_hash) = 64",
 }
)
@dlt.view(name="v_account_cdc_validated")
def v_account_cdc_validated:
 return dlt.read_stream("v_account_cdc")

dlt.apply_changes(
 target="dim_account",
 source="v_account_cdc_validated",
 keys=["account_id_bk"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 track_history_column_list=[
 "account_type", "account_status", "custodian_id", "clearing_member_id",
 "cat_large_trader_flag", "row_hash", "dv2_source_hk",
 ],
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)
