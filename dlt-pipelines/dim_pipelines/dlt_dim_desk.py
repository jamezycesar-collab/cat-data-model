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

"""SCD2 dim_desk pipeline. Internal desk hierarchy tracked via apply_changes."""
import dlt
from pyspark.sql import functions as F

SILVER_DB = spark.conf.get("silver_database", "silver")

@dlt.view(name="v_desk_cdc")
def v_desk_cdc():
 hub = spark.table(f"{SILVER_DB}.hub_desk")
 pit = spark.table(f"{SILVER_DB}.pit_desk")
 sd = spark.table(f"{SILVER_DB}.sat_desk_details")

 return (
 pit.alias("pit").join(hub.alias("h"), F.col("pit.desk_hk") == F.col("h.desk_hk")).join(sd.alias("sd"), F.col("pit.sat_desk_details_pk") == F.col("sd.sat_desk_details_pk")).select(
 F.col("h.desk_id_bk").alias("desk_id_bk"),
 F.col("sd.desk_name").alias("desk_name"),
 F.col("sd.desk_hierarchy").alias("desk_hierarchy"),
 F.col("sd.region").alias("region"),
 F.col("sd.asset_class_coverage").alias("asset_class_coverage"),
 F.col("pit.load_date_effective").alias("effective_start_date"),
 F.col("pit.cdc_operation").alias("cdc_operation"),
 F.col("pit.cdc_sequence").alias("cdc_sequence"),
 F.col("h.desk_hk").alias("dv2_source_hk"),
).withColumn(
 "row_hash",
 F.sha2(
 F.concat_ws("|",
 F.coalesce(F.col("desk_name"), F.lit("")),
 F.coalesce(F.col("desk_hierarchy"), F.lit("")),
 F.coalesce(F.col("region"), F.lit("")),
 F.coalesce(F.col("asset_class_coverage"), F.lit("")),
),
 256,
),
).withColumn("record_source", F.lit(f"{SILVER_DB}.hub_desk")).withColumn("load_date", F.current_timestamp())
)

dlt.create_streaming_table(
 name="dim_desk",
 comment="Gold SCD2 desk dimension",
 table_properties={
 "delta.enableChangeDataFeed": "true",
 "scd_type": "2",
 "subject_area": "gold_dim",
 },
 partition_cols=["is_current"],
)

@dlt.expect_all_or_fail(
 {
 "desk_id_bk_not_null": "desk_id_bk IS NOT NULL",
 "row_hash_sha256_len": "length(row_hash) = 64",
 }
)
@dlt.view(name="v_desk_cdc_validated")
def v_desk_cdc_validated():
 return dlt.read_stream("v_desk_cdc")

dlt.apply_changes(
 target="dim_desk",
 source="v_desk_cdc_validated",
 keys=["desk_id_bk"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 track_history_column_list=[
 "desk_name", "desk_hierarchy", "region", "asset_class_coverage",
 "row_hash", "dv2_source_hk",
 ],
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)
