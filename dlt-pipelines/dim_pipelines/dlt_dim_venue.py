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

"""SCD2 dim_venue pipeline. Venue attributes (MIC, operating MIC, market segment) tracked via apply_changes."""
import dlt
from pyspark.sql import functions as F

SILVER_DB = spark.conf.get("silver_database", "silver")

@dlt.view(name="v_venue_cdc")
def v_venue_cdc():
 hub = spark.table(f"{SILVER_DB}.hub_venue")
 pit = spark.table(f"{SILVER_DB}.pit_venue")
 sd = spark.table(f"{SILVER_DB}.sat_venue_details")

 return (
 pit.alias("pit").join(hub.alias("h"), F.col("pit.venue_hk") == F.col("h.venue_hk")).join(sd.alias("sd"), F.col("pit.sat_venue_details_pk") == F.col("sd.sat_venue_details_pk")).select(
 F.col("h.venue_id_bk").alias("venue_id_bk"),
 F.col("sd.mic").alias("mic"),
 F.col("sd.venue_name").alias("venue_name"),
 F.col("sd.venue_category").alias("venue_category"),
 F.col("sd.country_code").alias("country_code"),
 F.col("sd.operating_segment").alias("operating_segment"),
 F.col("pit.load_date_effective").alias("effective_start_date"),
 F.col("pit.cdc_operation").alias("cdc_operation"),
 F.col("pit.cdc_sequence").alias("cdc_sequence"),
 F.col("h.venue_hk").alias("dv2_source_hk"),
).withColumn(
 "row_hash",
 F.sha2(
 F.concat_ws("|",
 F.coalesce(F.col("mic"), F.lit("")),
 F.coalesce(F.col("venue_name"), F.lit("")),
 F.coalesce(F.col("venue_category"), F.lit("")),
 F.coalesce(F.col("country_code"), F.lit("")),
 F.coalesce(F.col("operating_segment"),F.lit("")),
),
 256,
),
).withColumn("record_source", F.lit(f"{SILVER_DB}.hub_venue")).withColumn("load_date", F.current_timestamp())
)

dlt.create_streaming_table(
 name="dim_venue",
 comment="Gold SCD2 venue dimension (ISO 10383 MIC)",
 table_properties={
 "delta.enableChangeDataFeed": "true",
 "scd_type": "2",
 "subject_area": "gold_dim",
 },
 partition_cols=["is_current"],
)

@dlt.expect_all_or_fail(
 {
 "venue_id_bk_not_null": "venue_id_bk IS NOT NULL",
 "row_hash_sha256_len": "length(row_hash) = 64",
 "mic_format": "mic IS NULL OR length(mic) = 4",
 "country_code_format": "country_code IS NULL OR length(country_code) = 2",
 }
)
@dlt.view(name="v_venue_cdc_validated")
def v_venue_cdc_validated():
 return dlt.read_stream("v_venue_cdc")

dlt.apply_changes(
 target="dim_venue",
 source="v_venue_cdc_validated",
 keys=["venue_id_bk"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 track_history_column_list=[
 "mic", "venue_name", "venue_category", "country_code",
 "operating_segment", "row_hash", "dv2_source_hk",
 ],
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)
