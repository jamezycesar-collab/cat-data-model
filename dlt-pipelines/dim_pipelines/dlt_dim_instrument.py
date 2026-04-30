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

"""SCD2 dim_instrument pipeline. Tracks instrument attributes across time via apply_changes against the instrument satellite."""
import dlt
from pyspark.sql import functions as F

SILVER_DB = spark.conf.get("silver_database", "silver")

@dlt.view(name="v_instrument_cdc")
def v_instrument_cdc():
 hub = spark.table(f"{SILVER_DB}.hub_instrument")
 pit = spark.table(f"{SILVER_DB}.pit_instrument")
 sd = spark.table(f"{SILVER_DB}.sat_instrument_details")
 si = spark.table(f"{SILVER_DB}.sat_instrument_identifiers")
 sc = spark.table(f"{SILVER_DB}.sat_instrument_classification")

 return (
 pit.alias("pit").join(hub.alias("h"), F.col("pit.instrument_hk") == F.col("h.instrument_hk")).join(sd.alias("sd"), F.col("pit.sat_instrument_details_pk") == F.col("sd.sat_instrument_details_pk")).join(si.alias("si"), F.col("pit.sat_instrument_identifiers_pk") == F.col("si.sat_instrument_identifiers_pk"), "left").join(sc.alias("sc"), F.col("pit.sat_instrument_classification_pk") == F.col("sc.sat_instrument_classification_pk"), "left").select(
 F.col("h.instrument_id_bk").alias("instrument_id_bk"),
 F.col("si.isin").alias("isin"),
 F.col("si.cusip").alias("cusip"),
 F.col("si.figi").alias("figi"),
 F.col("sd.symbol").alias("symbol"),
 F.coalesce(F.col("si.isin"), F.col("si.cusip"), F.col("si.figi")).alias("security_id"),
 F.when(F.col("si.isin").isNotNull, F.lit("ISIN")).when(F.col("si.cusip").isNotNull, F.lit("CUSIP")).when(F.col("si.figi").isNotNull, F.lit("FIGI")).otherwise(F.lit(None)).alias("security_id_source"),
 F.col("sc.asset_class").alias("asset_class"),
 F.col("sc.cfi_code").alias("cfi_code"),
 F.col("sd.issuer_party_id").alias("issuer_party_id"),
 F.col("sd.currency_code").alias("currency_code"),
 F.col("sd.primary_listing_mic").alias("primary_listing_mic"),
 F.col("pit.load_date_effective").alias("effective_start_date"),
 F.col("pit.cdc_operation").alias("cdc_operation"),
 F.col("pit.cdc_sequence").alias("cdc_sequence"),
 F.col("h.instrument_hk").alias("dv2_source_hk"),
).withColumn(
 "row_hash",
 F.sha2(
 F.concat_ws(
 "|",
 F.coalesce(F.col("isin"), F.lit("")),
 F.coalesce(F.col("cusip"), F.lit("")),
 F.coalesce(F.col("figi"), F.lit("")),
 F.coalesce(F.col("symbol"), F.lit("")),
 F.coalesce(F.col("asset_class"), F.lit("")),
 F.coalesce(F.col("cfi_code"), F.lit("")),
 F.coalesce(F.col("issuer_party_id"), F.lit("")),
 F.coalesce(F.col("currency_code"), F.lit("")),
 F.coalesce(F.col("primary_listing_mic"),F.lit("")),
),
 256,
),
).withColumn("record_source", F.lit(f"{SILVER_DB}.hub_instrument")).withColumn("load_date", F.current_timestamp())
)

dlt.create_streaming_table(
 name="dim_instrument",
 comment="Gold SCD2 instrument dimension",
 table_properties={
 "delta.enableChangeDataFeed": "true",
 "scd_type": "2",
 "subject_area": "gold_dim",
 },
 partition_cols=["is_current"],
)

@dlt.expect_all_or_fail(
 {
 "instrument_id_bk_not_null": "instrument_id_bk IS NOT NULL",
 "row_hash_sha256_len": "length(row_hash) = 64",
 "security_id_source_enum":
 "security_id_source IS NULL OR security_id_source IN ('ISIN','CUSIP','FIGI')",
 }
)
@dlt.view(name="v_instrument_cdc_validated")
def v_instrument_cdc_validated():
 return dlt.read_stream("v_instrument_cdc")

dlt.apply_changes(
 target="dim_instrument",
 source="v_instrument_cdc_validated",
 keys=["instrument_id_bk"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 track_history_column_list=[
 "isin", "cusip", "figi", "symbol", "security_id", "security_id_source",
 "asset_class", "cfi_code", "issuer_party_id", "currency_code",
 "primary_listing_mic", "row_hash", "dv2_source_hk",
 ],
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)
