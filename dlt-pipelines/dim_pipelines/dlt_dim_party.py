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

"""SCD2 dim_party pipeline. Reads the party hub and satellites, applies SCD2 via apply_changes, enforces the expected set of natural-key columns."""
import dlt
from pyspark.sql import functions as F

SILVER_DB = spark.conf.get("silver_database", "silver")

# ---------------------------------------------------------------------------
# Step 1 - curate view of current & versioned party details from Silver
# ---------------------------------------------------------------------------
@dlt.view(
 name="v_party_cdc",
 comment="Curated Silver view producing SCD2-ready CDC stream for dim_party",
)
def v_party_cdc:
 hub = spark.table(f"{SILVER_DB}.hub_party")
 pit = spark.table(f"{SILVER_DB}.pit_party")
 sd = spark.table(f"{SILVER_DB}.sat_party_details")
 sc = spark.table(f"{SILVER_DB}.sat_party_cat_ids")
 sr = spark.table(f"{SILVER_DB}.sat_party_regulatory")

 return (
 pit.alias("pit").join(hub.alias("h"), F.col("pit.party_hk") == F.col("h.party_hk")).join(sd.alias("sd"),
 F.col("pit.sat_party_details_pk") == F.col("sd.sat_party_details_pk")).join(sc.alias("sc"),
 F.col("pit.sat_party_cat_ids_pk") == F.col("sc.sat_party_cat_ids_pk"),
 "left").join(sr.alias("sr"),
 F.col("pit.sat_party_regulatory_pk") == F.col("sr.sat_party_regulatory_pk"),
 "left").select(
 F.col("h.party_id_bk").alias("party_id_bk"),
 F.col("sd.lei").alias("lei"),
 F.col("sc.cat_imid").alias("cat_imid"),
 F.col("sc.crd").alias("crd"),
 F.col("sc.fdid").alias("fdid"),
 F.col("sd.party_type").alias("party_type"),
 F.col("sd.party_status").alias("party_status"),
 F.col("sd.legal_form").alias("legal_form"),
 F.col("sd.incorporation_country").alias("incorporation_country"),
 F.col("sd.domicile_country").alias("domicile_country"),
 F.col("sd.tax_residency").alias("tax_residency"),
 F.col("sr.ultimate_parent_party_id").alias("ultimate_parent_party_id"),
 F.col("sr.g_sib_flag").alias("g_sib_flag"),
 F.col("sd.pep_flag").alias("pep_flag"),
 F.col("sd.mifid_ii_category").alias("mifid_ii_category"),
 F.col("pit.load_date_effective").alias("effective_start_date"),
 F.col("pit.cdc_operation").alias("cdc_operation"),
 F.col("pit.cdc_sequence").alias("cdc_sequence"),
 F.col("h.party_hk").alias("dv2_source_hk"),
).withColumn(
 "row_hash",
 F.sha2(
 F.concat_ws(
 "|",
 F.coalesce(F.col("lei"), F.lit("")),
 F.coalesce(F.col("cat_imid"), F.lit("")),
 F.coalesce(F.col("crd"), F.lit("")),
 F.coalesce(F.col("fdid"), F.lit("")),
 F.col("party_type"),
 F.coalesce(F.col("party_status"), F.lit("")),
 F.coalesce(F.col("legal_form"), F.lit("")),
 F.coalesce(F.col("incorporation_country"),F.lit("")),
 F.coalesce(F.col("domicile_country"), F.lit("")),
 F.coalesce(F.col("tax_residency"), F.lit("")),
 F.coalesce(F.col("ultimate_parent_party_id"), F.lit("")),
 F.coalesce(F.col("g_sib_flag").cast("string"), F.lit("")),
 F.coalesce(F.col("pep_flag").cast("string"), F.lit("")),
 F.coalesce(F.col("mifid_ii_category"), F.lit("")),
),
 256,
),
).withColumn("record_source", F.lit(f"{SILVER_DB}.hub_party")).withColumn("load_date", F.current_timestamp)
)

# ---------------------------------------------------------------------------
# Step 2 - SCD2 target (DLT apply_changes generates effective_start_date /
# effective_end_date / is_current automatically)
# ---------------------------------------------------------------------------
dlt.create_streaming_table(
 name="dim_party",
 comment="Gold SCD2 party dimension",
 table_properties={
 "delta.enableChangeDataFeed": "true",
 "scd_type": "2",
 "subject_area": "gold_dim",
 },
 partition_cols=["is_current"],
)

# Hard-fail expectations (block pipeline on violation)
@dlt.expect_all_or_fail(
 {
 "party_id_bk_not_null": "party_id_bk IS NOT NULL",
 "party_type_not_null": "party_type IS NOT NULL",
 "row_hash_sha256_len": "length(row_hash) = 64",
 }
)
@dlt.view(name="v_party_cdc_validated", comment="Hard-fail DQ gate upstream of apply_changes")
def v_party_cdc_validated:
 return dlt.read_stream("v_party_cdc")

dlt.apply_changes(
 target="dim_party",
 source="v_party_cdc_validated",
 keys=["party_id_bk"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 track_history_column_list=[
 "lei", "cat_imid", "crd", "fdid", "party_type", "party_status",
 "legal_form", "incorporation_country", "domicile_country", "tax_residency",
 "ultimate_parent_party_id", "g_sib_flag", "pep_flag", "mifid_ii_category",
 "row_hash", "dv2_source_hk",
 ],
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)
