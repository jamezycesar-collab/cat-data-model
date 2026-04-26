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

"""DLT fact pipeline for CAIS customer records. Writes the CAIS snapshot fact table with point-in-time joins to party and account dims."""
import dlt
from pyspark.sql import functions as F

SILVER_DB = spark.conf.get("silver_database", "silver")
GOLD_DB = spark.conf.get("gold_database", "gold")

@dlt.view(name="v_cais_curated")
def v_cais_curated:
 lnk = spark.readStream.table(f"{SILVER_DB}.link_customer_record")
 sat = spark.table(f"{SILVER_DB}.sat_customer_record_details")

 return (
 lnk.alias("l").join(sat.alias("s"), F.col("l.customer_link_hk") == F.col("s.customer_link_hk")).select(
 F.col("s.firm_roe_id").alias("firm_roe_id"),
 F.col("s.fdid").alias("fdid"),
 F.col("s.cat_event_code").alias("cat_event_code"),
 F.col("l.party_id_bk").alias("party_id_bk"),
 F.col("l.account_id_bk").alias("account_id_bk"),
 F.col("s.record_type").alias("record_type"),
 F.col("s.action_type").alias("action_type"),
 F.col("s.customer_type").alias("customer_type"),
 F.col("s.natural_person_flag").alias("natural_person_flag"),
 F.col("s.primary_identifier_type").alias("primary_identifier_type"),
 F.col("s.primary_identifier_hash").alias("primary_identifier_hash"),
 F.col("s.birth_year").alias("birth_year"),
 F.col("s.residence_country").alias("residence_country"),
 F.col("s.postal_code").alias("postal_code"),
 F.col("s.account_type").alias("account_type"),
 F.col("s.account_status").alias("account_status"),
 F.col("s.account_open_date").alias("account_open_date"),
 F.col("s.account_close_date").alias("account_close_date"),
 F.col("s.trading_authorization").alias("trading_authorization"),
 F.col("s.authorized_trader_fdid").alias("authorized_trader_fdid"),
 F.col("s.relationship_start_date").alias("relationship_start_date"),
 F.col("s.relationship_end_date").alias("relationship_end_date"),
 F.col("s.large_trader_flag").alias("large_trader_flag"),
 F.col("s.large_trader_id").alias("large_trader_id"),
 F.col("s.effective_start_date").alias("effective_start_date"),
 F.col("s.snapshot_date").alias("snapshot_date"),
 F.col("s.snapshot_date").alias("event_date"),
 F.col("s.source_system").alias("source_system"),
 F.col("l.customer_link_hk").alias("dv2_source_hk"),
 F.current_timestamp.alias("load_date"),
 F.lit(f"{SILVER_DB}.link_customer_record").alias("record_source"),
)
)

@dlt.expect_all_or_fail(
 {
 "firm_roe_id_required": "firm_roe_id IS NOT NULL",
 "fdid_required": "fdid IS NOT NULL",
 "dv2_source_hk_required": "dv2_source_hk IS NOT NULL",
 "cat_event_code_cais_enum": "cat_event_code IN ('CAIS_C','CAIS_A','CAIS_R')",
 "record_type_required": "record_type IS NOT NULL",
 "action_type_enum": "action_type IN ('INSERT','UPDATE','DELETE')",
 "pii_hash_length":
 "primary_identifier_hash IS NULL OR length(primary_identifier_hash) = 64",
 "birth_year_reasonable":
 "birth_year IS NULL OR birth_year BETWEEN 1900 AND year(current_date)",
 "residence_country_iso":
 "residence_country IS NULL OR length(residence_country) = 2",
 "postal_code_privacy_length":
 "postal_code IS NULL OR length(postal_code) = 5",
 }
)
@dlt.view(name="v_cais_validated")
def v_cais_validated:
 return dlt.read_stream("v_cais_curated")

@dlt.view(name="v_cais_enriched")
def v_cais_enriched:
 c = dlt.read_stream("v_cais_validated")
 dim_party = spark.table(f"{GOLD_DB}.dim_party")
 dim_account = spark.table(f"{GOLD_DB}.dim_account")

 return (
 c.alias("c").join(
 dim_party.alias("p"),
 (F.col("c.party_id_bk") == F.col("p.party_id_bk"))
 & (F.col("c.snapshot_date").between(F.col("p.effective_start_date"), F.col("p.effective_end_date"))),
 "left",
).join(
 dim_account.alias("a"),
 (F.col("c.account_id_bk") == F.col("a.account_id_bk"))
 & (F.col("c.snapshot_date").between(F.col("a.effective_start_date"), F.col("a.effective_end_date"))),
 "left",
).select(
 "c.firm_roe_id", "c.fdid", "c.cat_event_code",
 F.col("p.dim_party_sk").alias("dim_party_sk"),
 F.col("a.dim_account_sk").alias("dim_account_sk"),
 "c.record_type", "c.action_type",
 "c.customer_type", "c.natural_person_flag",
 "c.primary_identifier_type", "c.primary_identifier_hash",
 "c.birth_year", "c.residence_country", "c.postal_code",
 "c.account_type", "c.account_status",
 "c.account_open_date", "c.account_close_date",
 "c.trading_authorization", "c.authorized_trader_fdid",
 "c.relationship_start_date", "c.relationship_end_date",
 "c.large_trader_flag", "c.large_trader_id",
 "c.effective_start_date", "c.snapshot_date", "c.event_date",
 "c.dv2_source_hk", "c.source_system", "c.load_date", "c.record_source",
)
)

@dlt.expect_all_or_fail(
 {
 "sk_party_resolved": "dim_party_sk IS NOT NULL",
 "sk_account_resolved": "dim_account_sk IS NOT NULL",
 }
)
@dlt.table(
 name="fact_cat_customer_records",
 comment="CAIS daily-snapshot fact - one row per CAIS record per snapshot_date",
 partition_cols=["snapshot_date"],
 table_properties={
 "delta.enableChangeDataFeed": "true",
 "delta.autoOptimize.optimizeWrite": "true",
 "subject_area": "gold_fact",
 "cat_submission_file_type": "CAIS",
 "grain": "one row per CAIS record per snapshot",
 "pii_classification": "HASHED",
 },
)
def fact_cat_customer_records:
 return dlt.read_stream("v_cais_enriched")
