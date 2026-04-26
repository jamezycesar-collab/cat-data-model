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

"""Ingest industry classifications, tax treaty references, and credit-rating agencies into their reference tables."""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

SEED_ROOT = "abfss://onelake@cat-prod.dfs.fabric.microsoft.com/ref/seeds"

def _audit(df, authority: str, seed: str, version: str):
 return (
 df.withColumn("effective_start_date",
 F.coalesce(F.col("effective_start_date"), F.current_date)).withColumn("effective_end_date",
 F.coalesce(F.col("effective_end_date"),
 F.lit("9999-12-31").cast(DateType))).withColumn("is_active", F.coalesce(F.col("is_active"), F.lit(True))).withColumn("source_authority", F.lit(authority)).withColumn("source_version", F.lit(version)).withColumn("last_updated_date", F.current_date).withColumn("record_source", F.lit(f"{SEED_ROOT}/{seed}")).withColumn("load_date", F.current_timestamp).withColumn("cdc_sequence", F.col("load_date").cast("long")).withColumn("cdc_operation", F.lit("UPSERT"))
)

def _hash_cols(cols):
 return F.sha2(
 F.concat_ws("|",
 *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]),
 256,
)

# ===========================================================================
# 1. ref_settlement_venue
# ===========================================================================
@dlt.view(name="v_settlement_venue_curated")
def v_settlement_venue_curated:
 raw = (
 spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaLocation", f"{SEED_ROOT}/_schemas/settlement_venues/").option("header", "true").load(f"{SEED_ROOT}/settlement_venues/")
)
 return (
 raw.transform(lambda df: _audit(df, "INTERNAL_TAXONOMY",
 "settlement_venues.csv", "2026.1")).withColumn("row_hash", _hash_cols([
 "settlement_venue_code", "settlement_venue_name", "category",
 "country_code", "currency_code", "bic_code",
 "supports_dvp", "supports_fop", "supports_cns",
 "t_plus_standard", "operating_hours_utc",
 ]))
)

@dlt.expect_all_or_fail({
 "code_not_null": "settlement_venue_code IS NOT NULL",
 "bic_valid_when_present": "bic_code IS NULL OR bic_code RLIKE '^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$'",
 "t_plus_nonneg": "t_plus_standard IS NULL OR t_plus_standard >= 0",
 "category_enum": "category IS NULL OR category IN ('CSD','ICSD','CCP','CASH_CSD')",
})
@dlt.view(name="v_settlement_venue_validated")
def v_settlement_venue_validated:
 return dlt.read_stream("v_settlement_venue_curated")

dlt.create_streaming_table(
 name="ref_settlement_venue",
 comment="CSDs, ICSDs, CCPs - semi-annual refresh",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_settlement_venue",
 source="v_settlement_venue_validated",
 keys=["settlement_venue_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)

# ===========================================================================
# 2. ref_regulator
# ===========================================================================
@dlt.view(name="v_regulator_curated")
def v_regulator_curated:
 raw = (
 spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaLocation", f"{SEED_ROOT}/_schemas/regulators/").option("header", "true").load(f"{SEED_ROOT}/regulators/")
)
 return (
 raw.transform(lambda df: _audit(df, "INTERNAL_TAXONOMY",
 "regulators.csv", "2026.1")).withColumn("row_hash", _hash_cols([
 "regulator_code", "regulator_name", "category",
 "country_code", "jurisdiction_scope",
 "primary_statute", "reporting_regimes", "lei_required",
 ]))
)

@dlt.expect_all_or_fail({
 "regulator_upper": "regulator_code RLIKE '^[A-Z_]{2,20}$'",
 "jurisdiction_enum": "jurisdiction_scope IS NULL OR jurisdiction_scope IN ('NATIONAL','FEDERAL','STATE','SUPRANATIONAL')",
})
@dlt.view(name="v_regulator_validated")
def v_regulator_validated:
 return dlt.read_stream("v_regulator_curated")

dlt.create_streaming_table(
 name="ref_regulator",
 comment="Global regulators - semi-annual refresh",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_regulator",
 source="v_regulator_validated",
 keys=["regulator_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)

# ===========================================================================
# 3. ref_credit_rating_scale
# ===========================================================================
@dlt.view(name="v_rating_curated")
def v_rating_curated:
 raw = (
 spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaLocation", f"{SEED_ROOT}/_schemas/rating_scales/").option("header", "true").load(f"{SEED_ROOT}/rating_scales/")
)
 return (
 raw.transform(lambda df: _audit(df, "RATING_AGENCY_COMPOSITE",
 "rating_scales.csv", "2026-Q1")).withColumn("row_hash", _hash_cols([
 "credit_rating_code", "credit_rating_name",
 "rating_agency", "agency_scale",
 "numeric_equivalent", "basel_risk_weight",
 "default_probability", "is_investment_grade",
 ]))
)

@dlt.expect_all_or_fail({
 "rating_agency_enum": "rating_agency IN ('SP','MOODYS','FITCH','DBRS','JCR','R_AND_I','SCOPE','KROLL')",
 "agency_scale_enum": "agency_scale IN ('LONG_TERM','SHORT_TERM','ISSUER','ISSUE','RECOVERY')",
 "default_prob_range": "default_probability IS NULL OR default_probability BETWEEN 0 AND 1",
 "basel_weight_range": "basel_risk_weight IS NULL OR basel_risk_weight BETWEEN 0 AND 1.5",
})
@dlt.view(name="v_rating_validated")
def v_rating_validated:
 return dlt.read_stream("v_rating_curated")

dlt.create_streaming_table(
 name="ref_credit_rating_scale",
 comment="S&P / Moody's / Fitch / DBRS credit rating scales - quarterly refresh",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_credit_rating_scale",
 source="v_rating_validated",
 keys=["credit_rating_code", "rating_agency", "agency_scale"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)

# ===========================================================================
# 4. ref_tax_jurisdiction
# ===========================================================================
@dlt.view(name="v_tax_curated")
def v_tax_curated:
 raw = (
 spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaLocation", f"{SEED_ROOT}/_schemas/tax_jurisdictions/").option("header", "true").load(f"{SEED_ROOT}/tax_jurisdictions/")
)
 return (
 raw.transform(lambda df: _audit(df, "OECD_IRS",
 "tax_jurisdictions.csv", "2026")).withColumn("row_hash", _hash_cols([
 "tax_jurisdiction_code", "tax_jurisdiction_name",
 "category", "country_code", "subdivision_code",
 "tax_authority", "default_withholding_rate",
 "treaty_withholding_rate", "fatca_participant",
 "crs_participant", "is_treaty_jurisdiction",
 ]))
)

@dlt.expect_all_or_fail({
 "country_code_iso3166": "country_code RLIKE '^[A-Z]{2}$'",
 "default_withhold_range": "default_withholding_rate IS NULL OR default_withholding_rate BETWEEN 0 AND 1",
 "treaty_withhold_range": "treaty_withholding_rate IS NULL OR treaty_withholding_rate BETWEEN 0 AND 1",
 "category_enum": "category IS NULL OR category IN ('FEDERAL','STATE','PROVINCIAL','MUNICIPAL','SUPRANATIONAL')",
})
@dlt.view(name="v_tax_validated")
def v_tax_validated:
 return dlt.read_stream("v_tax_curated")

dlt.create_streaming_table(
 name="ref_tax_jurisdiction",
 comment="Tax jurisdictions + withholding treaty rates - annual refresh",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_tax_jurisdiction",
 source="v_tax_validated",
 keys=["tax_jurisdiction_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)
