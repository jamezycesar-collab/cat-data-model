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

"""Ingest ISO 3166-1 country codes, ISO 4217 currency codes, and ISO 10383 MIC codes into their respective reference tables."""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
 StringType, BooleanType, IntegerType, DateType, TimestampType, StructField, StructType
)

SILVER_DB = spark.conf.get("silver_database", "silver")
GOLD_DB = spark.conf.get("gold_database", "gold")

BRONZE_ROOT = "abfss://onelake@cat-prod.dfs.fabric.microsoft.com/ref/bronze/iso"

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _sha256_key_cols(cols) -> F.Column:
 """SHA-256(concat_ws('|', coalesce(col,'') for col in cols))."""
 return F.sha2(
 F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]),
 256,
)

def _with_audit(df, authority: str, source_path: str):
 return (
 df.withColumn("effective_end_date", F.lit("9999-12-31").cast(DateType)).withColumn("is_active", F.lit(True)).withColumn("source_authority", F.lit(authority)).withColumn("record_source", F.lit(source_path)).withColumn("load_date", F.current_timestamp()).withColumn("cdc_sequence", F.col("load_date").cast("long")).withColumn("cdc_operation",
 F.when(F.col("status") == F.lit("DELETED"), F.lit("DELETE")).otherwise(F.lit("UPSERT")))
)

# ===========================================================================
# 1. ref_mic (ISO 10383)
# ===========================================================================
@dlt.view(name="v_mic_raw")
def v_mic_raw():
 return (
 spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaLocation", f"{BRONZE_ROOT}/mic/_schemas/").option("header", "true").load(f"{BRONZE_ROOT}/mic/")
)

@dlt.view(name="v_mic_curated")
def v_mic_curated():
 return (
 dlt.read_stream("v_mic_raw").select(
 F.upper(F.col("MIC")).alias("mic_code"),
 F.col("MARKET NAME-INSTITUTION DESCRIPTION").alias("mic_name"),
 F.col("LEGAL ENTITY NAME").alias("mic_description"),
 F.col("MARKET CATEGORY CODE").alias("market_category_code"),
 F.col("OPERATING MIC").alias("operating_mic"),
 F.when(F.col("MIC").eqNullSafe(F.col("OPERATING MIC")), F.lit("OPRT")).otherwise(F.lit("SGMT")).alias("category"),
 F.col("ACRONYM").alias("acronym"),
 F.upper(F.col("ISO COUNTRY CODE (ISO 3166)")).alias("country_code"),
 F.col("CITY").alias("city"),
 F.col("WEBSITE").alias("website_url"),
 F.upper(F.col("STATUS")).alias("status"),
 F.to_date(F.col("LAST UPDATE DATE"), "yyyyMMdd").alias("last_updated_date"),
 F.coalesce(F.to_date(F.col("CREATION DATE"), "yyyyMMdd"),
 F.current_date).alias("effective_start_date"),
 F.lit("ISO 10383 2026-03").alias("source_version"),
).transform(lambda df: _with_audit(df, "ISO_10383", f"{BRONZE_ROOT}/mic")).withColumn("row_hash", _sha256_key_cols([
 "mic_code", "mic_name", "operating_mic", "status",
 "country_code", "category", "market_category_code",
 ]))
)

@dlt.expect_all_or_fail({
 "mic_code_4_upper_alnum": "mic_code RLIKE '^[A-Z0-9]{4}$'",
 "status_enum": "status IS NULL OR status IN ('ACTIVE','DELETED','MODIFIED','EXPIRED')",
 "country_code_iso3166": "country_code IS NULL OR country_code RLIKE '^[A-Z]{2}$'",
 "effective_start_required":"effective_start_date IS NOT NULL",
})
@dlt.view(name="v_mic_validated")
def v_mic_validated():
 return dlt.read_stream("v_mic_curated")

dlt.create_streaming_table(
 name="ref_mic",
 comment="ISO 10383 Market Identifier Codes - monthly refresh",
 table_properties={
 "delta.enableChangeDataFeed": "true",
 "subject_area": "reference",
 "scd_type": "2",
 },
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_mic",
 source="v_mic_validated",
 keys=["mic_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 track_history_column_list=[
 "mic_name", "mic_description", "category", "operating_mic",
 "market_category_code", "acronym", "country_code", "city",
 "website_url", "status", "row_hash",
 ],
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)

# ===========================================================================
# 2. ref_country (ISO 3166-1)
# ===========================================================================
@dlt.view(name="v_country_curated")
def v_country_curated():
 raw = (
 spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaLocation", f"{BRONZE_ROOT}/country/_schemas/").option("header", "true").load(f"{BRONZE_ROOT}/country/")
)
 return (
 raw.select(
 F.upper(F.col("alpha2")).alias("country_code"),
 F.col("name").alias("country_name"),
 F.col("official_name").alias("country_description"),
 F.col("region").alias("category"),
 F.upper(F.col("alpha3")).alias("alpha_3_code"),
 F.col("numeric").alias("numeric_code"),
 F.col("iso_region").alias("iso_region"),
 F.col("un_region").alias("un_region"),
 F.col("is_eu").cast(BooleanType).alias("is_eu_member"),
 F.col("is_oecd").cast(BooleanType).alias("is_oecd_member"),
 F.col("is_fatf").cast(BooleanType).alias("is_fatf_member"),
 F.col("is_sanctioned").cast(BooleanType).alias("is_sanctioned"),
 F.upper(F.col("primary_currency")).alias("primary_currency"),
 F.current_date.alias("last_updated_date"),
 F.current_date.alias("effective_start_date"),
 F.lit("ISO 3166-1:2020").alias("source_version"),
 F.lit("ACTIVE").alias("status"),
).transform(lambda df: _with_audit(df, "ISO_3166", f"{BRONZE_ROOT}/country")).withColumn("row_hash", _sha256_key_cols([
 "country_code", "country_name", "category", "alpha_3_code",
 "numeric_code", "is_sanctioned", "primary_currency",
 ]))
)

@dlt.expect_all_or_fail({
 "country_code_2_alpha": "country_code RLIKE '^[A-Z]{2}$'",
 "alpha_3_valid": "alpha_3_code IS NULL OR alpha_3_code RLIKE '^[A-Z]{3}$'",
 "numeric_code_3_digits": "numeric_code IS NULL OR numeric_code RLIKE '^[0-9]{3}$'",
 "category_enum": "category IS NULL OR category IN ('AMERICAS','EMEA','APAC','AFRICA')",
})
@dlt.view(name="v_country_validated")
def v_country_validated():
 return dlt.read_stream("v_country_curated")

dlt.create_streaming_table(
 name="ref_country",
 comment="ISO 3166-1 country codes - quarterly refresh",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_country",
 source="v_country_validated",
 keys=["country_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)

# ===========================================================================
# 3. ref_currency (ISO 4217)
# ===========================================================================
@dlt.view(name="v_currency_curated")
def v_currency_curated():
 raw = (
 spark.readStream.format("cloudFiles").option("cloudFiles.format", "xml").option("rowTag", "CcyNtry").option("cloudFiles.schemaLocation", f"{BRONZE_ROOT}/currency/_schemas/").load(f"{BRONZE_ROOT}/currency/")
)
 return (
 raw.select(
 F.upper(F.col("Ccy")).alias("currency_code"),
 F.col("CcyNm").alias("currency_name"),
 F.col("CcyNm").alias("currency_description"),
 F.lit("FIAT").alias("category"),
 F.col("CcyNbr").alias("numeric_code"),
 F.col("CcyMnrUnts").cast(IntegerType).alias("minor_unit"),
 F.lit(None).cast(StringType).alias("symbol"),
 F.upper(F.col("CtryNm")).substr(1, 2).alias("country_code"),
 F.lit(False).alias("is_g10"),
 F.lit(False).alias("is_cls_eligible"),
 F.lit(False).alias("is_crypto"),
 F.current_date.alias("last_updated_date"),
 F.current_date.alias("effective_start_date"),
 F.lit("ISO 4217 2025-06").alias("source_version"),
 F.lit("ACTIVE").alias("status"),
).transform(lambda df: _with_audit(df, "ISO_4217", f"{BRONZE_ROOT}/currency")).withColumn("row_hash", _sha256_key_cols([
 "currency_code", "currency_name", "minor_unit", "numeric_code", "category",
 ]))
)

@dlt.expect_all_or_fail({
 "currency_code_3_alpha": "currency_code RLIKE '^[A-Z]{3}$'",
 "minor_unit_valid": "minor_unit IS NULL OR minor_unit BETWEEN 0 AND 8",
 "category_enum": "category IS NULL OR category IN ('FIAT','CRYPTO','PRECIOUS_METAL','FUND','OTHER')",
})
@dlt.view(name="v_currency_validated")
def v_currency_validated():
 return dlt.read_stream("v_currency_curated")

dlt.create_streaming_table(
 name="ref_currency",
 comment="ISO 4217 currency codes - quarterly refresh",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_currency",
 source="v_currency_validated",
 keys=["currency_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)

# ===========================================================================
# 4. ref_cfi_category (ISO 10962)
# ===========================================================================
@dlt.view(name="v_cfi_curated")
def v_cfi_curated():
 raw = (
 spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaLocation", f"{BRONZE_ROOT}/cfi/_schemas/").option("header", "true").load(f"{BRONZE_ROOT}/cfi/")
)
 return (
 raw.select(
 F.upper(F.col("cfi_code")).alias("cfi_code"),
 F.col("name").alias("cfi_name"),
 F.col("description").alias("cfi_description"),
 F.substring(F.col("cfi_code"), 1, 1).alias("category"),
 F.col("subcategory").alias("subcategory"),
 F.col("attr_1").alias("attribute_1"),
 F.col("attr_2").alias("attribute_2"),
 F.col("attr_3").alias("attribute_3"),
 F.col("attr_4").alias("attribute_4"),
 F.current_date.alias("last_updated_date"),
 F.current_date.alias("effective_start_date"),
 F.lit("ISO 10962:2019").alias("source_version"),
 F.lit("ACTIVE").alias("status"),
).transform(lambda df: _with_audit(df, "ISO_10962", f"{BRONZE_ROOT}/cfi")).withColumn("row_hash", _sha256_key_cols([
 "cfi_code", "cfi_name", "subcategory",
 "attribute_1", "attribute_2", "attribute_3", "attribute_4",
 ]))
)

@dlt.expect_all_or_fail({
 "cfi_code_6_chars": "cfi_code RLIKE '^[A-Z-]{6}$'",
 "category_prefix_valid": "category IS NULL OR category IN ('E','D','R','O','F','S','H','I','J','K','L','M','T')",
})
@dlt.view(name="v_cfi_validated")
def v_cfi_validated():
 return dlt.read_stream("v_cfi_curated")

dlt.create_streaming_table(
 name="ref_cfi_category",
 comment="ISO 10962 CFI codes - annual refresh",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_cfi_category",
 source="v_cfi_validated",
 keys=["cfi_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)

# ===========================================================================
# 5. ref_entity_legal_form (ISO 20275 / GLEIF)
# ===========================================================================
@dlt.view(name="v_elf_curated")
def v_elf_curated():
 raw = (
 spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaLocation", f"{BRONZE_ROOT}/elf/_schemas/").option("header", "true").load(f"{BRONZE_ROOT}/elf/")
)
 return (
 raw.select(
 F.upper(F.col("ELF Code")).alias("elf_code"),
 F.col("Entity Legal Form name Local name Transliterated").alias("elf_name"),
 F.col("Entity Legal Form Abbreviation Local name").alias("abbreviation"),
 F.lit("CORPORATE").alias("category"),
 F.upper(F.col("Country Code (ISO 3166-1)")).alias("country_code"),
 F.col("Entity Legal Form Name (Local Name)").alias("local_name"),
 F.upper(F.col("Language Code (ISO 639-1)")).alias("local_name_language"),
 F.col("Country Of Formation").alias("elf_description"),
 F.lit(False).alias("is_regulated_entity"),
 F.current_date.alias("last_updated_date"),
 F.current_date.alias("effective_start_date"),
 F.lit("GLEIF 2026-Q1").alias("source_version"),
 F.col("ELF Status ACTV/INAC").alias("status"),
).filter(F.col("elf_code").isNotNull).transform(lambda df: _with_audit(df, "ISO_20275", f"{BRONZE_ROOT}/elf")).withColumn("row_hash", _sha256_key_cols([
 "elf_code", "elf_name", "abbreviation", "country_code", "status",
 ]))
)

@dlt.expect_all_or_fail({
 "elf_code_4_alnum": "elf_code RLIKE '^[A-Z0-9]{4}$'",
 "country_code_iso3166": "country_code IS NULL OR country_code RLIKE '^[A-Z]{2}$'",
})
@dlt.view(name="v_elf_validated")
def v_elf_validated():
 return dlt.read_stream("v_elf_curated")

dlt.create_streaming_table(
 name="ref_entity_legal_form",
 comment="ISO 20275 / GLEIF Entity Legal Forms - monthly refresh",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_entity_legal_form",
 source="v_elf_validated",
 keys=["elf_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)
