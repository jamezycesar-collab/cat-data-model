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

"""Ingest firm-internal desk and risk-framework taxonomies into the firm reference tables."""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
 StructType, StructField, StringType, BooleanType, DateType,
)

FIRM_VERSION = "2026.1"
FIRM_URL = "internal://taxonomies/data-governance/2026.1"

# ===========================================================================
# 1. ref_party_role
# ===========================================================================
PARTY_ROLES = [
 # (code, name, description, category, capacity, requires_lei, requires_crd, is_regulated, fix_452)
 ("CUSTOMER", "Customer", "End-customer / retail or institutional buyer-side client", "INVESTOR", "AGENCY", False, False, False, "3"),
 ("PROSPECTIVE_CUSTOMER", "Prospective Customer", "Onboarding prospective customer (pre-KYC)", "INVESTOR", "AGENCY", False, False, False, "3"),
 ("INVESTMENT_MANAGER", "Investment Manager", "Registered investment adviser acting for customer accounts", "INTERMEDIARY", "FIDUCIARY", True, False, True, "11"),
 ("BROKER_DEALER", "Broker-Dealer", "FINRA-registered broker-dealer", "INTERMEDIARY", "PRINCIPAL", True, True, True, "1"),
 ("PRIME_BROKER", "Prime Broker", "Prime brokerage service provider (financing / clearing)", "INTERMEDIARY", "PRINCIPAL", True, True, True, "1"),
 ("EXECUTING_BROKER", "Executing Broker", "Broker executing trades for another broker / investment manager", "INTERMEDIARY", "AGENCY", True, True, True, "1"),
 ("INTRODUCING_BROKER", "Introducing Broker", "Introducing firm relying on clearing broker", "INTERMEDIARY", "AGENCY", True, True, True, "14"),
 ("CLEARING_BROKER", "Clearing Broker", "Clearing-only broker-dealer", "INTERMEDIARY", "PRINCIPAL", True, True, True, "4"),
 ("MARKET_MAKER", "Market Maker", "Registered market-maker posting two-sided quotes", "INTERMEDIARY", "PRINCIPAL", True, True, True, "66"),
 ("DEALER", "Dealer", "OTC dealer taking principal risk", "INTERMEDIARY", "PRINCIPAL", True, True, True, "1"),
 ("SWAP_DEALER", "Swap Dealer", "CFTC-registered swap dealer", "INTERMEDIARY", "PRINCIPAL", True, True, True, "1"),
 ("SECURITIES_BASED_SWAP_DEALER", "Security-Based Swap Dealer", "SEC-registered security-based swap dealer", "INTERMEDIARY", "PRINCIPAL", True, True, True, "1"),
 ("CUSTODIAN", "Custodian", "Asset custodian (bank or trust company)", "INFRASTRUCTURE", "FIDUCIARY", True, False, True, "24"),
 ("CCP", "Central Counterparty", "CCP (NSCC, OCC, LCH, CME, ICE, etc.)", "INFRASTRUCTURE", "PRINCIPAL", True, False, True, "62"),
 ("CSD", "Central Securities Depository", "Central securities depository (DTCC, Euroclear, Clearstream)", "INFRASTRUCTURE", "FIDUCIARY", True, False, True, "38"),
 ("CLEARING_MEMBER", "Clearing Member", "Direct clearing member of a CCP", "INTERMEDIARY", "PRINCIPAL", True, True, True, "66"),
 ("ISSUER", "Issuer", "Primary issuer of a security", "ISSUER", "PRINCIPAL", True, False, False, "38"),
 ("GUARANTOR", "Guarantor", "Guarantor on a debt / derivative contract", "ISSUER", "PRINCIPAL", True, False, False, "38"),
 ("REGULATOR", "Regulator", "Regulatory authority (SEC, FINRA, CFTC, ESMA, etc.)", "REGULATOR", "FIDUCIARY", True, False, True, "44"),
 ("TRADE_REPOSITORY", "Trade Repository", "Swap / trade reporting repository (DTCC GTR, CME, ICE TR)", "INFRASTRUCTURE", "FIDUCIARY", True, False, True, "44"),
 ("AFFIRMATION_PLATFORM", "Affirmation Platform", "Post-trade affirmation platform (CTM, OASYS)", "INFRASTRUCTURE", "AGENCY", False, False, False, "83"),
 ("CLEARING_ORGANIZATION", "Clearing Organization", "Generic clearing organization (CAT reporting)", "INFRASTRUCTURE", "PRINCIPAL", True, False, True, "62"),
]

PARTY_ROLE_SCHEMA = StructType([
 StructField("party_role_code", StringType, False),
 StructField("party_role_name", StringType, False),
 StructField("party_role_description", StringType, True),
 StructField("category", StringType, True),
 StructField("regulatory_capacity", StringType, True),
 StructField("requires_lei", BooleanType, True),
 StructField("requires_crd", BooleanType, True),
 StructField("is_regulated_role", BooleanType, True),
 StructField("fix_party_role_value", StringType, True),
])

@dlt.view(name="v_party_role_seed")
def v_party_role_seed():
 df = spark.createDataFrame(PARTY_ROLES, schema=PARTY_ROLE_SCHEMA)
 return (
 df.withColumn("effective_start_date", F.current_date).withColumn("effective_end_date", F.lit("9999-12-31").cast(DateType)).withColumn("is_active", F.lit(True)).withColumn("source_authority", F.lit("INTERNAL_TAXONOMY")).withColumn("source_version", F.lit(FIRM_VERSION)).withColumn("last_updated_date", F.current_date).withColumn("record_source", F.lit(FIRM_URL)).withColumn("load_date", F.current_timestamp()).withColumn("cdc_sequence", F.col("load_date").cast("long")).withColumn("cdc_operation", F.lit("UPSERT")).withColumn("row_hash",
 F.sha2(F.concat_ws("|",
 F.col("party_role_code"), F.col("party_role_name"),
 F.coalesce(F.col("category"), F.lit("")),
 F.coalesce(F.col("regulatory_capacity"), F.lit("")),
 F.col("requires_lei").cast("string"),
 F.col("requires_crd").cast("string"),
 F.col("is_regulated_role").cast("string"),
 F.coalesce(F.col("fix_party_role_value"), F.lit("")),
), 256))
)

@dlt.expect_all_or_fail({
 "party_role_upper": "party_role_code RLIKE '^[A-Z_]{3,40}$'",
 "category_enum": "category IN ('ISSUER','INVESTOR','INTERMEDIARY','INFRASTRUCTURE','REGULATOR')",
 "regulatory_capacity_enum": "regulatory_capacity IS NULL OR regulatory_capacity IN ('PRINCIPAL','AGENCY','RISKLESS_PRINCIPAL','FIDUCIARY')",
})
@dlt.view(name="v_party_role_validated")
def v_party_role_validated():
 return dlt.read("v_party_role_seed")

dlt.create_streaming_table(
 name="ref_party_role",
 comment="Firm party-role taxonomy - FIX 452 aligned",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_party_role",
 source="v_party_role_validated",
 keys=["party_role_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)

# ===========================================================================
# 2. ref_instrument_type
# ===========================================================================
INSTRUMENT_TYPES = [
 # (code, name, description, category, asset_class, cfi_prefix, listed, cleared, otc, fractional)
 ("COMMON_STOCK", "Common Stock", "Voting common equity of an issuer", "EQUITY", "EQUITY", "E", True, True, False, True),
 ("PREFERRED_STOCK", "Preferred Stock", "Preferred equity with fixed dividend", "EQUITY", "EQUITY", "E", True, True, False, False),
 ("ADR", "American Depositary Receipt", "US-listed certificate representing foreign equity", "EQUITY", "EQUITY", "E", True, True, False, False),
 ("GDR", "Global Depositary Receipt", "International depositary receipt", "EQUITY", "EQUITY", "E", True, True, False, False),
 ("ETF", "Exchange-Traded Fund", "Listed index / active ETF", "FUND", "EQUITY", "E", True, True, False, True),
 ("ETN", "Exchange-Traded Note", "Unsecured debt ETN", "DEBT", "FIXED_INCOME", "D", True, True, False, False),
 ("REIT", "Real Estate Investment Trust", "REIT security", "EQUITY", "EQUITY", "E", True, True, False, True),
 ("MUTUAL_FUND", "Mutual Fund", "Open-end mutual fund", "FUND", "EQUITY", "E", False, False, True, True),
 ("HEDGE_FUND", "Hedge Fund Interest", "Limited partnership interest in a hedge fund", "FUND", "EQUITY", "E", False, False, True, False),
 ("CORPORATE_BOND", "Corporate Bond", "Corporate issuer debt", "DEBT", "FIXED_INCOME", "D", False, False, True, False),
 ("GOVERNMENT_BOND", "Government Bond", "Sovereign debt (UST, Bund, OAT, Gilt, JGB)", "DEBT", "FIXED_INCOME", "D", False, False, True, False),
 ("MUNICIPAL_BOND", "Municipal Bond", "US state/local government debt", "DEBT", "FIXED_INCOME", "D", False, False, True, False),
 ("AGENCY_BOND", "Agency Bond", "US agency or GSE debt (Fannie, Freddie, Ginnie)", "DEBT", "FIXED_INCOME", "D", False, False, True, False),
 ("STRUCTURED_NOTE", "Structured Note", "Structured debt (linker, range accrual, etc.)", "STRUCTURED_PRODUCT", "FIXED_INCOME", "D", False, False, True, False),
 ("ABS", "Asset-Backed Security", "ABS / MBS / CLO", "STRUCTURED_PRODUCT", "FIXED_INCOME", "D", False, False, True, False),
 ("FUTURE", "Future", "Listed futures contract", "DERIVATIVE", "EQUITY_DERIVATIVE", "F", True, True, False, False),
 ("OPTION", "Option", "Listed or OTC option", "DERIVATIVE", "EQUITY_DERIVATIVE", "O", True, True, True, False),
 ("FX_OPTION", "FX Option", "OTC FX option", "DERIVATIVE", "FX", "O", False, False, True, False),
 ("SWAP_IRS", "Interest-Rate Swap", "OTC or cleared interest rate swap", "DERIVATIVE", "RATES", "S", False, True, True, False),
 ("SWAP_CDS", "Credit Default Swap", "Single-name or index CDS", "DERIVATIVE", "CREDIT", "S", False, True, True, False),
 ("SWAP_TRS", "Total-Return Swap", "OTC total-return swap", "DERIVATIVE", "EQUITY_DERIVATIVE", "S", False, False, True, False),
 ("SWAP_EQUITY", "Equity Swap", "OTC equity swap", "DERIVATIVE", "EQUITY_DERIVATIVE", "S", False, False, True, False),
 ("FORWARD", "Forward", "OTC forward contract", "DERIVATIVE", "RATES", "J", False, False, True, False),
 ("FX_SPOT", "FX Spot", "T+2 FX spot", "FOREX", "FX", "I", False, False, True, True),
 ("FX_SWAP", "FX Swap", "FX swap (spot + forward leg)", "FOREX", "FX", "I", False, False, True, False),
 ("REPO", "Repurchase Agreement", "Repo / reverse repo", "DEBT", "RATES", "L", False, True, True, False),
 ("SECURITIES_LENDING", "Securities Lending Agreement", "Securities lending transaction", "DEBT", "RATES", "L", False, False, True, False),
 ("TBA", "To-Be-Announced", "TBA mortgage contract", "STRUCTURED_PRODUCT", "FIXED_INCOME", "D", False, False, True, False),
 ("COMMODITY", "Commodity", "Physical commodity", "COMMODITY", "COMMODITY", "T", False, False, True, False),
 ("CRYPTO_SPOT", "Crypto Spot", "Spot cryptocurrency", "CRYPTO", "CRYPTO", "M", False, False, True, True),
 ("CRYPTO_DERIVATIVE", "Crypto Derivative", "Futures / options / perpetuals on crypto", "DERIVATIVE", "CRYPTO", "F", True, True, True, False),
]

INSTRUMENT_TYPE_SCHEMA = StructType([
 StructField("instrument_type_code", StringType, False),
 StructField("instrument_type_name", StringType, False),
 StructField("instrument_type_description", StringType, True),
 StructField("category", StringType, True),
 StructField("asset_class", StringType, True),
 StructField("cfi_category_prefix", StringType, True),
 StructField("is_listed", BooleanType, True),
 StructField("is_cleared", BooleanType, True),
 StructField("is_otc", BooleanType, True),
 StructField("supports_fractional", BooleanType, True),
])

@dlt.view(name="v_instrument_type_seed")
def v_instrument_type_seed():
 df = spark.createDataFrame(INSTRUMENT_TYPES, schema=INSTRUMENT_TYPE_SCHEMA)
 return (
 df.withColumn("effective_start_date", F.current_date).withColumn("effective_end_date", F.lit("9999-12-31").cast(DateType)).withColumn("is_active", F.lit(True)).withColumn("source_authority", F.lit("INTERNAL_TAXONOMY")).withColumn("source_version", F.lit(FIRM_VERSION)).withColumn("last_updated_date", F.current_date).withColumn("record_source", F.lit(FIRM_URL)).withColumn("load_date", F.current_timestamp()).withColumn("cdc_sequence", F.col("load_date").cast("long")).withColumn("cdc_operation", F.lit("UPSERT")).withColumn("row_hash",
 F.sha2(F.concat_ws("|",
 F.col("instrument_type_code"),
 F.col("instrument_type_name"),
 F.coalesce(F.col("category"), F.lit("")),
 F.coalesce(F.col("asset_class"), F.lit("")),
 F.coalesce(F.col("cfi_category_prefix"), F.lit("")),
 F.col("is_listed").cast("string"),
 F.col("is_cleared").cast("string"),
 F.col("is_otc").cast("string"),
 F.col("supports_fractional").cast("string"),
), 256))
)

@dlt.expect_all_or_fail({
 "instrument_type_upper": "instrument_type_code RLIKE '^[A-Z_]{3,40}$'",
 "asset_class_enum":
 "asset_class IN ('EQUITY','FIXED_INCOME','FX','COMMODITY','CRYPTO','RATES','CREDIT','EQUITY_DERIVATIVE')",
 "cfi_prefix_1_char": "cfi_category_prefix IS NULL OR length(cfi_category_prefix) = 1",
 "category_enum":
 "category IN ('EQUITY','DEBT','DERIVATIVE','FUND','FOREX','COMMODITY','CRYPTO','STRUCTURED_PRODUCT')",
})
@dlt.view(name="v_instrument_type_validated")
def v_instrument_type_validated():
 return dlt.read("v_instrument_type_seed")

dlt.create_streaming_table(
 name="ref_instrument_type",
 comment="Firm instrument-type taxonomy - 30 canonical codes",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_instrument_type",
 source="v_instrument_type_validated",
 keys=["instrument_type_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)
