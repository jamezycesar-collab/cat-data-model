-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 cat-pretrade-data-model contributors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- ============================================================================
-- File: 01b_party_attributes_delta.sql
-- Purpose: Enterprise Party Model - Attribute Entities (4 entities, Delta Lake)
-- SIC/NAICS/GICS/FATF on classification; account_purpose/margin_type/
-- custodian_party_id on account.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Entity 1 of 4: party_identifier (enriched) - ENHANCED
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_identifier (
 identifier_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 party_id STRING NOT NULL COMMENT 'FK to party.party_id',
 id_type STRING NOT NULL COMMENT 'Identifier scheme: LEI, EIN, SSN, CRD, IMID, MPID, FDID, LTID, SWIFT_BIC, DTCC_PARTICIPANT, NFA_ID, IARD, SEC_FILE_NUMBER, CFTC_ID, PASSPORT, NATIONAL_ID',
 id_value STRING NOT NULL COMMENT 'Identifier value - encrypted for SSN/TIN/passport; plaintext for LEI/EIN',
 id_issuer STRING COMMENT 'NEW - Issuing organization / LOU / government agency (e.g., GLEIF-accredited LOU, IRS, UK Passport Office)',
 id_country STRING COMMENT 'NEW - ISO 3166-1 alpha-2 country of issuance (jurisdiction of the identifier)',
 issuing_authority STRING COMMENT 'Authority that issued the identifier (e.g., SEC, FINRA, DTCC, SWIFT); may equal id_issuer for regulatory IDs',
 verification_date DATE COMMENT 'NEW - Date the identifier was last verified/refreshed (KYC refresh cycle driver; null = not verified)',
 is_primary BOOLEAN DEFAULT false COMMENT 'Primary identifier flag for this id_type within the party (exactly one TRUE per party+id_type)',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Party identifiers with issuer/country provenance and KYC verification_date; supports regulatory cross-reference (LEI, CRD, MPID, IMID, FDID/LTID for CAT)'
PARTITIONED BY (id_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Party identifier catalog with issuer and verification provenance',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (party_id, id_type, id_value) - KYC/regulatory lookup
-- LIQUID CLUSTERING BY (id_type, id_country)

-- ----------------------------------------------------------------------------
-- Entity 2 of 4: party_address (enriched) - ENHANCED
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_address (
 address_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 party_id STRING NOT NULL COMMENT 'FK to party.party_id',
 address_type STRING NOT NULL COMMENT 'Physical/logical address type: LEGAL, MAILING, TRADING_DESK, REGISTERED_AGENT, PRINCIPAL_OFFICE, BRANCH, CONTACT',
 address_purpose STRING COMMENT 'NEW - Usage purpose: REGISTERED (jurisdictional/legal record), OPERATIONAL (daily operations), MAILING (statements/correspondence), LEGAL (service of process); an address_type may have multiple purposes over time',
 address_line_1 STRING COMMENT 'PII - street address (mask non-prod)',
 address_line_2 STRING COMMENT 'PII - suite/floor/unit',
 city STRING COMMENT 'City/municipality',
 state_province STRING COMMENT 'State/province/region abbreviation',
 postal_code STRING COMMENT 'ZIP / postal code',
 country_code STRING NOT NULL COMMENT 'ISO 3166-1 alpha-2 (e.g., US, GB, JP) - FK-enforceable against ref_country',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Party addresses with address_purpose distinguishing REGISTERED / OPERATIONAL / MAILING / LEGAL use; PII-sensitive with non-prod masking obligations'
PARTITIONED BY (country_code)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'pii_columns' = 'address_line_1,address_line_2,city,state_province,postal_code',
 'description' = 'Address dimension (PII-sensitive) with purpose tagging',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (party_id, address_type, address_purpose)
-- PII: address fields require non-prod masking via external view

-- ----------------------------------------------------------------------------
-- Entity 3 of 4: party_classification (enriched) - ENHANCED
-- sic_code, naics_code, gics_sector, fatf_risk_rating
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_classification (
 classification_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 party_id STRING NOT NULL COMMENT 'FK to party.party_id',
 classification_scheme STRING NOT NULL COMMENT 'Scheme: MIFID_CLIENT (CAT), CFTC_CLASSIFICATION, SEC_13F, FATCA_STATUS, CRS_STATUS, INSTITUTIONAL_TYPE, CAT_CUSTOMER_TYPE, KYC_RATING, SANCTIONS_SCREENING',
 classification_value STRING NOT NULL COMMENT 'Scheme-specific value (e.g., PROFESSIONAL, RETAIL, INSTITUTIONAL, FFI, PASSIVE_NFFE)',
 sic_code STRING COMMENT 'NEW - Standard Industrial Classification code (4 digits); enables sector-based surveillance and sanctions screening',
 naics_code STRING COMMENT 'NEW - North American Industry Classification System code (6 digits); NA replacement for SIC',
 gics_sector STRING COMMENT 'NEW - MSCI/S&P Global Industry Classification Standard sector (e.g., 10=Energy, 25=Consumer Discretionary, 40=Financials)',
 fatf_risk_rating STRING COMMENT 'NEW - FATF AML/CTF risk classification: HIGH, MEDIUM, LOW; drives EDD (Enhanced Due Diligence) gating and transaction monitoring thresholds',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Party classifications - regulatory (MiFID/SEC/CFTC/FATCA/CRS) and commercial (SIC/NAICS/GICS) classifications with FATF AML risk tier'
PARTITIONED BY (classification_scheme)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Classification mappings with SIC/NAICS/GICS and FATF risk tiering',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (party_id, classification_scheme)
-- LIQUID CLUSTERING BY (fatf_risk_rating, gics_sector)

-- ----------------------------------------------------------------------------
-- Entity 4 of 4: account (enriched) - ENHANCED
-- account_purpose, base_currency (already present), margin_type, custodian_party_id
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS account (
 account_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 account_holder_role_id STRING NOT NULL COMMENT 'FK to party_role.party_role_id (role_type = ACCOUNT_HOLDER)',
 account_number STRING NOT NULL COMMENT 'Firm-assigned account number; unique within firm',
 account_type STRING NOT NULL COMMENT 'Account category: CUSTOMER_CASH, CUSTOMER_MARGIN, PROPRIETARY, MARKET_MAKING, ERROR_ACCOUNT, AVERAGE_PRICE, ALLOCATION, OMNIBUS, GIVE_UP, PRIME_BROKERAGE, CUSTODY',
 account_subtype STRING COMMENT 'Further classification: IRA, 401K, JOINT, UGMA, INDIVIDUAL, EDGE, SMA, UMA, LLC, PARTNERSHIP',
 account_purpose STRING COMMENT 'NEW - Intended purpose: TRADING, INVESTMENT, HEDGING, SEGREGATED, FIRM_CAPITAL, CUSTODY_ONLY, CLEARING; drives regulatory segregation and reg-capital treatment',
 firm_designated_id STRING COMMENT 'CAIS FDID - Firm Designated ID reported to CAT for this account (CAT Rule 613)',
 account_name STRING COMMENT 'PII - account name/nickname; mask non-prod',
 account_status STRING NOT NULL COMMENT 'Lifecycle: OPEN, CLOSED, FROZEN, RESTRICTED, DORMANT, LIQUIDATING',
 opening_date DATE COMMENT 'Date account was opened',
 closing_date DATE COMMENT 'Date account was closed (null if still open)',
 base_currency STRING DEFAULT 'USD' COMMENT 'ISO 4217 base currency of account (NEW in SKILL but DEFAULT retained)',
 margin_eligible_flag BOOLEAN DEFAULT false COMMENT 'Whether account can trade on margin (renamed from margin_eligible boolean convention)',
 margin_type STRING COMMENT 'NEW - Margin regime: REG_T (Fed Reg T, 50%), PORTFOLIO_MARGIN (SEC-approved PM, risk-based), PROPRIETARY (firm margin), CASH (no margin), CUSTOMER_PORTFOLIO_MARGIN',
 options_level INT DEFAULT 0 COMMENT 'Options approval: 0=none, 1=covered calls, 2=spreads, 3=puts, 4=uncovered writes, 5=complex spreads',
 custodian_party_id STRING COMMENT 'NEW - FK to party.party_id identifying custodian for this account; NULL for self-custody',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Customer and proprietary accounts - enriched with account_purpose, margin_type (Reg T / Portfolio Margin), and custodian_party_id for tri-party custody linkage'
PARTITIONED BY (account_status)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'pii_columns' = 'account_name,account_number',
 'description' = 'Account dimension (customer, proprietary, firm) with margin regime and custodian linkage',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (account_holder_role_id, account_number)
-- LIQUID CLUSTERING BY (account_type, account_status, margin_type)
-- PII: account_name and account_number should be masked in non-prod

-- ============================================================================
-- END OF FILE - 4 CREATE TABLE statements
-- Verification: grep -c "CREATE TABLE" should return 4
-- ============================================================================
