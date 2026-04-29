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
-- File: 01_dim_tables.sql
-- Purpose: Gold layer - 8 conformed dimensions (SCD Type 2)
-- Pattern: Every SCD2 dim has: effective_start_date, effective_end_date,
-- is_current, row_hash (SHA-256 of non-key attrs) for change detection.
-- Consumers: fact_cat_order_events, fact_cat_allocations, fact_cat_quotes,
-- fact_cat_customer_records + Power BI Direct Lake semantic model.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. dim_date - static trading calendar with CAT and settlement deadlines
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
 date_sk INT NOT NULL COMMENT 'Surrogate key = YYYYMMDD',
 calendar_date DATE NOT NULL COMMENT 'Actual calendar date (ISO 8601)',
 day_of_week INT COMMENT '1=Monday... 7=Sunday per ISO 8601',
 day_name STRING COMMENT 'Monday... Sunday',
 day_of_month INT,
 day_of_year INT,
 week_of_year INT,
 month_number INT,
 month_name STRING,
 quarter INT,
 year_number INT,
 fiscal_year INT COMMENT 'Based on firm fiscal calendar',
 fiscal_quarter INT,
 is_trading_day BOOLEAN NOT NULL COMMENT 'NYSE + NASDAQ trading calendar',
 is_settlement_day BOOLEAN NOT NULL COMMENT 'DTCC settlement calendar',
 t_plus_1_settlement_date DATE COMMENT 'T+1 settlement per SEC Rule 15c6-1 (May 2024)',
 t_plus_3_error_correction_deadline DATE COMMENT 'CAT T+3 error correction deadline',
 cat_submission_deadline_timestamp TIMESTAMP COMMENT 'CAT 08:00 ET on T+1 hard deadline',
 holiday_flag BOOLEAN COMMENT 'NYSE observed holiday',
 holiday_name STRING
)
USING DELTA
COMMENT 'Static calendar dimension with CAT and settlement deadlines'
TBLPROPERTIES (
 'delta.enableChangeDataFeed'='false',
 'compression.codec'='zstd',
 'subject_area'='gold_dim',
 'scd_type'='0'
);

-- ----------------------------------------------------------------------------
-- 2. dim_party - SCD Type 2
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_party (
 dim_party_sk BIGINT NOT NULL COMMENT 'Monotonically increasing surrogate',
 party_id_bk STRING NOT NULL COMMENT 'Business key from Silver hub_party',
 lei STRING COMMENT 'ISO 17442 Legal Entity Identifier',
 cat_imid STRING COMMENT 'FINRA CAT IMID for reporters',
 crd STRING COMMENT 'FINRA Central Registration Depository ID',
 fdid STRING COMMENT 'CAT Firm Designated ID (CAIS)',
 party_type STRING NOT NULL COMMENT 'LEGAL_ENTITY | NATURAL_PERSON',
 party_status STRING COMMENT 'ACTIVE | INACTIVE | SUSPENDED',
 legal_form STRING COMMENT 'ISO 20275 ELF code',
 incorporation_country STRING COMMENT 'ISO 3166-1 alpha-2',
 domicile_country STRING COMMENT 'ISO 3166-1 alpha-2',
 tax_residency STRING COMMENT 'ISO 3166-1 alpha-2',
 ultimate_parent_party_id STRING COMMENT 'FK to dim_party.party_id_bk (ultimate parent)',
 g_sib_flag BOOLEAN COMMENT 'Basel BCBS G-SIB designation',
 pep_flag BOOLEAN COMMENT 'Politically Exposed Person (natural_person)',
 mifid_ii_category STRING COMMENT 'MiFID II client category',
 effective_start_date DATE NOT NULL COMMENT 'SCD2 - version starts',
 effective_end_date DATE NOT NULL COMMENT 'SCD2 - 9999-12-31 if current',
 is_current BOOLEAN NOT NULL COMMENT 'SCD2 current flag',
 row_hash STRING NOT NULL COMMENT 'SHA-256 of non-key attrs for change detection',
 dv2_source_hk STRING COMMENT 'Silver party_hk for lineage',
 load_date TIMESTAMP NOT NULL COMMENT 'Gold load timestamp',
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (is_current)
COMMENT 'Party conformed dimension (SCD Type 2) - enriched with CAT identifiers'
TBLPROPERTIES (
 'delta.enableChangeDataFeed'='true',
 'compression.codec'='zstd',
 'subject_area'='gold_dim',
 'scd_type'='2'
);

-- ----------------------------------------------------------------------------
-- 3. dim_instrument - SCD Type 2
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_instrument (
 dim_instrument_sk BIGINT NOT NULL,
 instrument_id_bk STRING NOT NULL,
 isin STRING COMMENT 'ISO 6166 International Securities Identification Number',
 cusip STRING COMMENT 'North American CUSIP',
 sedol STRING COMMENT 'UK SEDOL',
 figi STRING COMMENT 'OpenFIGI',
 symbol STRING COMMENT 'Exchange ticker symbol (CAT field)',
 security_id STRING COMMENT 'CAT securityID (derived from ISIN/CUSIP/FIGI)',
 security_id_source STRING COMMENT 'CAT securityIDSource (ISIN|CUSIP|FIGI|OCC|SYM)',
 asset_class STRING NOT NULL COMMENT 'EQUITY | FIXED_INCOME | OPTION | FUTURE | SWAP | FX | DIGITAL | STRUCTURED',
 cfi_code STRING COMMENT 'ISO 10962 Classification of Financial Instrument',
 instrument_type STRING,
 primary_exchange_mic STRING COMMENT 'ISO 10383 MIC of primary listing',
 issuer_party_id_bk STRING COMMENT 'FK to dim_party.party_id_bk',
 currency STRING COMMENT 'ISO 4217',
 maturity_date DATE COMMENT 'Fixed income / option expiration',
 option_type STRING COMMENT 'CALL | PUT (options only)',
 option_style STRING COMMENT 'AMERICAN | EUROPEAN | BERMUDAN',
 strike_price DECIMAL(20,8),
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_current BOOLEAN NOT NULL,
 row_hash STRING NOT NULL,
 dv2_source_hk STRING,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (is_current)
COMMENT 'Instrument conformed dimension (SCD Type 2) - CAT securityID fields denormalised'
TBLPROPERTIES (
 'delta.enableChangeDataFeed'='true',
 'compression.codec'='zstd',
 'subject_area'='gold_dim',
 'scd_type'='2'
);

-- ----------------------------------------------------------------------------
-- 4. dim_venue - SCD Type 2
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_venue (
 dim_venue_sk BIGINT NOT NULL,
 venue_id_bk STRING NOT NULL,
 mic STRING NOT NULL COMMENT 'ISO 10383 Market Identifier Code',
 operating_mic STRING,
 venue_name STRING,
 venue_category STRING COMMENT 'EXCHANGE | ATS | MTF | SDP | OTC | DARK | SI',
 venue_type STRING COMMENT 'REGULATED_MARKET | ORGANISED_TRADING_FACILITY | MULTILATERAL_TRADING_FACILITY',
 country_code STRING COMMENT 'ISO 3166-1 alpha-2',
 market_category STRING,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_current BOOLEAN NOT NULL,
 row_hash STRING NOT NULL,
 dv2_source_hk STRING,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (is_current)
COMMENT 'Venue conformed dimension (SCD Type 2)'
TBLPROPERTIES (
 'delta.enableChangeDataFeed'='true',
 'compression.codec'='zstd',
 'subject_area'='gold_dim',
 'scd_type'='2'
);

-- ----------------------------------------------------------------------------
-- 5. dim_account - SCD Type 2
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_account (
 dim_account_sk BIGINT NOT NULL,
 account_id_bk STRING NOT NULL,
 account_type STRING COMMENT 'CUSTOMER | PROPRIETARY | MARKET_MAKER | ERROR | INVENTORY',
 account_holder_party_id_bk STRING COMMENT 'FK to dim_party',
 custodian_party_id_bk STRING,
 clearing_member_party_id_bk STRING,
 prime_broker_party_id_bk STRING,
 base_currency STRING COMMENT 'ISO 4217',
 account_status STRING,
 cat_large_trader_flag BOOLEAN COMMENT 'SEC Rule 13h-1 Large Trader',
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_current BOOLEAN NOT NULL,
 row_hash STRING NOT NULL,
 dv2_source_hk STRING,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (is_current)
COMMENT 'Account conformed dimension (SCD Type 2)'
TBLPROPERTIES (
 'delta.enableChangeDataFeed'='true',
 'compression.codec'='zstd',
 'subject_area'='gold_dim',
 'scd_type'='2'
);

-- ----------------------------------------------------------------------------
-- 6. dim_event_type - static taxonomy (99 rows from CAT IM v4.1.0r15, SCD Type 0)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_event_type (
 dim_event_type_sk INT NOT NULL COMMENT 'Stable surrogate = 1..99 ordered by section number',
 cat_event_code STRING NOT NULL COMMENT 'FINRA CAT 4-letter event code (e.g. MENO, MEOT, MEPA)',
 event_name STRING NOT NULL,
 event_description STRING,
 event_phase STRING NOT NULL COMMENT 'ORDER | ROUTING | INTERNAL_ROUTE | CHILD_ORDER | MODIFICATION | QUOTE | EXECUTION | FULFILLMENT | POST_TRADE | LIFECYCLE',
 submission_file_type STRING NOT NULL COMMENT 'OrderEvents | QuoteEvents | Allocations | CAIS',
 order_of_events INT COMMENT 'Canonical lifecycle ordinal for audit',
 is_manual BOOLEAN,
 is_electronic BOOLEAN,
 is_correction_eligible BOOLEAN,
 active_flag BOOLEAN NOT NULL,
 effective_start_date DATE NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Conformed FINRA CAT event taxonomy dimension - SCD Type 0 - 99 rows from v4.1.0r15'
TBLPROPERTIES (
 'delta.enableChangeDataFeed'='false',
 'compression.codec'='zstd',
 'subject_area'='gold_dim',
 'scd_type'='0',
 'row_count_invariant'='99'
);

-- Seed: dim_event_type is populated by the DLT pipeline at
-- dlt-pipelines/dim_pipelines/dlt_dim_event_type.py, which loads from the
-- verified CSV at primary-sources/cat_im_event_types.csv (CAT IM Tech Specs
-- v4.1.0r15 Tables 15, 60, 61). 99 rows total: 39 equity + 35 simple option
-- + 25 multi-leg. Do not hand-INSERT here; that path produced fabrications
-- in v2.0.0 and is now rejected by the validator.

-- ----------------------------------------------------------------------------
-- 7. dim_desk - SCD Type 2
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_desk (
 dim_desk_sk BIGINT NOT NULL,
 desk_id_bk STRING NOT NULL,
 desk_name STRING,
 desk_type STRING COMMENT 'EQUITY | FI | DERIVATIVES | FX | MM | PROP | RISK',
 parent_desk_id_bk STRING COMMENT 'Self-ref for hierarchy',
 desk_hierarchy_path STRING COMMENT 'Denormalised / slash-separated',
 lei STRING COMMENT 'Desk-level LEI when allocated',
 supervisor_party_id_bk STRING,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_current BOOLEAN NOT NULL,
 row_hash STRING NOT NULL,
 dv2_source_hk STRING,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (is_current)
COMMENT 'Trading desk dimension (SCD Type 2) with hierarchy path'
TBLPROPERTIES (
 'delta.enableChangeDataFeed'='true',
 'compression.codec'='zstd',
 'subject_area'='gold_dim',
 'scd_type'='2'
);

-- ----------------------------------------------------------------------------
-- 8. dim_trader - SCD Type 2 (individual trader identity for CAT traderID)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_trader (
 dim_trader_sk BIGINT NOT NULL,
 trader_id_bk STRING NOT NULL COMMENT 'Internal trader ID (BK)',
 cat_trader_id STRING COMMENT 'CAT TraderID field value (hashed when natural_person)',
 given_name_hash STRING COMMENT 'SHA-256 of given name - PII protection',
 family_name_hash STRING COMMENT 'SHA-256 of family name',
 desk_id_bk STRING COMMENT 'FK to dim_desk',
 manager_trader_id_bk STRING COMMENT 'FK to dim_trader (supervisor)',
 license_status STRING COMMENT 'ACTIVE | INACTIVE | SUSPENDED',
 series_7_holder BOOLEAN,
 series_24_holder BOOLEAN,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_current BOOLEAN NOT NULL,
 row_hash STRING NOT NULL,
 dv2_source_hk STRING,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (is_current)
COMMENT 'Trader conformed dimension (SCD Type 2) - PII-hashed'
TBLPROPERTIES (
 'delta.enableChangeDataFeed'='true',
 'compression.codec'='zstd',
 'subject_area'='gold_dim',
 'scd_type'='2',
 'pii_columns'='given_name_hash,family_name_hash'
);

-- ============================================================================
-- END - 8 dimensions created (1 static calendar + 1 static taxonomy + 6 SCD2)
-- Every SCD2 dim has: effective_start_date, effective_end_date, is_current, row_hash
-- Event taxonomy dim populated from primary-sources/cat_im_event_types.csv (99 rows).
-- ============================================================================
