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
-- 6. dim_event_type - static taxonomy (exactly 50 rows, SCD Type 0)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_event_type (
 dim_event_type_sk INT NOT NULL COMMENT 'Stable surrogate = 1..50',
 cat_event_code STRING NOT NULL COMMENT 'FINRA CAT 4-letter event code (e.g. MENO, MEOT, MEPA)',
 event_name STRING NOT NULL,
 event_description STRING,
 event_phase STRING NOT NULL COMMENT 'PRE_TRADE | ORDER_REQUEST | ORDER_OPEN | ORDER_ROUTING | EXECUTION | ALLOCATION | CORRECTION | CUSTOMER_RECORD',
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
COMMENT 'Static 50-row FINRA CAT event taxonomy dimension - SCD Type 0'
TBLPROPERTIES (
 'delta.enableChangeDataFeed'='false',
 'compression.codec'='zstd',
 'subject_area'='gold_dim',
 'scd_type'='0',
 'row_count_invariant'='50'
);

-- Seed: 50 CAT event types - aligned to v4.1.0r9 Industry Member Technical Spec
INSERT INTO dim_event_type VALUES
 (1, 'MEQR', 'Quote Request (electronic)', 'Request for quotation from client to dealer', 'PRE_TRADE', 'QuoteEvents', 10, FALSE, TRUE, FALSE, TRUE, DATE'2024-01-01', 'SEED'),
 (2, 'MEQS', 'Quote Sent (electronic)', 'Quote response posted to RFQ', 'PRE_TRADE', 'QuoteEvents', 11, FALSE, TRUE, FALSE, TRUE, DATE'2024-01-01', 'SEED'),
 (3, 'MEIR', 'Internal Order Received (electronic)', 'Order received internally from a customer', 'ORDER_REQUEST', 'OrderEvents', 20, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (4, 'MOIR', 'Internal Order Received (manual)', 'Order received internally via manual channel', 'ORDER_REQUEST', 'OrderEvents', 21, TRUE, FALSE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (5, 'MENO', 'New Order', 'Order opened on central book', 'ORDER_OPEN', 'OrderEvents', 30, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (6, 'MEOA', 'Order Accepted', 'Order accepted by trading system', 'ORDER_OPEN', 'OrderEvents', 31, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (7, 'MEOM', 'Order Modified', 'Order attributes modified', 'ORDER_OPEN', 'OrderEvents', 32, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (8, 'MEOR', 'Order Rejected', 'Order rejected by trading system', 'ORDER_OPEN', 'OrderEvents', 33, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (9, 'MEOC', 'Order Cancelled', 'Order cancelled before fill', 'ORDER_OPEN', 'OrderEvents', 34, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (10, 'MEOE', 'Order Expired', 'Order expired per time_in_force', 'ORDER_OPEN', 'OrderEvents', 35, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (11, 'MECO', 'Manual/Electronic Route Out', 'Order routed to external venue', 'ORDER_ROUTING', 'OrderEvents', 40, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (12, 'MLCO', 'Route Out to an Exchange', 'Order routed specifically to an exchange', 'ORDER_ROUTING', 'OrderEvents', 41, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (13, 'MOCO', 'Manual Route Out', 'Order routed via manual channel', 'ORDER_ROUTING', 'OrderEvents', 42, TRUE, FALSE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (14, 'MEOT', 'Order Fulfillment', 'Execution (fill) received', 'EXECUTION', 'OrderEvents', 50, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (15, 'MEOTQ', 'Order Fulfillment Nanosecond', 'Execution with nanosecond timestamp', 'EXECUTION', 'OrderEvents', 51, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (16, 'MEOTS', 'Stopped Order Fulfillment', 'Fulfillment of stopped order', 'EXECUTION', 'OrderEvents', 52, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (17, 'MEOF', 'Order Fulfillment Final', 'Terminal fill - order fully executed', 'EXECUTION', 'OrderEvents', 53, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (18, 'MEPA', 'Post-trade Allocation (prorata)', 'Allocation of fill to customer accounts', 'ALLOCATION', 'Allocations', 60, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (19, 'MEAA', 'Affirmed Allocation', 'Allocation affirmed by customer', 'ALLOCATION', 'Allocations', 61, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (20, 'MOFA', 'Order Fill Allocated to Account', 'Specific fill attributed to one account', 'ALLOCATION', 'Allocations', 62, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (21, 'MEOJ', 'Order Adjustment / Correction', 'Post-event correction / adjustment', 'CORRECTION', 'OrderEvents', 70, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (22, 'MOQR', 'Manual Quote Request', 'Quote request received via manual channel', 'PRE_TRADE', 'QuoteEvents', 12, TRUE, FALSE, FALSE, TRUE, DATE'2024-01-01', 'SEED'),
 (23, 'MOQS', 'Manual Quote Sent', 'Quote sent via manual channel', 'PRE_TRADE', 'QuoteEvents', 13, TRUE, FALSE, FALSE, TRUE, DATE'2024-01-01', 'SEED'),
 (24, 'MONO', 'Manual New Order', 'New order via manual channel', 'ORDER_OPEN', 'OrderEvents', 36, TRUE, FALSE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (25, 'MOOA', 'Manual Order Accepted', 'Order accepted via manual channel', 'ORDER_OPEN', 'OrderEvents', 37, TRUE, FALSE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (26, 'MOOR', 'Manual Order Rejected', 'Order rejected via manual channel', 'ORDER_OPEN', 'OrderEvents', 38, TRUE, FALSE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (27, 'MOOM', 'Manual Order Modification', 'Order modified via manual channel', 'ORDER_OPEN', 'OrderEvents', 39, TRUE, FALSE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (28, 'MOOC', 'Manual Order Cancelled', 'Order cancelled via manual channel', 'ORDER_OPEN', 'OrderEvents', 43, TRUE, FALSE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (29, 'MOOT', 'Manual Order Fulfillment', 'Manual execution record', 'EXECUTION', 'OrderEvents', 54, TRUE, FALSE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (30, 'MEPB', 'Block Order Allocation', 'Block fill allocated across accounts', 'ALLOCATION', 'Allocations', 63, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (31, 'MEOJR', 'Order Adjustment Rejected', 'Adjustment/correction rejected by CAT', 'CORRECTION', 'OrderEvents', 71, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (32, 'MELC', 'Linkage of Child Order', 'Parent-child order linkage', 'ORDER_ROUTING', 'OrderEvents', 44, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (33, 'MELP', 'Linkage of Parent Order', 'Child reports parent linkage', 'ORDER_ROUTING', 'OrderEvents', 45, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (34, 'MEOX', 'Order Expired (exchange-driven)', 'Order expired on venue side', 'ORDER_OPEN', 'OrderEvents', 46, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (35, 'MEOS', 'Stopped Order Stop Price Triggered', 'Stop price condition triggered', 'ORDER_OPEN', 'OrderEvents', 47, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (36, 'MEON', 'Order Notice', 'General notice event on order', 'ORDER_OPEN', 'OrderEvents', 48, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (37, 'MEOK', 'Order Locked / Held', 'Order held by supervisor / compliance', 'ORDER_OPEN', 'OrderEvents', 49, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (38, 'MEPZ', 'Allocation Cancelled', 'Previously reported allocation cancelled', 'ALLOCATION', 'Allocations', 64, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (39, 'MEPM', 'Allocation Modified', 'Allocation attributes modified', 'ALLOCATION', 'Allocations', 65, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (40, 'MEAX', 'Affirmation Cancelled', 'Allocation affirmation withdrawn', 'ALLOCATION', 'Allocations', 66, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (41, 'MECX', 'Cancel Route-out', 'Cancellation of outbound route', 'ORDER_ROUTING', 'OrderEvents', 55, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (42, 'MECM', 'Modified Route', 'Outbound route modified', 'ORDER_ROUTING', 'OrderEvents', 56, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (43, 'MERO', 'Route Out Response', 'Response from downstream venue', 'ORDER_ROUTING', 'OrderEvents', 57, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (44, 'MERR', 'Route Rejection', 'Route was rejected by destination', 'ORDER_ROUTING', 'OrderEvents', 58, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (45, 'MERC', 'Route Cancelled by Venue', 'Destination venue cancelled route', 'ORDER_ROUTING', 'OrderEvents', 59, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (46, 'MEXC', 'Execution Cancelled', 'Previously reported execution cancelled', 'EXECUTION', 'OrderEvents', 72, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (47, 'MEXM', 'Execution Modified', 'Execution fields modified', 'EXECUTION', 'OrderEvents', 73, FALSE, TRUE, TRUE, TRUE, DATE'2024-01-01', 'SEED'),
 (48, 'CAIS_C', 'CAIS Customer Record', 'Customer record for CAIS submission', 'CUSTOMER_RECORD','CAIS', 80, FALSE, TRUE, FALSE, TRUE, DATE'2024-01-01', 'SEED'),
 (49, 'CAIS_A', 'CAIS Account Record', 'Account record for CAIS submission', 'CUSTOMER_RECORD','CAIS', 81, FALSE, TRUE, FALSE, TRUE, DATE'2024-01-01', 'SEED'),
 (50, 'CAIS_R', 'CAIS Trading Relationship', 'Customer-account trading relationship', 'CUSTOMER_RECORD','CAIS', 82, FALSE, TRUE, FALSE, TRUE, DATE'2024-01-01', 'SEED');

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
-- Event taxonomy dim seeded with exactly 50 rows.
-- ============================================================================
