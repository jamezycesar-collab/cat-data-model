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
-- File: 02_fact_tables.sql
-- Purpose: Gold layer - 4 event-grain facts aligned to FINRA CAT taxonomy
-- Grain: One row per CAT event (except fact_cat_customer_records = daily snapshot)
-- Required: firm_roe_id + dv2_source_hk on every row
-- Partition: BY event_date for all three event facts (BY snapshot_date for CAIS)
-- Consumers: CAT submission generator (07_*.py) + Power BI semantic model
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. fact_cat_order_events - Order lifecycle events
-- Covers CAT events: MEIR, MOIR, MENO, MEOA, MEOM, MEOR, MEOC, MEOE,
-- MECO, MLCO, MOCO, MEOT, MEOTQ, MEOTS, MEOF, MEOJ,
-- and all manual/options counterparts (~40 of 50 CAT events)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_cat_order_events (order_event_fact_sk BIGINT NOT NULL COMMENT 'Monotonically increasing surrogate',
 event_date DATE NOT NULL COMMENT 'Partition key - CAT activity date',
 firm_roe_id STRING NOT NULL COMMENT 'CAT firm Reporter Order Event ID - unique per day per IMID',
 cat_order_id STRING NOT NULL COMMENT 'CAT order identifier - same across lifecycle',
 cat_event_code STRING NOT NULL COMMENT 'FK -> dim_event_type.cat_event_code',
 dim_party_sk BIGINT NOT NULL COMMENT 'FK -> dim_party (reporting firm)',
 dim_instrument_sk BIGINT NOT NULL COMMENT 'FK -> dim_instrument',
 dim_venue_sk BIGINT COMMENT 'FK -> dim_venue (null for internal events)',
 dim_account_sk BIGINT COMMENT 'FK -> dim_account',
 dim_desk_sk BIGINT COMMENT 'FK -> dim_desk',
 dim_trader_sk BIGINT COMMENT 'FK -> dim_trader',
 counterparty_party_sk BIGINT COMMENT 'FK -> dim_party (counterparty, if known)',
 event_timestamp TIMESTAMP NOT NULL COMMENT 'Event time with nanosecond precision (CAT MEOTQ requirement)',
 event_nanos BIGINT COMMENT 'Sub-second nanosecond component for MEOTQ precision',
 received_timestamp TIMESTAMP COMMENT 'Firm receive timestamp',
 route_timestamp TIMESTAMP COMMENT 'Route-out timestamp for MECO/MLCO/MOCO',
 side STRING COMMENT 'BUY | SELL | SELL_SHORT | SELL_SHORT_EXEMPT',
 order_type STRING COMMENT 'MARKET | LIMIT | STOP | STOP_LIMIT | PEGGED',
 order_capacity STRING COMMENT 'AGENCY | PRINCIPAL | RISKLESS_PRINCIPAL',
 time_in_force STRING COMMENT 'DAY | IOC | FOK | GTC | GTD | OPG | CLS',
 quantity DECIMAL(38,10) COMMENT 'Original or modified order quantity',
 leaves_quantity DECIMAL(38,10) COMMENT 'Open quantity remaining',
 cumulative_filled_quantity DECIMAL(38,10) COMMENT 'Sum of fills to date',
 price DECIMAL(38,10) COMMENT 'Limit or stop price',
 execution_price DECIMAL(38,10) COMMENT 'Fill price (MEOT/MEOTQ/MEOTS only)',
 execution_quantity DECIMAL(38,10) COMMENT 'Fill quantity',
 currency_code STRING COMMENT 'ISO 4217',
 handling_instructions STRING COMMENT 'NOT_HELD | HELD | DNR | DNI',
 special_handling_codes STRING COMMENT 'Comma-delimited CAT codes (e.g. ISO, DIR)',
 display_instruction STRING COMMENT 'DISPLAY | NON_DISPLAY | RESERVE',
 iso_flag BOOLEAN COMMENT 'Intermarket Sweep Order',
 short_sale_exempt_flag BOOLEAN COMMENT 'Regulation SHO exempt',
 solicited_flag BOOLEAN,
 directed_flag BOOLEAN,
 parent_order_id STRING COMMENT 'Parent order (child order link)',
 routed_to_venue_mic STRING COMMENT 'MIC of destination for MECO/MLCO',
 routed_to_firm_imid STRING COMMENT 'Destination IMID',
 original_event_ref STRING COMMENT 'For MEOJ corrections: firmROEID being corrected',
 correction_reason STRING COMMENT 'For MEOJ: why',
 cancel_reason STRING COMMENT 'For MEOC',
 rejection_reason STRING COMMENT 'For MEOR',
 dv2_source_hk STRING NOT NULL COMMENT 'Silver hub/link hash key - BCBS 239 lineage',
 source_system STRING COMMENT 'Originating OMS/EMS system',
 load_date TIMESTAMP NOT NULL COMMENT 'Gold load timestamp',
 record_source STRING NOT NULL)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'CAT order lifecycle event fact - event grain - one row per CAT OrderEvents submission'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true',
 'delta.autoOptimize.optimizeWrite'='true',
 'delta.autoOptimize.autoCompact'='true',
 'compression.codec'='zstd',
 'subject_area'='gold_fact',
 'cat_submission_file_type'='OrderEvents',
 'grain'='one row per CAT order event');

-- ----------------------------------------------------------------------------
-- 2. fact_cat_allocations - Post-trade allocation events
-- Covers CAT events: MEPA, MEAA, MOFA (Allocations file type)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_cat_allocations (allocation_fact_sk BIGINT NOT NULL,
 event_date DATE NOT NULL COMMENT 'Partition key - allocation activity date',
 firm_roe_id STRING NOT NULL COMMENT 'CAT firm Reporter Order Event ID',
 cat_order_id STRING NOT NULL,
 cat_allocation_id STRING COMMENT 'Firm allocation identifier',
 cat_event_code STRING NOT NULL COMMENT 'FK -> dim_event_type (MEPA|MEAA|MOFA)',
 dim_party_sk BIGINT NOT NULL COMMENT 'FK -> dim_party (reporting firm)',
 dim_instrument_sk BIGINT NOT NULL COMMENT 'FK -> dim_instrument',
 dim_account_sk BIGINT NOT NULL COMMENT 'FK -> dim_account (sub-account allocation)',
 dim_desk_sk BIGINT COMMENT 'FK -> dim_desk',
 parent_execution_id STRING COMMENT 'FK to MEOT/MEOTQ execution being allocated',
 allocation_method STRING COMMENT 'PRO_RATA | MANUAL | FIFO | LIFO | AVG_PRICE',
 allocation_status STRING COMMENT 'PROPOSED | AFFIRMED | CONFIRMED | REJECTED',
 allocated_quantity DECIMAL(38,10) NOT NULL,
 allocated_price DECIMAL(38,10),
 allocation_pct DECIMAL(18,10) COMMENT 'Percentage of parent execution',
 gross_amount DECIMAL(38,10),
 commission DECIMAL(38,10),
 commission_currency STRING,
 net_amount DECIMAL(38,10),
 settlement_date DATE COMMENT 'T+1 per SEC 15c6-1',
 settlement_currency STRING,
 affirmation_timestamp TIMESTAMP COMMENT 'For MEAA - when counterparty affirmed',
 allocation_timestamp TIMESTAMP NOT NULL COMMENT 'With nanosecond precision',
 allocation_nanos BIGINT,
 dv2_source_hk STRING NOT NULL COMMENT 'Silver allocation link hash - BCBS 239 lineage',
 source_system STRING,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'CAT post-trade allocation fact - one row per sub-account allocation'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true',
 'delta.autoOptimize.optimizeWrite'='true',
 'compression.codec'='zstd',
 'subject_area'='gold_fact',
 'cat_submission_file_type'='Allocations',
 'grain'='one row per allocation event');

-- ----------------------------------------------------------------------------
-- 3. fact_cat_quotes - Quote request / quote sent events
-- Covers CAT events: MEQR, MEQS (QuoteEvents file type)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_cat_quotes (quote_fact_sk BIGINT NOT NULL,
 event_date DATE NOT NULL COMMENT 'Partition key - quote activity date',
 firm_roe_id STRING NOT NULL COMMENT 'CAT firm Reporter Order Event ID',
 cat_quote_id STRING NOT NULL COMMENT 'CAT quote identifier',
 cat_rfq_id STRING COMMENT 'Linked RFQ identifier for MEQR -> MEQS pairs',
 cat_event_code STRING NOT NULL COMMENT 'FK -> dim_event_type (MEQR|MEQS)',
 dim_party_sk BIGINT NOT NULL COMMENT 'FK -> dim_party (quoting firm)',
 counterparty_party_sk BIGINT COMMENT 'FK -> dim_party (RFQ requester)',
 dim_instrument_sk BIGINT NOT NULL COMMENT 'FK -> dim_instrument',
 dim_venue_sk BIGINT COMMENT 'FK -> dim_venue (MIC of quote venue)',
 dim_trader_sk BIGINT COMMENT 'FK -> dim_trader',
 quote_side STRING COMMENT 'BID | OFFER | TWO_WAY',
 bid_price DECIMAL(38,10),
 ask_price DECIMAL(38,10),
 bid_size DECIMAL(38,10),
 ask_size DECIMAL(38,10),
 currency_code STRING,
 quote_status STRING COMMENT 'ACTIVE | EXPIRED | CANCELLED | EXECUTED',
 quote_type STRING COMMENT 'INDICATIVE | FIRM | TRADEABLE',
 min_quote_life_ms INT COMMENT 'Minimum quote life (millis)',
 quote_expiry_timestamp TIMESTAMP,
 rfq_expiry_timestamp TIMESTAMP COMMENT 'For MEQR - when RFQ expires',
 quote_timestamp TIMESTAMP NOT NULL COMMENT 'Nanosecond precision',
 quote_nanos BIGINT,
 dv2_source_hk STRING NOT NULL COMMENT 'Silver quote link hash - BCBS 239 lineage',
 source_system STRING,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'CAT quote event fact - one row per MEQR or MEQS'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true',
 'delta.autoOptimize.optimizeWrite'='true',
 'compression.codec'='zstd',
 'subject_area'='gold_fact',
 'cat_submission_file_type'='QuoteEvents',
 'grain'='one row per quote event');

-- ----------------------------------------------------------------------------
-- 4. fact_cat_customer_records - CAIS daily snapshot
-- Covers CAT events: CAIS_Customer, CAIS_Account, CAIS_TradingRelationship
-- Snapshot grain - partitioned by snapshot_date (per CAIS file submission)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_cat_customer_records (cais_fact_sk BIGINT NOT NULL,
 snapshot_date DATE NOT NULL COMMENT 'Partition key - CAIS snapshot date',
 event_date DATE NOT NULL COMMENT 'Alias of snapshot_date for submission dispatch',
 firm_roe_id STRING NOT NULL COMMENT 'CAIS submission identifier (firm + date + seq)',
 fdid STRING NOT NULL COMMENT 'CAT Firm Designated ID - natural key of CAIS record',
 cat_event_code STRING NOT NULL COMMENT 'FK -> dim_event_type (CAIS_C|CAIS_A|CAIS_R)',
 dim_party_sk BIGINT NOT NULL COMMENT 'FK -> dim_party',
 dim_account_sk BIGINT NOT NULL COMMENT 'FK -> dim_account',
 record_type STRING NOT NULL COMMENT 'CUSTOMER | ACCOUNT | TRADING_RELATIONSHIP',
 action_type STRING NOT NULL COMMENT 'INSERT | UPDATE | DELETE',
 customer_type STRING COMMENT 'INDIVIDUAL | ENTITY',
 natural_person_flag BOOLEAN,
 primary_identifier_type STRING COMMENT 'SSN | EIN | ITIN | TIN | PASSPORT',
 primary_identifier_hash STRING COMMENT 'SHA-256 of PII per CAT privacy requirements',
 birth_year INT COMMENT 'CAT allows year only',
 residence_country STRING COMMENT 'ISO 3166-1 alpha-2',
 postal_code STRING COMMENT 'First 5 digits for CAT privacy',
 account_type STRING,
 account_status STRING,
 account_open_date DATE,
 account_close_date DATE,
 trading_authorization STRING COMMENT 'For TradingRelationship records',
 authorized_trader_fdid STRING,
 relationship_start_date DATE,
 relationship_end_date DATE,
 large_trader_flag BOOLEAN COMMENT 'Regulation 13H',
 large_trader_id STRING COMMENT 'SEC-assigned LTID (first 8 chars)',
 effective_start_date DATE NOT NULL COMMENT 'Record effective date',
 dv2_source_hk STRING NOT NULL COMMENT 'Silver customer link hash - BCBS 239 lineage',
 source_system STRING,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'CAIS customer/account/relationship fact - daily snapshot grain'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true',
 'delta.autoOptimize.optimizeWrite'='true',
 'compression.codec'='zstd',
 'subject_area'='gold_fact',
 'cat_submission_file_type'='CAIS',
 'grain'='one row per CAIS record per snapshot');

-- ----------------------------------------------------------------------------
-- Z-ORDER optimisation - run nightly post-load
-- ----------------------------------------------------------------------------
-- OPTIMIZE fact_cat_order_events ZORDER BY (dim_party_sk, cat_event_code, dim_instrument_sk);
-- OPTIMIZE fact_cat_allocations ZORDER BY (dim_party_sk, cat_order_id);
-- OPTIMIZE fact_cat_quotes ZORDER BY (dim_party_sk, dim_instrument_sk);
-- OPTIMIZE fact_cat_customer_records ZORDER BY (dim_party_sk, fdid);
