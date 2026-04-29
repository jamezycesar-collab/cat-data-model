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
-- File: dv2_satellites.sql
-- Purpose: Data Vault 2.0 Silver - Satellite Tables (~52 satellites)
-- Pattern: {parent}_hk + load_date (start) + load_end_date (NULL=current)
-- + hash_diff (SHA-256 of all descriptive cols) + record_source + attrs
-- SCD: Type 2 via hash_diff delta detection - append new row when diff changes
-- Regulatory:
-- * Satellites on hub_order, hub_execution, hub_allocation, hub_confirmation,
-- hub_clearing, hub_settlement, hub_submission are APPEND-ONLY
-- (line 785 - SEC Rule 613 / FINRA CAT requires immutable audit)
-- * hash_diff allows SCD2-style "change detection" without updating prior rows
-- * For append-only hubs, load_end_date is omitted / deprecated - every new row
-- is a new observation, prior rows remain the system-of-record for their t
-- Retention: 6 years (BCBS 239 / SEC 17a-4)
-- ============================================================================

-- ============================================================================
-- SECTION 1: HUB SATELLITES - PARTY DOMAIN
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Sat 1: sat_party_details - core party attributes
-- Parent: hub_party
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_party_details (
 party_hk STRING NOT NULL COMMENT 'FK to hub_party.party_hk',
 load_date TIMESTAMP NOT NULL COMMENT 'Validity start (UTC)',
 load_end_date TIMESTAMP COMMENT 'NULL = current version',
 hash_diff STRING NOT NULL COMMENT 'SHA-256 of descriptive cols for change detection',
 record_source STRING NOT NULL COMMENT 'Source system lineage',
 party_type STRING COMMENT 'LEGAL_ENTITY | NATURAL_PERSON',
 party_name STRING COMMENT 'Legal/registered name',
 legal_form STRING COMMENT 'ISO 20275 ELF code',
 jurisdiction_country STRING COMMENT 'ISO 3166-1 alpha-2',
 registration_date DATE COMMENT 'Entity incorporation / person registration',
 lei_status STRING COMMENT 'ISSUED | LAPSED | RETIRED | MERGED'
)
USING DELTA
COMMENT 'Descriptive attributes of Party - SCD2 via hash_diff'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 2: sat_party_classification - KYC/AML/regulatory classification
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_party_classification (
 party_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 kyc_status STRING COMMENT 'APPROVED | PENDING | REJECTED | PERIODIC_REVIEW',
 kyc_refresh_date DATE,
 aml_risk_rating STRING COMMENT 'LOW | MEDIUM | HIGH | PROHIBITED',
 sanctions_flag BOOLEAN,
 pep_flag BOOLEAN COMMENT 'Politically Exposed Person',
 fatca_status STRING COMMENT 'FFI | NFFE | EXEMPT',
 crs_status STRING,
 tax_jurisdiction STRING COMMENT 'ISO 3166-1 alpha-2'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Classification/KYC/AML of party - refreshed on periodic review'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 3: sat_party_contact - address & contact (PII subject to retention rules)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_party_contact (
 party_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 primary_address_line1 STRING,
 primary_address_line2 STRING,
 primary_city STRING,
 primary_state_province STRING,
 primary_postal_code STRING,
 primary_country STRING COMMENT 'ISO 3166-1 alpha-2',
 primary_phone STRING,
 primary_email STRING,
 pii_classification STRING COMMENT 'RESTRICTED | CONFIDENTIAL'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Contact attributes - GDPR/CCPA-sensitive; mask via Unity Catalog'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 4: sat_account_details
-- Parent: hub_account
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_account_details (
 account_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 account_type STRING COMMENT 'CUSTOMER | PROPRIETARY | OMNIBUS | ERROR | PAB',
 account_name STRING,
 base_currency STRING COMMENT 'ISO 4217',
 open_date DATE,
 close_date DATE,
 margin_flag BOOLEAN,
 options_level STRING COMMENT 'Level 1-5',
 pattern_day_trader_flag BOOLEAN,
 short_sale_flag BOOLEAN
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Account attributes - SCD2'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 5: sat_account_status
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_account_status (
 account_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 account_status STRING COMMENT 'ACTIVE | FROZEN | CLOSED | RESTRICTED | REG_T_CALL',
 restriction_reason STRING,
 restriction_start_date DATE,
 restriction_end_date DATE
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Operational status of account'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 6: sat_desk_details
-- Parent: hub_desk
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_desk_details (
 desk_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 desk_name STRING,
 desk_asset_class STRING COMMENT 'EQUITIES | FIXED_INCOME | FX | RATES | CREDIT | COMMODITIES',
 desk_region STRING COMMENT 'AMER | EMEA | APAC',
 legal_entity_hk STRING COMMENT 'FK to hub_party.party_hk of parent legal entity',
 supervisor_party_hk STRING COMMENT 'FK to hub_party.party_hk of supervising trader',
 active_flag BOOLEAN
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Desk attributes - active flag triggers information-barrier updates'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ============================================================================
-- SECTION 2: HUB SATELLITES - AGREEMENT DOMAIN
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Sat 7: sat_agreement_details
-- Parent: hub_agreement
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_agreement_details (
 agreement_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 agreement_type STRING COMMENT 'ISDA_MASTER | CSA | GMRA | GMSLA | PB | GIVE_UP',
 agreement_version STRING COMMENT 'e.g. ISDA 2002, GMRA 2011',
 execution_date DATE,
 effective_date DATE,
 termination_date DATE,
 governing_law STRING COMMENT 'ISO 3166-1 alpha-2',
 jurisdiction STRING,
 agreement_currency STRING COMMENT 'ISO 4217'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Agreement headers - rare updates (amendments)'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 8: sat_agreement_terms - negotiated economic terms
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_agreement_terms (
 agreement_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 threshold_amount DECIMAL(20,4),
 minimum_transfer_amount DECIMAL(20,4),
 rounding_amount DECIMAL(20,4),
 independent_amount DECIMAL(20,4),
 eligible_collateral STRING COMMENT 'Pipe-separated eligible collateral types',
 margin_frequency STRING COMMENT 'DAILY | WEEKLY | INTRADAY',
 notification_deadline STRING COMMENT 'Time of day for margin notification',
 haircut_schedule_id STRING,
 netting_eligible BOOLEAN
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Economic terms - CSA/collateral specifics'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 9: sat_agreement_status
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_agreement_status (
 agreement_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 agreement_status STRING COMMENT 'DRAFT | EXECUTED | ACTIVE | AMENDED | TERMINATED | VOID',
 amendment_count INT,
 last_amendment_date DATE,
 termination_reason STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Agreement lifecycle state - matches state_machines.md section 17'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ============================================================================
-- SECTION 3: HUB SATELLITES - INSTRUMENT DOMAIN
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Sat 10: sat_instrument_details
-- Parent: hub_instrument
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_instrument_details (
 instrument_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 ticker STRING,
 instrument_name STRING,
 cfi_code STRING COMMENT 'ISO 10962 CFI code (6 chars)',
 primary_currency STRING COMMENT 'ISO 4217',
 issue_date DATE,
 maturity_date DATE,
 issuer_party_hk STRING COMMENT 'FK to hub_party.party_hk of issuer',
 face_value DECIMAL(20,4),
 contract_size DECIMAL(20,4)
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Instrument attributes - descriptive'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 11: sat_instrument_classification
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_instrument_classification (
 instrument_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 asset_class STRING COMMENT 'EQUITY | FIXED_INCOME | FX | COMMODITY | DERIVATIVE | FUND',
 instrument_type STRING COMMENT 'CS | CORP_BOND | TSY | OPT | FUT | SWAP | ETF | MF',
 sub_type STRING,
 icb_code STRING COMMENT 'Industry Classification Benchmark',
 sic_code STRING,
 credit_rating_moody STRING,
 credit_rating_sp STRING,
 credit_rating_fitch STRING,
 esg_rating STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Classification - sector/rating/ESG (can update as ratings refresh)'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 12: sat_instrument_status
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_instrument_status (
 instrument_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 listing_status STRING COMMENT 'LISTED | DELISTED | SUSPENDED | HALT',
 trading_status STRING COMMENT 'ACTIVE | HALT | SUSPENDED | CIRCUIT_BREAKER',
 halt_reason STRING,
 active_flag BOOLEAN,
 matured_flag BOOLEAN,
 reg_sho_threshold_flag BOOLEAN COMMENT 'Regulation SHO Threshold Security',
 hard_to_borrow_flag BOOLEAN
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Trading status of instrument - frequently updated intraday'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 13: sat_instrument_derivative_terms (for derivative_instrument subtype)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_instrument_derivative_terms (
 instrument_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 derivative_type STRING COMMENT 'OPTION | FUTURE | SWAP | FORWARD | CFD',
 strike_price DECIMAL(20,4),
 option_type STRING COMMENT 'CALL | PUT',
 expiration_date DATE,
 exercise_style STRING COMMENT 'AMERICAN | EUROPEAN | BERMUDAN | ASIAN',
 settlement_type STRING COMMENT 'PHYSICAL | CASH',
 underlying_instrument_hk STRING COMMENT 'FK to hub_instrument of underlying',
 multiplier DECIMAL(20,4)
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Derivative-specific terms (options/futures/swaps)'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ============================================================================
-- SECTION 4: HUB SATELLITES - VENUE DOMAIN
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Sat 14: sat_venue_details
-- Parent: hub_venue
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_venue_details (
 venue_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 mic_code STRING COMMENT 'ISO 10383 operating MIC',
 mic_segment STRING COMMENT 'ISO 10383 segment MIC',
 venue_name STRING,
 venue_type STRING COMMENT 'EXCHANGE | MTF | OTF | SI | DARK_POOL | ATS | ECN | OTC',
 country_code STRING COMMENT 'ISO 3166-1 alpha-2',
 operator_party_hk STRING COMMENT 'FK to hub_party of venue operator',
 time_zone STRING,
 active_flag BOOLEAN
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Venue descriptive attributes - sourced from ISO 10383 MIC list'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 15: sat_venue_connectivity
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_venue_connectivity (
 venue_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 fix_version_supported STRING COMMENT 'Pipe-list: 4.2|4.4|5.0',
 binary_protocol STRING COMMENT 'ITCH | OUCH | SAIL | CME_MDP',
 session_open_time STRING COMMENT 'Local time HH:MM',
 session_close_time STRING,
 pre_open_time STRING,
 auction_times STRING COMMENT 'Opening/closing auctions',
 firm_sender_comp_id STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Venue connectivity attributes - trading session times'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ============================================================================
-- SECTION 5: HUB SATELLITES - PRE-TRADE DOMAIN
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Sat 16: sat_order_request_details (MEIR/MOIR CAT event)
-- Parent: hub_order_request
-- APPEND-ONLY: request events are immutable
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_order_request_details (
 order_request_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL COMMENT 'Event capture timestamp (APPEND-ONLY)',
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 cat_event_type STRING COMMENT 'MEIR | MOIR',
 request_timestamp TIMESTAMP COMMENT 'Customer request time (milli/micro per Phase 2d)',
 systematized_timestamp TIMESTAMP COMMENT 'Time when request became an order',
 side STRING COMMENT 'BUY | SELL | SELL_SHORT | SELL_SHORT_EXEMPT',
 order_intent STRING,
 requested_quantity DECIMAL(20,4),
 requested_price DECIMAL(20,8),
 tif STRING COMMENT 'DAY | GTC | IOC | FOK | GTD | AT_OPEN | AT_CLOSE',
 customer_display_flag BOOLEAN,
 tid_type STRING COMMENT 'Phase 2d Trader ID type',
 correspondent_crd STRING COMMENT 'Phase 2d correspondent CRD'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Order Request details (MEIR/MOIR) - APPEND-ONLY per SEC Rule 613'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 17: sat_order_request_status
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_order_request_status (
 order_request_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 request_status STRING COMMENT 'RECEIVED | SYSTEMATIZED | ORDER_CREATED | REJECTED | EXPIRED',
 rejection_reason STRING,
 clock_sync_breach_flag BOOLEAN COMMENT 'TRUE if clock sync breach >50ms electronic / >1s manual'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Order Request status - matches state_machines.md section 1'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 18: sat_quote_request_details (MEQR CAT event)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_quote_request_details (
 quote_request_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 cat_event_type STRING COMMENT 'MEQR',
 request_timestamp TIMESTAMP,
 requesting_party_hk STRING,
 instrument_hk STRING,
 side STRING COMMENT 'BUY | SELL | TWO_WAY',
 requested_quantity DECIMAL(20,4),
 expire_timestamp TIMESTAMP,
 rfq_channel STRING COMMENT 'VOICE | ELECTRONIC | REQUEST_FOR_STREAM'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'RFQ request details - APPEND-ONLY per CAT'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 19: sat_quote_request_status
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_quote_request_status (
 quote_request_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 request_status STRING COMMENT 'OPEN | ANSWERED | EXPIRED | CANCELLED',
 response_count INT
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'RFQ state (state_machines.md section 5)'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 20: sat_quote_details (MEQS CAT event)
-- Parent: hub_quote
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_quote_details (
 quote_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 cat_event_type STRING COMMENT 'MEQS',
 quote_timestamp TIMESTAMP,
 quoting_party_hk STRING,
 instrument_hk STRING,
 bid_price DECIMAL(20,8),
 ask_price DECIMAL(20,8),
 bid_quantity DECIMAL(20,4),
 ask_quantity DECIMAL(20,4),
 validity_end TIMESTAMP,
 prior_quote_hk STRING COMMENT 'Populated on refresh (SUPERSEDED)',
 venue_hk STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Quote details (MEQS) - APPEND-ONLY'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 21: sat_quote_status
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_quote_status (
 quote_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 quote_status STRING COMMENT 'ACTIVE | EXECUTED | SUPERSEDED | CANCELLED | EXPIRED'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Quote lifecycle (state_machines.md section 6)'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 22: sat_rfe_details (Request For Execution)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_rfe_details (
 rfe_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 request_timestamp TIMESTAMP,
 requesting_party_hk STRING,
 receiving_party_hk STRING,
 instrument_hk STRING,
 side STRING,
 quantity DECIMAL(20,4),
 requested_price DECIMAL(20,8),
 execution_instructions STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'RFE details - APPEND-ONLY'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 23: sat_rfe_status
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_rfe_status (
 rfe_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 rfe_status STRING COMMENT 'RECEIVED | ACCEPTED | REJECTED | EXPIRED',
 resulting_order_hk STRING COMMENT 'Populated when ACCEPTED'
)
USING DELTA
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 24: sat_ioi_details (Indication Of Interest)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_ioi_details (
 ioi_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 originator_party_hk STRING,
 instrument_hk STRING,
 side STRING,
 max_quantity DECIMAL(20,4),
 min_quantity DECIMAL(20,4),
 ioi_quality STRING COMMENT 'HIGH | MEDIUM | LOW',
 ioi_natural_flag BOOLEAN COMMENT 'TRUE = natural (client), FALSE = principal',
 price_type STRING,
 indicative_price DECIMAL(20,8),
 venue_hk STRING,
 expire_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'IOI details - updates permitted (withdrawals/refresh)'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 25: sat_ioi_status
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_ioi_status (
 ioi_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 ioi_status STRING COMMENT 'ACTIVE | WITHDRAWN | EXPIRED',
 withdrawn_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ============================================================================
-- SECTION 6: HUB SATELLITES - ORDER DOMAIN (APPEND-ONLY: SEC Rule 613)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Sat 26: sat_order_details (MENO/MONO/MLNO/MECO)
-- Parent: hub_order
-- *** APPEND-ONLY *** per line 785
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_order_details (
 order_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL COMMENT 'Event capture timestamp - immutable',
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 cat_event_type STRING COMMENT 'MENO | MONO | MLNO | MECO',
 order_timestamp TIMESTAMP,
 client_order_id STRING,
 broker_order_id STRING,
 instrument_hk STRING,
 account_hk STRING,
 side STRING,
 order_quantity DECIMAL(20,4),
 order_type STRING COMMENT 'MARKET | LIMIT | STOP | STOP_LIMIT | PEG | MOC',
 time_in_force STRING,
 limit_price DECIMAL(20,8),
 stop_price DECIMAL(20,8),
 parent_order_hk STRING COMMENT 'MECO/MLCO parent order linkage',
 order_handling_instruction STRING,
 solicitation_flag STRING,
 anonymity_flag BOOLEAN,
 display_quantity DECIMAL(20,4),
 new_order_fdid STRING COMMENT 'Phase 2d new order FDID'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Order details - APPEND-ONLY (insert new row per event, never update)'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 27: sat_order_routing (MEOR/MEIR CAT)
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_order_routing (
 order_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 route_timestamp TIMESTAMP,
 destination_venue_hk STRING,
 routing_broker_hk STRING,
 route_type STRING COMMENT 'DIRECT | SOR | WHEEL | MANUAL',
 algo_strategy STRING COMMENT 'VWAP | TWAP | POV | IS | DMA',
 route_status STRING COMMENT 'SENT | ACKNOWLEDGED | ROUTED_AT_VENUE | REJECTED | CANCELLED'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Order routing leg - APPEND-ONLY; matches state_machines.md section 3'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 28: sat_order_status
-- APPEND-ONLY (status changes recorded as new event rows)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_order_status (
 order_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 status_timestamp TIMESTAMP,
 order_status STRING COMMENT 'NEW | ACCEPTED | WORKING | PARTIALLY_FILLED | FILLED | CANCELLED | REJECTED | EXPIRED | BUST | MODIFIED',
 cumulative_qty DECIMAL(20,4),
 remaining_quantity DECIMAL(20,4),
 avg_price DECIMAL(20,8),
 rejection_reason STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Order status timeline - APPEND-ONLY; state_machines.md section 2'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 29: sat_order_modification (MEOM/MEIM/MLIM/MEOMR)
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_order_modification (
 order_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 cat_event_type STRING COMMENT 'MEOM | MEIM | MLIM | MEOMR',
 request_timestamp TIMESTAMP COMMENT 'Customer request time (MEOMR)',
 event_timestamp TIMESTAMP COMMENT 'Broker acceptance time (MEOM)',
 prior_order_id STRING COMMENT 'Original ClOrdID / OrigClOrdID',
 new_quantity DECIMAL(20,4),
 new_limit_price DECIMAL(20,8),
 new_stop_price DECIMAL(20,8),
 new_tif STRING,
 modification_status STRING COMMENT 'REQUESTED | ACCEPTED | APPLIED_TO_ORDER | REJECTED'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Order modifications - APPEND-ONLY; state_machines.md section 4'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ============================================================================
-- SECTION 7: HUB SATELLITES - EXECUTION DOMAIN (APPEND-ONLY)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Sat 30: sat_execution_details (MEOT (Trade) and MEOTS (Trade Supplement)/MOOT/Section 5.2 multi-leg events/)
-- *** APPEND-ONLY ***
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_execution_details (
 execution_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 cat_event_type STRING COMMENT 'MEOT | MEOTS | MOOT | Section 5.2 multi-leg events | ',
 trade_date DATE,
 execution_timestamp TIMESTAMP,
 order_hk STRING,
 execution_venue_hk STRING,
 contra_party_hk STRING,
 execution_quantity DECIMAL(20,4),
 execution_price DECIMAL(20,8),
 side STRING,
 settlement_date DATE,
 last_market STRING COMMENT 'FIX tag 30 - MIC'
)
USING DELTA
PARTITIONED BY (record_source, trade_date)
COMMENT 'Execution fills - APPEND-ONLY; FINRA CAT MEOT event'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 31: sat_execution_classification
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_execution_classification (
 execution_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 execution_type STRING COMMENT 'NEW | PARTIAL | FULL | BUST | CORRECT | CANCEL',
 capacity STRING COMMENT 'A=Agency | P=Principal | R=RisklessPrincipal',
 liquidity_indicator STRING COMMENT '1=Added | 2=Removed | 3=LiquidityRouted | 4=Auction',
 trd_type STRING COMMENT 'Regular | Block | EFP | Transfer | LateTrade | OffHours',
 exec_restatement_reason STRING,
 iso_flag BOOLEAN COMMENT 'Intermarket Sweep Order'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Execution type/capacity/liquidity - APPEND-ONLY'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 32: sat_execution_reversal (bust/correct adjustments)
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_execution_reversal (
 execution_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 original_execution_hk STRING COMMENT 'Execution being busted/corrected',
 reversal_type STRING COMMENT 'BUST | CORRECT',
 reversal_reason STRING,
 reversal_timestamp TIMESTAMP,
 reversed_by_party_hk STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Bust/correct linkage - APPEND-ONLY'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ============================================================================
-- SECTION 8: HUB SATELLITES - ALLOCATION / CONFIRMATION / CLEARING / SETTLE
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Sat 33: sat_allocation_details (MEAA)
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_allocation_details (
 allocation_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 allocation_timestamp TIMESTAMP,
 execution_hk STRING,
 order_hk STRING,
 allocated_account_hk STRING,
 allocated_party_hk STRING,
 allocated_quantity DECIMAL(20,4),
 allocated_price DECIMAL(20,8),
 allocation_type STRING COMMENT 'STEP_OUT | STEP_IN | GIVE_UP | BLOCK',
 allocation_sequence INT COMMENT 'Ordinal within a block',
 average_price_flag BOOLEAN
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Allocation details - APPEND-ONLY'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 34: sat_allocation_status
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_allocation_status (
 allocation_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 allocation_status STRING COMMENT 'PENDING | CONFIRMED | REJECTED | AMENDED | CANCELLED',
 rejection_reason STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Allocation lifecycle - state_machines.md section 10'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 35: sat_confirmation_details
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_confirmation_details (
 confirmation_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 confirmation_timestamp TIMESTAMP,
 allocation_hk STRING,
 execution_hk STRING,
 confirming_party_hk STRING,
 counterparty_hk STRING,
 confirmation_type STRING COMMENT 'NEW | REPLACE | CANCEL',
 affirmation_timestamp TIMESTAMP COMMENT 'SEC Rule 15c6-2 same-day affirm target',
 settlement_instruction_hk STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Trade confirmation - APPEND-ONLY; SEC Rule 15c6-2 compliance'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 36: sat_confirmation_status
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_confirmation_status (
 confirmation_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 confirmation_status STRING COMMENT 'UNCONFIRMED | AFFIRMED | DK | DISPUTED | CANCELLED',
 match_status STRING COMMENT 'UNMATCHED | MATCHED | ALLEGED',
 dk_reason STRING,
 dispute_reason STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Confirmation match + affirm status - state_machines.md section 11'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 37: sat_clearing_details
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_clearing_details (
 clearing_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 clearing_timestamp TIMESTAMP,
 execution_hk STRING,
 allocation_hk STRING,
 ccp_party_hk STRING COMMENT 'OCC | NSCC | FICC | LCH | CME',
 clearing_member_party_hk STRING,
 netting_set_hk STRING,
 clearing_account STRING,
 novation_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Clearing submission - APPEND-ONLY'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 38: sat_clearing_status
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_clearing_status (
 clearing_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 clearing_status STRING COMMENT 'SUBMITTED | ACCEPTED | NOVATED | REJECTED',
 rejection_reason STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Clearing lifecycle - state_machines.md section 12'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 39: sat_settlement_details
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_settlement_details (
 settlement_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 settlement_instruction_timestamp TIMESTAMP,
 allocation_hk STRING,
 clearing_hk STRING,
 custodian_party_hk STRING,
 contractual_settle_date DATE COMMENT 'T+1 per DTCC 2024-05-28',
 actual_settle_date DATE,
 settlement_location STRING COMMENT 'DTCC | EUROCLEAR | CLEARSTREAM | FED',
 settlement_currency STRING COMMENT 'ISO 4217',
 settle_net_amount DECIMAL(20,4),
 delivery_vs_payment BOOLEAN
)
USING DELTA
PARTITIONED BY (record_source, contractual_settle_date)
COMMENT 'Settlement instruction - APPEND-ONLY'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 40: sat_settlement_status
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_settlement_status (
 settlement_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 settlement_status STRING COMMENT 'INSTRUCTED | MATCHED | UNMATCHED | SETTLING | SETTLED | PARTIAL | FAILED',
 fail_reason STRING,
 csdr_penalty_flag BOOLEAN COMMENT 'CSDR cash penalty trigger'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Settlement lifecycle - state_machines.md section 13'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ============================================================================
-- SECTION 9: HUB SATELLITES - REGULATORY SUBMISSION & FINANCING
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Sat 41: sat_submission_details
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_submission_details (
 submission_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 submission_timestamp TIMESTAMP,
 submission_type STRING COMMENT 'CAT | OATS | TRACE | MSRB | BLUE_SHEETS | EMIR | MiFIR',
 regulator STRING COMMENT 'FINRA | SEC | CFTC | ESMA | FCA',
 reporting_party_hk STRING,
 report_date DATE,
 submission_file_name STRING,
 submission_batch_id STRING,
 record_count BIGINT
)
USING DELTA
PARTITIONED BY (record_source, report_date)
COMMENT 'Regulatory submission record - APPEND-ONLY for audit trail'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 42: sat_submission_status
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_submission_status (
 submission_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 report_status STRING COMMENT 'GENERATED | SUBMITTED | ACCEPTED | REJECTED | CORRECTED',
 rejection_count INT,
 rejection_reason STRING,
 correction_count INT
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Submission lifecycle - state_machines.md section 19'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 43: sat_repo_details
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_repo_details (
 repo_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 agreement_hk STRING COMMENT 'GMRA master',
 instrument_hk STRING COMMENT 'Collateral security',
 repo_type STRING COMMENT 'OVERNIGHT | TERM | OPEN | TRI_PARTY',
 trade_date DATE,
 start_date DATE,
 end_date DATE,
 repo_rate DECIMAL(10,6),
 haircut_pct DECIMAL(10,6),
 collateral_value DECIMAL(20,4),
 cash_amount DECIMAL(20,4),
 currency STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Repo transaction details under GMRA'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 44: sat_securities_loan_details
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_securities_loan_details (
 loan_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 agreement_hk STRING COMMENT 'GMSLA master',
 instrument_hk STRING,
 loan_start_date DATE,
 expected_return_date DATE,
 actual_return_date DATE,
 loan_quantity DECIMAL(20,4),
 loan_value DECIMAL(20,4),
 rebate_rate DECIMAL(10,6),
 fee_rate DECIMAL(10,6),
 collateral_type STRING COMMENT 'CASH | NON_CASH | MIXED',
 collateral_value DECIMAL(20,4)
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Securities loan under GMSLA'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ============================================================================
-- SECTION 10: LINK SATELLITES - descriptive attrs on relationships
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Sat 45: sat_link_party_hierarchy_control
-- Parent: link_party_hierarchy
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_link_party_hierarchy_control (
 link_hk STRING NOT NULL COMMENT 'FK to link_party_hierarchy.link_hk',
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 relationship_type STRING COMMENT 'OWNERSHIP | CONTROL | GUARANTEE | AGENCY',
 ownership_pct DECIMAL(10,6),
 voting_pct DECIMAL(10,6),
 beneficial_owner_flag BOOLEAN,
 ultimate_parent_flag BOOLEAN,
 start_date DATE,
 end_date DATE
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Ownership/control attributes of party hierarchy link'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 46: sat_link_party_account_role
-- Parent: link_party_account
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_link_party_account_role (
 link_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 role_type STRING COMMENT 'OWNER | AUTHORIZED_TRADER | BENEFICIAL_OWNER | POA',
 authorization_level STRING,
 start_date DATE,
 end_date DATE
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Party-account role descriptive'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 47: sat_link_party_agreement_terms
-- Parent: link_party_agreement
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_link_party_agreement_terms (
 link_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 party_role_in_agreement STRING COMMENT 'PRINCIPAL | COUNTERPARTY | GUARANTOR | PLEDGOR',
 negotiated_terms STRING COMMENT 'JSON of party-specific overrides',
 commission_schedule_id STRING,
 start_date DATE,
 end_date DATE
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Party-agreement relationship terms (overrides, commission schedule)'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 48: sat_link_execution_order_fill_attrs
-- Parent: link_execution_order - carries fill-progression attrs
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_link_execution_order_fill_attrs (
 link_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 fill_sequence INT COMMENT 'Ordinal fill on order',
 cumulative_qty DECIMAL(20,4),
 leaves_qty DECIMAL(20,4),
 fill_contribution_pct DECIMAL(10,6) COMMENT 'This fill / total order qty'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Fill linkage attributes - APPEND-ONLY'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 49: sat_link_allocation_account_allocation_pct
-- Parent: link_allocation_account
-- APPEND-ONLY
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_link_allocation_account_allocation_pct (
 link_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 allocation_pct DECIMAL(10,6),
 allocation_basis STRING COMMENT 'PRO_RATA | FIXED | AVERAGE_PRICE',
 average_price_indicator BOOLEAN
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Allocation percentage per account (block trade split)'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver', 'append_only_flag'='true');

-- ----------------------------------------------------------------------------
-- Sat 50: sat_link_clearing_execution_fees
-- Parent: link_clearing_execution
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_link_clearing_execution_fees (
 link_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 clearing_fee DECIMAL(20,4),
 regulatory_fee DECIMAL(20,4) COMMENT 'SEC Section 31 fee',
 taf_fee DECIMAL(20,4) COMMENT 'FINRA TAF',
 occ_fee DECIMAL(20,4),
 exchange_fee DECIMAL(20,4),
 clearing_currency STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Clearing-leg fee breakout per execution'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 51: sat_link_settlement_allocation_dates
-- Parent: link_settlement_allocation
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_link_settlement_allocation_dates (
 link_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 instructed_date DATE,
 matched_date DATE,
 expected_settle_date DATE,
 actual_settle_date DATE,
 days_to_settle INT,
 settle_fail_days INT
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Settlement-allocation timing - feeds CSDR penalty calc'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ----------------------------------------------------------------------------
-- Sat 52: sat_link_netting_set_terms
-- Parent: link_netting_set
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_link_netting_set_terms (
 link_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 netting_set_name STRING,
 close_out_netting BOOLEAN,
 settlement_netting BOOLEAN,
 cross_product_netting BOOLEAN,
 jurisdiction_enforceable BOOLEAN COMMENT 'Legal opinion received'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Netting set terms - drives counterparty credit risk calc'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver');

-- ============================================================================
-- END OF SATELLITES - 52 total
-- Summary:
-- Hub Satellites: 44 (covers 20 hubs, avg 2.2 sats per hub)
-- Link Satellites: 8 (covers the 8 highest-value relationship attribute sets)
-- Append-only satellites (SEC Rule 613 / FINRA CAT immutability): 23 total
-- order_request_details, quote_request_details, quote_details, rfe_details,
-- order_details, order_routing, order_status, order_modification,
-- execution_details, execution_classification, execution_reversal,
-- allocation_details, allocation_status,
-- confirmation_details, confirmation_status,
-- clearing_details, clearing_status,
-- settlement_details, settlement_status,
-- submission_details, submission_status,
-- link_execution_order_fill_attrs, link_allocation_account_allocation_pct
-- ============================================================================
