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
-- File: 08_fabric_warehouse_variant.sql
-- Purpose: T-SQL variant of Gold star schema for Microsoft Fabric Warehouse
-- Notes: - Type mapping: STRING -> NVARCHAR(MAX), TIMESTAMP -> DATETIME2(7),
-- BOOLEAN -> BIT, DECIMAL(p,s) -> DECIMAL(p,s), INT/BIGINT unchanged
-- - DISTRIBUTION = HASH(<fixed-width col>) for large facts
-- (cat_order_id / firm_roe_id / fdid are fixed-length business keys)
-- - DISTRIBUTION = ROUND_ROBIN for small dims and ops tables
-- - NVARCHAR(MAX) is NOT eligible as HASH distribution column - -- use fixed-width NVARCHAR(n) (cat_order_id NVARCHAR(40)) instead
-- - CLUSTERED COLUMNSTORE INDEX is default in Fabric Warehouse
-- ============================================================================

------------------------------------------------------------------------------
-- DIMENSIONS
------------------------------------------------------------------------------

CREATE TABLE dim_date (
 date_sk INT NOT NULL,
 calendar_date DATE NOT NULL,
 day_of_week INT NULL,
 day_name NVARCHAR(20) NULL,
 day_of_month INT NULL,
 day_of_year INT NULL,
 week_of_year INT NULL,
 month_number INT NULL,
 month_name NVARCHAR(20) NULL,
 quarter INT NULL,
 year_number INT NULL,
 fiscal_year INT NULL,
 fiscal_quarter INT NULL,
 is_trading_day BIT NOT NULL,
 is_settlement_day BIT NOT NULL,
 t_plus_1_settlement_date DATE NULL,
 t_plus_3_error_correction_deadline DATE NULL,
 cat_submission_deadline_timestamp DATETIME2(7) NULL,
 holiday_flag BIT NULL,
 holiday_name NVARCHAR(100) NULL
)
WITH (
 DISTRIBUTION = HASH(date_sk),
 CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dim_party (
 dim_party_sk BIGINT NOT NULL,
 party_id_bk NVARCHAR(40) NOT NULL,
 lei NVARCHAR(20) NULL,
 cat_imid NVARCHAR(8) NULL,
 crd NVARCHAR(20) NULL,
 fdid NVARCHAR(40) NULL,
 party_type NVARCHAR(40) NOT NULL,
 party_status NVARCHAR(20) NULL,
 legal_form NVARCHAR(20) NULL,
 incorporation_country NVARCHAR(2) NULL,
 domicile_country NVARCHAR(2) NULL,
 tax_residency NVARCHAR(2) NULL,
 ultimate_parent_party_id NVARCHAR(40) NULL,
 g_sib_flag BIT NULL,
 pep_flag BIT NULL,
 mifid_ii_category NVARCHAR(40) NULL,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_current BIT NOT NULL,
 row_hash NVARCHAR(64) NOT NULL,
 dv2_source_hk NVARCHAR(64) NULL,
 load_date DATETIME2(7) NOT NULL,
 record_source NVARCHAR(200) NOT NULL
)
WITH (
 DISTRIBUTION = HASH(dim_party_sk),
 CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dim_instrument (
 dim_instrument_sk BIGINT NOT NULL,
 instrument_id_bk NVARCHAR(40) NOT NULL,
 isin NVARCHAR(12) NULL,
 cusip NVARCHAR(9) NULL,
 figi NVARCHAR(12) NULL,
 symbol NVARCHAR(14) NULL,
 security_id NVARCHAR(12) NULL,
 security_id_source NVARCHAR(4) NULL,
 asset_class NVARCHAR(20) NULL,
 cfi_code NVARCHAR(6) NULL,
 issuer_party_id NVARCHAR(40) NULL,
 currency_code NVARCHAR(3) NULL,
 primary_listing_mic NVARCHAR(4) NULL,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_current BIT NOT NULL,
 row_hash NVARCHAR(64) NOT NULL,
 dv2_source_hk NVARCHAR(64) NULL,
 load_date DATETIME2(7) NOT NULL,
 record_source NVARCHAR(200) NOT NULL
)
WITH (
 DISTRIBUTION = HASH(dim_instrument_sk),
 CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dim_venue (
 dim_venue_sk BIGINT NOT NULL,
 venue_id_bk NVARCHAR(40) NOT NULL,
 mic NVARCHAR(4) NULL,
 venue_name NVARCHAR(200) NULL,
 venue_category NVARCHAR(20) NULL,
 country_code NVARCHAR(2) NULL,
 operating_segment NVARCHAR(10) NULL,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_current BIT NOT NULL,
 row_hash NVARCHAR(64) NOT NULL,
 dv2_source_hk NVARCHAR(64) NULL,
 load_date DATETIME2(7) NOT NULL,
 record_source NVARCHAR(200) NOT NULL
)
WITH (
 DISTRIBUTION = ROUND_ROBIN,
 CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dim_account (
 dim_account_sk BIGINT NOT NULL,
 account_id_bk NVARCHAR(40) NOT NULL,
 account_type NVARCHAR(20) NULL,
 account_status NVARCHAR(20) NULL,
 custodian_id NVARCHAR(40) NULL,
 clearing_member_id NVARCHAR(40) NULL,
 cat_large_trader_flag BIT NULL,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_current BIT NOT NULL,
 row_hash NVARCHAR(64) NOT NULL,
 dv2_source_hk NVARCHAR(64) NULL,
 load_date DATETIME2(7) NOT NULL,
 record_source NVARCHAR(200) NOT NULL
)
WITH (
 DISTRIBUTION = HASH(dim_account_sk),
 CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dim_event_type (
 dim_event_type_sk INT NOT NULL,
 cat_event_code NVARCHAR(10) NOT NULL,
 event_name NVARCHAR(80) NOT NULL,
 submission_file_type NVARCHAR(12) NOT NULL,
 event_phase NVARCHAR(40) NOT NULL,
 event_description NVARCHAR(400) NULL,
 is_cais_event BIT NULL,
 cat_spec_version NVARCHAR(10) NULL,
 load_date DATETIME2(7) NOT NULL,
 record_source NVARCHAR(200) NOT NULL
)
WITH (
 DISTRIBUTION = ROUND_ROBIN,
 CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dim_desk (
 dim_desk_sk BIGINT NOT NULL,
 desk_id_bk NVARCHAR(40) NOT NULL,
 desk_name NVARCHAR(100) NULL,
 desk_hierarchy NVARCHAR(400) NULL,
 region NVARCHAR(40) NULL,
 asset_class_coverage NVARCHAR(100) NULL,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_current BIT NOT NULL,
 row_hash NVARCHAR(64) NOT NULL,
 dv2_source_hk NVARCHAR(64) NULL,
 load_date DATETIME2(7) NOT NULL,
 record_source NVARCHAR(200) NOT NULL
)
WITH (
 DISTRIBUTION = ROUND_ROBIN,
 CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dim_trader (
 dim_trader_sk BIGINT NOT NULL,
 trader_id_bk NVARCHAR(40) NOT NULL,
 cat_trader_id NVARCHAR(40) NULL,
 given_name_hash NVARCHAR(64) NULL,
 family_name_hash NVARCHAR(64) NULL,
 desk_id_bk NVARCHAR(40) NULL,
 active_flag BIT NULL,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_current BIT NOT NULL,
 row_hash NVARCHAR(64) NOT NULL,
 dv2_source_hk NVARCHAR(64) NULL,
 load_date DATETIME2(7) NOT NULL,
 record_source NVARCHAR(200) NOT NULL
)
WITH (
 DISTRIBUTION = HASH(dim_trader_sk),
 CLUSTERED COLUMNSTORE INDEX
);

------------------------------------------------------------------------------
-- FACTS
------------------------------------------------------------------------------

-- NOTE: event_date is NOT used for HASH distribution (Fabric Warehouse does
-- not support partitioning via DATE partitioning; use HASH on a fixed-width
-- business key and rely on columnstore row groups for pruning instead).

CREATE TABLE fact_cat_order_events (
 order_event_fact_sk BIGINT NOT NULL,
 event_date DATE NOT NULL,
 firm_roe_id NVARCHAR(40) NOT NULL,
 cat_order_id NVARCHAR(40) NOT NULL,
 cat_event_code NVARCHAR(10) NOT NULL,
 dim_party_sk BIGINT NOT NULL,
 dim_instrument_sk BIGINT NOT NULL,
 dim_venue_sk BIGINT NULL,
 dim_account_sk BIGINT NULL,
 dim_desk_sk BIGINT NULL,
 dim_trader_sk BIGINT NULL,
 counterparty_party_sk BIGINT NULL,
 event_timestamp DATETIME2(7) NOT NULL,
 event_nanos BIGINT NULL,
 received_timestamp DATETIME2(7) NULL,
 route_timestamp DATETIME2(7) NULL,
 side NVARCHAR(20) NULL,
 order_type NVARCHAR(12) NULL,
 order_capacity NVARCHAR(20) NULL,
 time_in_force NVARCHAR(3) NULL,
 quantity DECIMAL(38,10) NULL,
 leaves_quantity DECIMAL(38,10) NULL,
 cumulative_filled_quantity DECIMAL(38,10) NULL,
 price DECIMAL(38,10) NULL,
 execution_price DECIMAL(38,10) NULL,
 execution_quantity DECIMAL(38,10) NULL,
 currency_code NVARCHAR(3) NULL,
 handling_instructions NVARCHAR(10) NULL,
 special_handling_codes NVARCHAR(80) NULL,
 display_instruction NVARCHAR(15) NULL,
 iso_flag BIT NULL,
 short_sale_exempt_flag BIT NULL,
 solicited_flag BIT NULL,
 directed_flag BIT NULL,
 parent_order_id NVARCHAR(40) NULL,
 routed_to_venue_mic NVARCHAR(4) NULL,
 routed_to_firm_imid NVARCHAR(8) NULL,
 original_event_ref NVARCHAR(40) NULL,
 correction_reason NVARCHAR(255) NULL,
 cancel_reason NVARCHAR(255) NULL,
 rejection_reason NVARCHAR(255) NULL,
 dv2_source_hk NVARCHAR(64) NOT NULL,
 source_system NVARCHAR(40) NULL,
 load_date DATETIME2(7) NOT NULL,
 record_source NVARCHAR(200) NOT NULL
)
WITH (
 DISTRIBUTION = HASH(cat_order_id),
 CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE fact_cat_allocations (
 allocation_fact_sk BIGINT NOT NULL,
 event_date DATE NOT NULL,
 firm_roe_id NVARCHAR(40) NOT NULL,
 cat_order_id NVARCHAR(40) NOT NULL,
 cat_allocation_id NVARCHAR(40) NULL,
 cat_event_code NVARCHAR(10) NOT NULL,
 dim_party_sk BIGINT NOT NULL,
 dim_instrument_sk BIGINT NOT NULL,
 dim_account_sk BIGINT NOT NULL,
 dim_desk_sk BIGINT NULL,
 parent_execution_id NVARCHAR(40) NULL,
 allocation_method NVARCHAR(12) NULL,
 allocation_status NVARCHAR(12) NULL,
 allocated_quantity DECIMAL(38,10) NOT NULL,
 allocated_price DECIMAL(38,10) NULL,
 allocation_pct DECIMAL(18,10) NULL,
 gross_amount DECIMAL(38,10) NULL,
 commission DECIMAL(38,10) NULL,
 commission_currency NVARCHAR(3) NULL,
 net_amount DECIMAL(38,10) NULL,
 settlement_date DATE NULL,
 settlement_currency NVARCHAR(3) NULL,
 affirmation_timestamp DATETIME2(7) NULL,
 allocation_timestamp DATETIME2(7) NOT NULL,
 allocation_nanos BIGINT NULL,
 dv2_source_hk NVARCHAR(64) NOT NULL,
 source_system NVARCHAR(40) NULL,
 load_date DATETIME2(7) NOT NULL,
 record_source NVARCHAR(200) NOT NULL
)
WITH (
 DISTRIBUTION = HASH(cat_order_id),
 CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE fact_cat_quotes (
 quote_fact_sk BIGINT NOT NULL,
 event_date DATE NOT NULL,
 firm_roe_id NVARCHAR(40) NOT NULL,
 cat_quote_id NVARCHAR(40) NOT NULL,
 cat_rfq_id NVARCHAR(40) NULL,
 cat_event_code NVARCHAR(10) NOT NULL,
 dim_party_sk BIGINT NOT NULL,
 counterparty_party_sk BIGINT NULL,
 dim_instrument_sk BIGINT NOT NULL,
 dim_venue_sk BIGINT NULL,
 dim_trader_sk BIGINT NULL,
 quote_side NVARCHAR(8) NULL,
 bid_price DECIMAL(38,10) NULL,
 ask_price DECIMAL(38,10) NULL,
 bid_size DECIMAL(38,10) NULL,
 ask_size DECIMAL(38,10) NULL,
 currency_code NVARCHAR(3) NULL,
 quote_status NVARCHAR(12) NULL,
 quote_type NVARCHAR(12) NULL,
 min_quote_life_ms INT NULL,
 quote_expiry_timestamp DATETIME2(7) NULL,
 rfq_expiry_timestamp DATETIME2(7) NULL,
 quote_timestamp DATETIME2(7) NOT NULL,
 quote_nanos BIGINT NULL,
 dv2_source_hk NVARCHAR(64) NOT NULL,
 source_system NVARCHAR(40) NULL,
 load_date DATETIME2(7) NOT NULL,
 record_source NVARCHAR(200) NOT NULL
)
WITH (
 DISTRIBUTION = HASH(cat_quote_id),
 CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE fact_cat_customer_records (
 cais_fact_sk BIGINT NOT NULL,
 snapshot_date DATE NOT NULL,
 event_date DATE NOT NULL,
 firm_roe_id NVARCHAR(40) NOT NULL,
 fdid NVARCHAR(40) NOT NULL,
 cat_event_code NVARCHAR(10) NOT NULL,
 dim_party_sk BIGINT NOT NULL,
 dim_account_sk BIGINT NOT NULL,
 record_type NVARCHAR(30) NOT NULL,
 action_type NVARCHAR(8) NOT NULL,
 customer_type NVARCHAR(12) NULL,
 natural_person_flag BIT NULL,
 primary_identifier_type NVARCHAR(10) NULL,
 primary_identifier_hash NVARCHAR(64) NULL,
 birth_year INT NULL,
 residence_country NVARCHAR(2) NULL,
 postal_code NVARCHAR(5) NULL,
 account_type NVARCHAR(20) NULL,
 account_status NVARCHAR(12) NULL,
 account_open_date DATE NULL,
 account_close_date DATE NULL,
 trading_authorization NVARCHAR(20) NULL,
 authorized_trader_fdid NVARCHAR(40) NULL,
 relationship_start_date DATE NULL,
 relationship_end_date DATE NULL,
 large_trader_flag BIT NULL,
 large_trader_id NVARCHAR(8) NULL,
 effective_start_date DATE NOT NULL,
 dv2_source_hk NVARCHAR(64) NOT NULL,
 source_system NVARCHAR(40) NULL,
 load_date DATETIME2(7) NOT NULL,
 record_source NVARCHAR(200) NOT NULL
)
WITH (
 DISTRIBUTION = HASH(fdid),
 CLUSTERED COLUMNSTORE INDEX
);

------------------------------------------------------------------------------
-- OPERATIONAL
------------------------------------------------------------------------------

CREATE TABLE cat_submission_batch (
 batch_id NVARCHAR(36) NOT NULL,
 event_date DATE NOT NULL,
 cat_reporter_imid NVARCHAR(8) NOT NULL,
 submission_file_type NVARCHAR(12) NOT NULL,
 submission_cycle NVARCHAR(12) NOT NULL,
 correction_cycle_number INT NULL,
 file_name NVARCHAR(200) NOT NULL,
 file_size_bytes BIGINT NULL,
 compressed_size_bytes BIGINT NULL,
 compression_algorithm NVARCHAR(10) NULL,
 record_count BIGINT NOT NULL,
 min_event_timestamp DATETIME2(7) NULL,
 max_event_timestamp DATETIME2(7) NULL,
 sha256_checksum NVARCHAR(64) NOT NULL,
 generation_started_at DATETIME2(7) NULL,
 generation_completed_at DATETIME2(7) NULL,
 generation_duration_seconds INT NULL,
 submission_timestamp DATETIME2(7) NULL,
 submission_method NVARCHAR(10) NULL,
 target_endpoint NVARCHAR(200) NULL,
 submission_attempt_number INT NULL,
 ack_timestamp DATETIME2(7) NULL,
 ack_status NVARCHAR(15) NOT NULL,
 ack_reference_id NVARCHAR(64) NULL,
 rejection_reason NVARCHAR(400) NULL,
 submission_deadline DATETIME2(7) NOT NULL,
 is_late_submission BIT NULL,
 records_accepted BIGINT NULL,
 records_rejected BIGINT NULL,
 records_repaired BIGINT NULL,
 cat_submission_version NVARCHAR(10) NULL,
 submitted_by NVARCHAR(100) NULL,
 dv2_source_hk NVARCHAR(MAX) NULL,
 load_date DATETIME2(7) NOT NULL,
 record_source NVARCHAR(200) NOT NULL
)
WITH (
 DISTRIBUTION = HASH(batch_id),
 CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE cat_feedback_error (
 error_id NVARCHAR(36) NOT NULL,
 feedback_date DATE NOT NULL,
 batch_id NVARCHAR(36) NOT NULL,
 event_date DATE NOT NULL,
 firm_roe_id NVARCHAR(40) NOT NULL,
 cat_order_id NVARCHAR(40) NULL,
 cat_event_code NVARCHAR(10) NULL,
 error_code NVARCHAR(20) NOT NULL,
 error_severity NVARCHAR(20) NOT NULL,
 error_category NVARCHAR(20) NULL,
 error_description NVARCHAR(1000) NOT NULL,
 field_name NVARCHAR(80) NULL,
 field_value NVARCHAR(400) NULL,
 expected_value NVARCHAR(400) NULL,
 detected_timestamp DATETIME2(7) NOT NULL,
 t_plus_3_deadline DATE NOT NULL,
 days_to_deadline INT NULL,
 resolution_status NVARCHAR(15) NOT NULL,
 resolution_action NVARCHAR(20) NULL,
 resolution_batch_id NVARCHAR(36) NULL,
 resolution_timestamp DATETIME2(7) NULL,
 resolved_by NVARCHAR(100) NULL,
 resolution_notes NVARCHAR(2000) NULL,
 escalation_level INT NULL,
 escalation_timestamp DATETIME2(7) NULL,
 regulatory_impact NVARCHAR(30) NULL,
 dv2_source_hk NVARCHAR(64) NULL,
 load_date DATETIME2(7) NOT NULL,
 record_source NVARCHAR(200) NOT NULL
)
WITH (
 DISTRIBUTION = HASH(batch_id),
 CLUSTERED COLUMNSTORE INDEX
);

------------------------------------------------------------------------------
-- Distribution summary
------------------------------------------------------------------------------
-- HASH: dim_date, dim_party, dim_instrument, dim_account, dim_trader,
-- fact_cat_order_events (cat_order_id),
-- fact_cat_allocations (cat_order_id),
-- fact_cat_quotes (cat_quote_id),
-- fact_cat_customer_records (fdid),
-- cat_submission_batch (batch_id), cat_feedback_error (batch_id)
-- ROUND_ROBIN: dim_venue, dim_event_type, dim_desk (low cardinality)
------------------------------------------------------------------------------
