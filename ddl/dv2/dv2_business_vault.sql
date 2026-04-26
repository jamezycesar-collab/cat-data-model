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
-- File: dv2_business_vault.sql
-- Purpose: Data Vault 2.0 - Business Vault (derived / calculated tables)
-- Pattern: Business Vault = the "thinking" layer. Contains:
-- * Computed Satellites (sat_bv_*): derived metrics on existing hubs
-- * Derived Links (link_bv_*): links implied by business logic (e.g., mapping
-- MEOT to the opening MENO even without an explicit pointer)
-- * Aggregated Bridges (bridge_bv_*): daily/hourly roll-ups
-- All BV tables are re-buildable from Raw Vault - never an authoritative source.
-- ============================================================================

-- ============================================================================
-- COMPUTED SATELLITES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- BV 1: sat_bv_party_risk - computed KYC risk score
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_bv_party_risk (
 party_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 load_end_date TIMESTAMP,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL COMMENT 'Always ''BUSINESS_VAULT''',
 kyc_score DECIMAL(10,4) COMMENT 'Composite 0-100',
 aml_score DECIMAL(10,4),
 concentration_score DECIMAL(10,4) COMMENT 'Exposure concentration',
 risk_rating_derived STRING COMMENT 'LOW | MEDIUM | HIGH | CRITICAL',
 last_review_date DATE,
 model_version STRING COMMENT 'Risk model version pinned per SR 11-7'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Derived party risk - inputs sat_party_classification + activity bridges'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_business');

-- ----------------------------------------------------------------------------
-- BV 2: sat_bv_order_best_execution - derived best-ex metrics
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_bv_order_best_execution (
 order_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 arrival_price DECIMAL(20,8) COMMENT 'NBBO midpoint at order arrival',
 vwap_benchmark DECIMAL(20,8) COMMENT 'Interval VWAP during order life',
 twap_benchmark DECIMAL(20,8),
 implementation_shortfall_bps DECIMAL(10,4) COMMENT 'IS vs arrival price',
 price_improvement_bps DECIMAL(10,4),
 effective_spread_bps DECIMAL(10,4),
 realized_spread_bps DECIMAL(10,4),
 adverse_selection_bps DECIMAL(10,4),
 venue_toxicity_score DECIMAL(10,4),
 nbbo_match_flag BOOLEAN,
 rule_606_category STRING COMMENT 'NMS category for Rule 606 report'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Order-level best-ex metrics - feeds Rule 605/606 and client TCA'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_business');

-- ----------------------------------------------------------------------------
-- BV 3: sat_bv_execution_latency - derived latency buckets
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_bv_execution_latency (
 execution_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 order_to_route_ms BIGINT COMMENT 'Order arrival -> route sent',
 route_to_ack_ms BIGINT COMMENT 'Route sent -> venue ack',
 ack_to_exec_ms BIGINT COMMENT 'Venue ack -> fill',
 total_order_life_ms BIGINT,
 clock_sync_breach BOOLEAN COMMENT 'CAT clock sync (>50ms)',
 latency_bucket STRING COMMENT 'SUB_MS | LOW | MED | HIGH | OUTLIER'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Execution latency - feeds SLA and CAT clock-sync monitoring'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_business');

-- ----------------------------------------------------------------------------
-- BV 4: sat_bv_confirmation_sla - derived SEC 15c6-2 compliance
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_bv_confirmation_sla (
 confirmation_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 same_day_affirm_flag BOOLEAN COMMENT 'SEC Rule 15c6-2 target',
 time_to_affirm_minutes BIGINT,
 affirm_latency_bucket STRING,
 dk_risk_flag BOOLEAN
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'SEC Rule 15c6-2 same-day affirmation compliance'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_business');

-- ----------------------------------------------------------------------------
-- BV 5: sat_bv_settlement_fail_risk - CSDR penalty predictor
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sat_bv_settlement_fail_risk (
 settlement_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL,
 fail_probability DECIMAL(10,6),
 expected_csdr_penalty DECIMAL(20,4),
 days_aged_at_fail INT,
 risk_bucket STRING,
 model_version STRING
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Settlement fail risk - CSDR penalty prediction'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_business');

-- ============================================================================
-- DERIVED LINKS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- BV 6: link_bv_order_to_execution_derived - when explicit link missing
-- Strategy: match by ClOrdID+TradeDate+Account; populated only if raw link NULL
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_bv_order_to_execution_derived (
 link_hk STRING NOT NULL,
 order_hk STRING NOT NULL,
 execution_hk STRING NOT NULL,
 derivation_rule STRING COMMENT 'CLORDID_TRADEDATE_ACCOUNT | TIMESTAMP_NEAREST | MANUAL',
 confidence_score DECIMAL(4,2) COMMENT '0.00 to 1.00',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL COMMENT 'BUSINESS_VAULT'
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Derived link when raw data lacks explicit order->execution join'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_business');

-- ----------------------------------------------------------------------------
-- BV 7: link_bv_beneficial_owner_chain - traversal of ownership hierarchy
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_bv_beneficial_owner_chain (
 link_hk STRING NOT NULL,
 party_hk STRING NOT NULL COMMENT 'Starting entity',
 ultimate_beneficial_owner_hk STRING NOT NULL COMMENT 'UBO after traversal',
 chain_depth INT COMMENT 'Number of hops',
 effective_ownership_pct DECIMAL(10,6) COMMENT 'Cumulative product',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'UBO traversal - FinCEN CTA compliance'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_business');

-- ============================================================================
-- AGGREGATED BRIDGES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- BV 8: bridge_bv_daily_trade_lifecycle_facts
-- Core fact for Gold star schema fct_trade_lifecycle.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_bv_daily_trade_lifecycle_facts (
 fact_date DATE NOT NULL,
 party_hk STRING NOT NULL,
 instrument_hk STRING NOT NULL,
 venue_hk STRING,
 account_hk STRING,
 order_count BIGINT,
 executed_order_count BIGINT,
 cancelled_order_count BIGINT,
 rejected_order_count BIGINT,
 execution_count BIGINT,
 buy_notional_usd DECIMAL(20,4),
 sell_notional_usd DECIMAL(20,4),
 total_notional_usd DECIMAL(20,4),
 avg_fill_price DECIMAL(20,8),
 vwap DECIMAL(20,8),
 price_improvement_total_bps DECIMAL(20,4),
 settlement_fail_count BIGINT,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (fact_date)
COMMENT 'Daily trade-lifecycle facts - Gold layer input'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_business');

-- ----------------------------------------------------------------------------
-- BV 9: bridge_bv_hourly_venue_quality - venue quality scoring
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_bv_hourly_venue_quality (
 hour_ts TIMESTAMP NOT NULL,
 venue_hk STRING NOT NULL,
 instrument_hk STRING NOT NULL,
 executed_qty DECIMAL(20,4),
 price_improvement_bps_avg DECIMAL(10,4),
 effective_spread_bps_avg DECIMAL(10,4),
 fill_rate DECIMAL(10,6),
 avg_latency_ms DECIMAL(12,4),
 venue_quality_score DECIMAL(10,4) COMMENT 'Composite 0-100',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (hour_ts)
COMMENT 'Hourly venue quality - drives SOR routing decisions'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_business');

-- ----------------------------------------------------------------------------
-- BV 10: bridge_bv_cat_event_coverage - CAT audit coverage check
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_bv_cat_event_coverage (
 cat_reportable_date DATE NOT NULL,
 reporting_party_hk STRING NOT NULL,
 cat_event_type STRING NOT NULL,
 event_count BIGINT,
 reported_count BIGINT,
 late_report_count BIGINT,
 rejected_count BIGINT,
 corrected_count BIGINT,
 coverage_pct DECIMAL(10,6),
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (cat_reportable_date)
COMMENT 'FINRA CAT coverage & reporting quality metrics - feeds regulator dashboards'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_business');

-- ----------------------------------------------------------------------------
-- BV 11: bridge_bv_counterparty_exposure - real-time counterparty risk
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_bv_counterparty_exposure (
 exposure_date DATE NOT NULL,
 party_hk STRING NOT NULL,
 counterparty_hk STRING NOT NULL,
 agreement_hk STRING COMMENT 'Governing ISDA/CSA/GMRA',
 gross_exposure_usd DECIMAL(20,4),
 net_exposure_usd DECIMAL(20,4) COMMENT 'After netting',
 collateral_posted_usd DECIMAL(20,4),
 collateral_received_usd DECIMAL(20,4),
 unsecured_exposure_usd DECIMAL(20,4),
 ee_5pct DECIMAL(20,4) COMMENT 'Expected exposure 5th pctile',
 ee_95pct DECIMAL(20,4),
 pfe DECIMAL(20,4) COMMENT 'Potential Future Exposure',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (exposure_date)
COMMENT 'Counterparty exposure - Basel III / SA-CCR input'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_business');

-- ============================================================================
-- END OF BUSINESS VAULT - 11 tables
-- 5 Computed Satellites
-- 2 Derived Links
-- 4 Aggregated Bridges
-- All tables re-buildable from Raw Vault. record_source = 'BUSINESS_VAULT'.
-- ============================================================================
