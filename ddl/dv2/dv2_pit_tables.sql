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
-- File: dv2_pit_tables.sql
-- Purpose: Data Vault 2.0 Silver - Point-In-Time (PIT) Tables
-- Pattern: For each Hub, a PIT table snapshots the "as-of" load_date for every
-- Satellite attached to it. Consumers (Gold, DLT, analytics) can join
-- via PIT instead of doing the SCD2 "find the most recent row"
-- range-join every query (massively improves performance).
-- Shape: {hub}_hk + snapshot_date + for each sat: (sat_load_date) picked with
-- load_date <= snapshot_date AND (load_end_date > snapshot_date
-- OR load_end_date IS NULL)
-- Strategy: Rebuilt daily via DLT; partitioned by snapshot_date for pruning.
-- Retention: 7 days rolling (older snapshots aged out) - append-only pit_
-- consumes disk; use OPTIMIZE + VACUUM 168 HOURS.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- PIT 1: pit_party - snapshot for Party + all its Satellites
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_party (
 party_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_party_details_load_date TIMESTAMP,
 sat_party_classification_load_date TIMESTAMP,
 sat_party_contact_load_date TIMESTAMP,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'PIT for hub_party - daily snapshot index into party satellites'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='7');

-- ----------------------------------------------------------------------------
-- PIT 2: pit_account
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_account (
 account_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_account_details_load_date TIMESTAMP,
 sat_account_status_load_date TIMESTAMP,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'PIT for hub_account'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='7');

-- ----------------------------------------------------------------------------
-- PIT 3: pit_instrument
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_instrument (
 instrument_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_instrument_details_load_date TIMESTAMP,
 sat_instrument_classification_load_date TIMESTAMP,
 sat_instrument_status_load_date TIMESTAMP,
 sat_instrument_derivative_terms_load_date TIMESTAMP,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'PIT for hub_instrument - T+1 listing status changes flow through'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='7');

-- ----------------------------------------------------------------------------
-- PIT 4: pit_venue
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_venue (
 venue_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_venue_details_load_date TIMESTAMP,
 sat_venue_connectivity_load_date TIMESTAMP,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'PIT for hub_venue'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='7');

-- ----------------------------------------------------------------------------
-- PIT 5: pit_agreement
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_agreement (
 agreement_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_agreement_details_load_date TIMESTAMP,
 sat_agreement_terms_load_date TIMESTAMP,
 sat_agreement_status_load_date TIMESTAMP,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'PIT for hub_agreement'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='7');

-- ----------------------------------------------------------------------------
-- PIT 6: pit_order - critical perf path (millions of orders per day)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_order (
 order_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_order_details_load_date TIMESTAMP,
 sat_order_routing_load_date TIMESTAMP,
 sat_order_status_load_date TIMESTAMP,
 sat_order_modification_load_date TIMESTAMP,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date, record_source)
COMMENT 'PIT for hub_order - rebuilt intraday for surveillance latency'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='30');

-- ----------------------------------------------------------------------------
-- PIT 7: pit_execution - partitioned by trade_date for clearing reconciliation
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_execution (
 execution_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_execution_details_load_date TIMESTAMP,
 sat_execution_classification_load_date TIMESTAMP,
 sat_execution_reversal_load_date TIMESTAMP,
 trade_date DATE,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date, trade_date)
COMMENT 'PIT for hub_execution'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='30');

-- ----------------------------------------------------------------------------
-- PIT 8: pit_allocation
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_allocation (
 allocation_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_allocation_details_load_date TIMESTAMP,
 sat_allocation_status_load_date TIMESTAMP,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'PIT for hub_allocation'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='30');

-- ----------------------------------------------------------------------------
-- PIT 9: pit_confirmation
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_confirmation (
 confirmation_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_confirmation_details_load_date TIMESTAMP,
 sat_confirmation_status_load_date TIMESTAMP,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'PIT for hub_confirmation - SEC Rule 15c6-2 affirm-rate feed'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='30');

-- ----------------------------------------------------------------------------
-- PIT 10: pit_clearing
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_clearing (
 clearing_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_clearing_details_load_date TIMESTAMP,
 sat_clearing_status_load_date TIMESTAMP,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'PIT for hub_clearing'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='30');

-- ----------------------------------------------------------------------------
-- PIT 11: pit_settlement - partitioned by contractual_settle_date for CSDR
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_settlement (
 settlement_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_settlement_details_load_date TIMESTAMP,
 sat_settlement_status_load_date TIMESTAMP,
 contractual_settle_date DATE,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date, contractual_settle_date)
COMMENT 'PIT for hub_settlement - feeds T+1 fails dashboard'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='30');

-- ----------------------------------------------------------------------------
-- PIT 12: pit_submission
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_submission (
 submission_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_submission_details_load_date TIMESTAMP,
 sat_submission_status_load_date TIMESTAMP,
 report_date DATE,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'PIT for hub_submission - regulator audit evidence'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='365');

-- ----------------------------------------------------------------------------
-- PIT 13: pit_quote - used by BestEx analytics
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_quote (
 quote_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_quote_details_load_date TIMESTAMP,
 sat_quote_status_load_date TIMESTAMP,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'PIT for hub_quote - feeds NBBO reconstruction'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='7');

-- ----------------------------------------------------------------------------
-- PIT 14: pit_order_request
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_order_request (
 order_request_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_order_request_details_load_date TIMESTAMP,
 sat_order_request_status_load_date TIMESTAMP,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'PIT for hub_order_request'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='30');

-- ----------------------------------------------------------------------------
-- PIT 15: pit_repo
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_repo (
 repo_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_repo_details_load_date TIMESTAMP,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'PIT for hub_repo'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='30');

-- ----------------------------------------------------------------------------
-- PIT 16: pit_securities_loan
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pit_securities_loan (
 loan_hk STRING NOT NULL,
 snapshot_date DATE NOT NULL,
 sat_securities_loan_details_load_date TIMESTAMP,
 record_source STRING NOT NULL,
 _pit_load_ts TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
COMMENT 'PIT for hub_securities_loan'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_pit', 'pit_retention_days'='30');

-- ============================================================================
-- END OF PIT TABLES - 16 PIT tables
-- Coverage: all 20 hubs -> (missing deliberately): hub_desk (rarely joined in
-- perf paths), hub_rfe, hub_ioi (low volume, on-demand query acceptable)
-- Build pattern: DLT @dlt.table with incremental MERGE on (hub_hk, snapshot_date)
-- Retention: 7-30 days operational, 365 days for pit_submission (audit)
-- ============================================================================
