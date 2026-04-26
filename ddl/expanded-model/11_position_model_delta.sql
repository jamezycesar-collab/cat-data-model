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
-- File: 11_position_model_delta.sql
-- Purpose: Position Stage Model (2 entities, Delta Lake)
-- Scope: Point-in-time position snapshots and reconciliation breaks
-- Note: position is Gold-layer (computed from settlements, never directly
-- loaded); position_break is Silver-layer reconciliation output.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Entity 1 of 2: position (NEW)
-- Point-in-time position snapshot - Gold-layer computed
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS position (
 position_id STRING NOT NULL COMMENT 'UUID v4 - Primary key (synthetic; or composite of account_id × instrument_id × position_date)',
 account_id STRING NOT NULL COMMENT 'FK to account.account_id',
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id',
 position_date DATE NOT NULL COMMENT 'As-of date for this position snapshot',
 quantity_long DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT 'Long quantity (positive)',
 quantity_short DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT 'Short quantity (positive magnitude of short)',
 net_quantity DECIMAL(18,4) NOT NULL COMMENT 'Net position: quantity_long - quantity_short',
 average_cost DECIMAL(18,8) COMMENT 'Average cost basis per unit (FIFO/LIFO/WAC per tax-lot accounting)',
 market_price DECIMAL(18,8) COMMENT 'Mark-to-market price on position_date',
 market_value DECIMAL(18,2) COMMENT 'Mark-to-market value: net_quantity × market_price',
 unrealized_pnl DECIMAL(18,2) COMMENT 'Unrealized P&L: (market_price - average_cost) × net_quantity',
 realized_pnl_daily DECIMAL(18,2) COMMENT 'Realized P&L for the day',
 realized_pnl_ytd DECIMAL(18,2) COMMENT 'Realized P&L year-to-date',
 accrued_interest DECIMAL(18,4) COMMENT 'Accrued interest (fixed income positions)',
 currency STRING COMMENT 'ISO 4217 position currency',
 position_source STRING NOT NULL COMMENT 'COMPUTED (derived from settlements), RECONCILED (matched to custodian), STATED (reported without verification)',
 custodian_party_id STRING COMMENT 'FK to party.party_id - custodian holding this position',
 reconciliation_status STRING COMMENT 'MATCHED, BROKEN, UNRECONCILED, IN_PROGRESS',
 last_reconciled_timestamp TIMESTAMP COMMENT 'When this position was last reconciled against custodian',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier (typically GOLD_PIPELINE)',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Point-in-time position snapshots - Gold-layer output computed from settlement records; never directly loaded; supports long/short quantity split, M2M, PnL decomposition, and custodian reconciliation status'
PARTITIONED BY (position_date)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Position snapshots - Gold-layer',
 'compression.codec' = 'zstd',
 'subject_area' = 'position',
 'source_lineage' = ' section Position Stage - position'
);

-- Z-ORDER BY (account_id, instrument_id)

-- ----------------------------------------------------------------------------
-- Entity 2 of 2: position_break (NEW)
-- Reconciliation breaks - internal vs. custodian/depot
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS position_break (
 break_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 position_id STRING NOT NULL COMMENT 'FK to position.position_id',
 custodian_quantity DECIMAL(18,4) COMMENT 'Quantity as reported by custodian/depot',
 internal_quantity DECIMAL(18,4) COMMENT 'Quantity as computed internally',
 break_quantity DECIMAL(18,4) COMMENT 'Difference: custodian_quantity - internal_quantity',
 custodian_value DECIMAL(18,2) COMMENT 'Market value as reported by custodian',
 internal_value DECIMAL(18,2) COMMENT 'Market value as computed internally',
 break_value DECIMAL(18,2) COMMENT 'Value break: custodian_value - internal_value',
 break_type STRING NOT NULL COMMENT 'QUANTITY, PRICE, SETTLEMENT_DATE, CURRENCY, ACCOUNT, INSTRUMENT_MAPPING, CORPORATE_ACTION',
 break_status STRING NOT NULL COMMENT 'OPEN, INVESTIGATING, RESOLVED, WRITTEN_OFF, AGED',
 break_detected_date DATE NOT NULL COMMENT 'Date the break was detected',
 resolution_date DATE COMMENT 'Date break was resolved',
 resolution_notes STRING COMMENT 'Free-text explanation of resolution',
 resolver_party_role_id STRING COMMENT 'FK to party_role.party_role_id - who resolved the break',
 aging_days INT COMMENT 'Days open (for aging reports; computed)',
 materiality STRING COMMENT 'LOW, MEDIUM, HIGH, CRITICAL - drives escalation',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Position reconciliation breaks - internal vs. custodian/depot; tracks quantity, price, settlement-date, currency, corporate-action and instrument-mapping breaks with aging and materiality'
PARTITIONED BY (break_detected_date)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Position breaks - reconciliation exceptions',
 'compression.codec' = 'zstd',
 'subject_area' = 'position',
 'source_lineage' = ' section Position Stage - position_break'
);

-- Z-ORDER BY (position_id, break_status, materiality)

-- ============================================================================
-- END OF FILE - 2 CREATE TABLE statements
-- ============================================================================
