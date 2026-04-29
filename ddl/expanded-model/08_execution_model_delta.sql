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
-- File: 08_execution_model_delta.sql
-- Purpose: Enterprise Execution Stage Model (2 entities, Delta Lake)
-- Scope: Fill lifecycle (partial, full, bust, correct) + multi-leg fills
-- CAT Events Covered: MEOT, MEOTS, MEOF, MOOT (and Section 5.2 multi-leg events MLOR/MLOC/...)
-- FIX Messages: 35=8 (ExecutionReport)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Entity 1 of 2: execution (NEW)
-- Individual fill/execution against an order
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS execution (
 execution_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 cat_event_type STRING NOT NULL COMMENT 'CAT event: MEOT (standard fill), MEOTS (trade linked to quote), MEOTS (trade with symbol - split routes), MEOF (order fulfillment - riskless principal), MOOT (manual trade), Section 5.2 multi-leg events (multi-leg trade)',
 order_event_id STRING NOT NULL COMMENT 'FK to order_event.order_event_id',
 cat_execution_id STRING NOT NULL COMMENT 'CAT-assigned execution identifier',
 quote_event_id STRING COMMENT 'FK to quote_event.quote_event_id - populated for MEOTS (CAT quote linkage)',
 execution_timestamp TIMESTAMP NOT NULL COMMENT 'Execution time - microsecond precision per CAT',
 execution_venue_id STRING COMMENT 'FK to venue.venue_id - venue where fill occurred',
 execution_price DECIMAL(18,8) NOT NULL COMMENT 'Fill price',
 execution_quantity DECIMAL(18,4) NOT NULL COMMENT 'Fill quantity',
 remaining_quantity DECIMAL(18,4) COMMENT 'Remaining open quantity after this fill (LeavesQty - FIX tag 151)',
 execution_type STRING NOT NULL COMMENT 'FIX tag 150 ExecType: NEW, PARTIAL (partial fill), FULL (filled), BUST, CORRECT, CANCEL, REPLACE, REJECT',
 contra_party_role_id STRING COMMENT 'FK to party_role.party_role_id - contra party on the fill',
 contra_side STRING COMMENT 'BUY, SELL (opposite of order.side)',
 capacity STRING NOT NULL COMMENT 'PRINCIPAL, AGENCY, RISKLESS_PRINCIPAL, MIXED (FIX tag 528 OrderCapacity)',
 last_market STRING COMMENT 'ISO 10383 MIC code of execution venue (FIX tag 30 LastMkt)',
 liquidity_indicator STRING COMMENT 'FIX tag 851: ADDED (maker), REMOVED (taker), ROUTED, AUCTION, HALT',
 commission DECIMAL(18,4) COMMENT 'Commission charged',
 commission_type STRING COMMENT 'PER_SHARE, FLAT, BPS, PERCENTAGE',
 regulatory_fee DECIMAL(18,4) COMMENT 'Regulatory fees (SEC Section 31, FINRA TAF, ORF, OCC)',
 manual_flag BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE for MOOT manual fills',
 affiliate_flag BOOLEAN COMMENT 'CAT affiliateFlag',
 cancel_flag BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE if this fill was cancelled (BUST)',
 cancel_timestamp TIMESTAMP COMMENT 'When cancellation was recorded',
 info_barrier_id STRING COMMENT 'CAT information-barrier identifier',
 settlement_date DATE COMMENT 'Contractual settlement date (T+1/T+2/T+3)',
 settlement_currency STRING COMMENT 'ISO 4217 settlement currency',
 event_date DATE NOT NULL COMMENT 'Partition key - derived from execution_timestamp',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Individual fills/executions - CAT MEOT/MEOTS/MEOF/MOOT + FIX ExecutionReport 35=8; separates fill grain from order grain (one order -> many executions); MEOTS populates quote_event_id for CAT quote linkage'
PARTITIONED BY (event_date)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Executions - MEOT/MEOTS/MEOF/MOOT',
 'compression.codec' = 'zstd',
 'subject_area' = 'execution',
 'source_lineage' = ' section Execution Stage - execution'
);

-- Z-ORDER BY (order_event_id, cat_event_type, execution_timestamp)

-- ----------------------------------------------------------------------------
-- Entity 2 of 2: execution_leg (NEW)
-- Multi-leg execution legs - spreads, combos, swaps
-- Associated CAT events: Section 5.2 multi-leg events
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS execution_leg (
 execution_leg_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 cat_event_type STRING NOT NULL COMMENT 'CAT event: Section 5.2 multi-leg events (Multi-Leg Order Trade)',
 execution_id STRING NOT NULL COMMENT 'FK to execution.execution_id',
 leg_number INT NOT NULL COMMENT 'Sequential leg number (1, 2, 3...) matching order_leg.leg_number',
 instrument_leg_id STRING COMMENT 'FK to instrument_leg.leg_id - canonical leg reference',
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id - instrument traded on this leg',
 leg_price DECIMAL(18,8) NOT NULL COMMENT 'Fill price for this leg',
 leg_quantity DECIMAL(18,4) NOT NULL COMMENT 'Fill quantity for this leg',
 leg_side STRING NOT NULL COMMENT 'BUY, SELL - leg-specific side',
 leg_venue_id STRING COMMENT 'FK to venue.venue_id - venue for this leg (may differ per leg on multi-venue legs)',
 leg_capacity STRING COMMENT 'PRINCIPAL, AGENCY, RISKLESS_PRINCIPAL (FIX tag 528 per-leg override)',
 leg_commission DECIMAL(18,4) COMMENT 'Leg-specific commission allocation',
 settlement_currency STRING COMMENT 'ISO 4217 settlement currency for this leg',
 event_date DATE NOT NULL COMMENT 'Partition key',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Multi-leg execution legs - CAT event Section 5.2 multi-leg events; spreads, combos, swaps, butterflies; links to order_leg via leg_number, to instrument_leg via instrument_leg_id'
PARTITIONED BY (event_date)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Multi-leg execution legs - Section 5.2 multi-leg events',
 'compression.codec' = 'zstd',
 'subject_area' = 'execution',
 'source_lineage' = ' section Execution Stage - execution_leg'
);

-- Z-ORDER BY (execution_id, leg_number)

-- ============================================================================
-- END OF FILE - 2 CREATE TABLE statements
-- ============================================================================
