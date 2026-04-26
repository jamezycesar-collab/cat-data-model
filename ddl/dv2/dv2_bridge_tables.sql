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
-- File: dv2_bridge_tables.sql
-- Purpose: Data Vault 2.0 Silver - Bridge Tables
-- Pattern: Bridges flatten multi-hop Link chains into one query-friendly row.
-- Unlike PITs (single-hub timeslice), Bridges cross multiple hubs and
-- resolve M:M junctions to 1:N where consumers expect it.
-- Shape: bridge_hk (SHA-256 of all hub_hks) + all participant hub_hks +
-- effective_start/end + record_source.
-- Use: Feeds the Gold star schema fact tables directly.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Bridge 1: bridge_trade_lifecycle - full path from order_request -> settlement
-- This is THE central bridge for trade-lifecycle analytics.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_trade_lifecycle (
 bridge_hk STRING NOT NULL COMMENT 'SHA-256 of all 10 hub_hk values',
 order_request_hk STRING COMMENT 'Origin (MEIR/MOIR) - nullable when lifecycle starts at MENO',
 quote_hk STRING COMMENT 'If RFQ-sourced (MEQR->MEQS->MENO)',
 order_hk STRING NOT NULL COMMENT 'MENO / core order',
 execution_hk STRING COMMENT 'Terminal fill (MEOT)',
 allocation_hk STRING COMMENT 'Post-trade allocation',
 confirmation_hk STRING COMMENT 'Trade confirmation',
 clearing_hk STRING COMMENT 'CCP novation',
 settlement_hk STRING COMMENT 'DTCC/Euroclear settlement',
 party_hk STRING COMMENT 'Order originator party',
 account_hk STRING COMMENT 'Trading/beneficiary account',
 instrument_hk STRING NOT NULL COMMENT 'Traded instrument',
 venue_hk STRING COMMENT 'Execution venue',
 effective_start TIMESTAMP NOT NULL,
 effective_end TIMESTAMP,
 trade_date DATE NOT NULL,
 lifecycle_status STRING COMMENT 'IN_FLIGHT | SETTLED | FAILED | CANCELLED',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (trade_date, record_source)
COMMENT 'End-to-end lifecycle bridge - feeds bv_trade_lifecycle_facts and FINRA CAT gold'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_bridge');

-- ----------------------------------------------------------------------------
-- Bridge 2: bridge_order_to_execution - order -> N executions
-- Captures order-fill multiplicity without the client having to walk links.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_order_to_execution (
 bridge_hk STRING NOT NULL,
 order_hk STRING NOT NULL,
 execution_hk STRING NOT NULL,
 fill_sequence INT COMMENT 'Ordinal fill on order',
 execution_timestamp TIMESTAMP NOT NULL,
 cumulative_fill_qty DECIMAL(20,4),
 trade_date DATE NOT NULL,
 effective_start TIMESTAMP NOT NULL,
 effective_end TIMESTAMP,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (trade_date)
COMMENT 'Order-to-execution bridge with fill ordinality'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_bridge');

-- ----------------------------------------------------------------------------
-- Bridge 3: bridge_execution_to_allocation - execution -> N allocations
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_execution_to_allocation (
 bridge_hk STRING NOT NULL,
 execution_hk STRING NOT NULL,
 allocation_hk STRING NOT NULL,
 account_hk STRING NOT NULL,
 allocation_pct DECIMAL(10,6),
 trade_date DATE NOT NULL,
 effective_start TIMESTAMP NOT NULL,
 effective_end TIMESTAMP,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (trade_date)
COMMENT 'Block-fill split to account-level allocations'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_bridge');

-- ----------------------------------------------------------------------------
-- Bridge 4: bridge_allocation_to_settlement - allocation -> clearing -> settle
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_allocation_to_settlement (
 bridge_hk STRING NOT NULL,
 allocation_hk STRING NOT NULL,
 clearing_hk STRING,
 settlement_hk STRING,
 trade_date DATE NOT NULL,
 contractual_settle_date DATE,
 actual_settle_date DATE,
 clearing_status STRING,
 settlement_status STRING,
 effective_start TIMESTAMP NOT NULL,
 effective_end TIMESTAMP,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (trade_date, contractual_settle_date)
COMMENT 'Post-trade bridge - feeds T+1 settlement fail dashboards'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_bridge');

-- ----------------------------------------------------------------------------
-- Bridge 5: bridge_party_instrument_activity - party-instrument trading summary
-- Cross-hub bridge: party × instrument × date with activity indicators.
-- Critical for surveillance / best-ex analytics.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_party_instrument_activity (
 bridge_hk STRING NOT NULL,
 party_hk STRING NOT NULL,
 instrument_hk STRING NOT NULL,
 activity_date DATE NOT NULL,
 first_order_timestamp TIMESTAMP,
 last_execution_timestamp TIMESTAMP,
 order_count BIGINT,
 execution_count BIGINT,
 buy_quantity DECIMAL(20,4),
 sell_quantity DECIMAL(20,4),
 notional_usd DECIMAL(20,4),
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (activity_date)
COMMENT 'Party-instrument daily activity bridge - surveillance feed'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_bridge');

-- ----------------------------------------------------------------------------
-- Bridge 6: bridge_quote_rfq_response - RFQ -> Quote(s) -> Response
-- Resolves the 3-way junction quote_response.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_quote_rfq_response (
 bridge_hk STRING NOT NULL,
 quote_request_hk STRING NOT NULL,
 quote_hk STRING NOT NULL,
 responding_party_hk STRING NOT NULL,
 response_timestamp TIMESTAMP NOT NULL,
 response_status STRING COMMENT 'HIT | LIFT | COUNTER | EXPIRED | COVER | DONE_AWAY',
 bid_price DECIMAL(20,8),
 ask_price DECIMAL(20,8),
 response_sequence INT,
 trade_date DATE,
 effective_start TIMESTAMP NOT NULL,
 effective_end TIMESTAMP,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (trade_date)
COMMENT 'RFQ -> Quote responses bridge'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_bridge');

-- ----------------------------------------------------------------------------
-- Bridge 7: bridge_party_agreement_netting - party × agreement × netting set
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_party_agreement_netting (
 bridge_hk STRING NOT NULL,
 party_hk STRING NOT NULL,
 counterparty_hk STRING NOT NULL,
 agreement_hk STRING NOT NULL,
 netting_set_id STRING,
 close_out_netting BOOLEAN,
 settlement_netting BOOLEAN,
 effective_start TIMESTAMP NOT NULL,
 effective_end TIMESTAMP,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Party-agreement-netting bridge - drives counterparty credit risk calc'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_bridge');

-- ----------------------------------------------------------------------------
-- Bridge 8: bridge_order_routing_chain - order -> N routes -> N venues
-- Captures the full routing tree for SEC Rule 606 best-ex reporting.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_order_routing_chain (
 bridge_hk STRING NOT NULL,
 order_hk STRING NOT NULL,
 parent_order_hk STRING COMMENT 'For MECO/MLCO routed chains',
 route_sequence INT COMMENT 'Ordinal in routing chain',
 destination_venue_hk STRING NOT NULL,
 routing_broker_hk STRING,
 route_type STRING,
 algo_strategy STRING,
 route_timestamp TIMESTAMP NOT NULL,
 trade_date DATE NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (trade_date)
COMMENT 'Order routing chain - feeds Rule 606 quarterly report'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_bridge');

-- ============================================================================
-- END OF BRIDGES - 8 bridge tables
-- Primary consumer: Gold star schema (dv2_business_vault.sql +
-- Rebuild pattern: DLT append-only append on new trade_date partitions.
-- ============================================================================
