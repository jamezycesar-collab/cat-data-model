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
-- File: 09_allocation_model_delta.sql
-- Purpose: Enterprise Allocation Stage Model (2 entities, Delta Lake)
-- Scope: Post-execution block allocation to customer accounts
-- CAT Events Covered: MEPA, MEAA, MOFA
-- FIX Messages: 35=J (AllocationInstruction), 35=P (AllocationInstructionAck)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Entity 1 of 2: allocation (NEW)
-- Post-execution allocation of fills to client accounts
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS allocation (
 allocation_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 cat_event_type STRING NOT NULL COMMENT 'CAT event: MEPA (Post-Trade Allocation), MEAA (Allocation Accepted), MOFA (Manual Order Fulfillment Allocation)',
 execution_id STRING NOT NULL COMMENT 'FK to execution.execution_id - the block fill being allocated',
 order_event_id STRING NOT NULL COMMENT 'FK to order_event.order_event_id - the parent order',
 allocated_account_id STRING NOT NULL COMMENT 'FK to account.account_id - customer account receiving the allocation',
 allocated_party_role_id STRING NOT NULL COMMENT 'FK to party_role.party_role_id - party associated with the allocated account',
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id',
 allocated_quantity DECIMAL(18,4) NOT NULL COMMENT 'Quantity allocated to this account',
 allocated_price DECIMAL(18,8) NOT NULL COMMENT 'Allocation price (typically execution_price, sometimes average)',
 allocation_method STRING NOT NULL COMMENT 'PRO_RATA, MANUAL, ROUND_ROBIN, FILL_ORDER, PCT_BASED, QTY_BASED, EQUAL',
 allocation_status STRING NOT NULL COMMENT 'PENDING, CONFIRMED, REJECTED, AMENDED, CANCELLED',
 allocation_timestamp TIMESTAMP NOT NULL COMMENT 'When allocation was booked',
 settlement_date DATE COMMENT 'Contractual settlement date for this allocation (T+1 / T+2)',
 net_amount DECIMAL(18,4) COMMENT 'Net amount for this allocation (price × qty ± commission ± fees)',
 accrued_interest DECIMAL(18,4) COMMENT 'Accrued interest (for fixed income allocations)',
 commission_allocated DECIMAL(18,4) COMMENT 'Commission allocated to this account',
 regulatory_fee_allocated DECIMAL(18,4) COMMENT 'Regulatory fees allocated to this account',
 tid_type STRING COMMENT 'CAT TIDType field (Phase 2d): Trader / Investment Manager identifier',
 correspondent_crd STRING COMMENT 'CRD number of correspondent firm (for correspondent allocations)',
 new_order_fdid STRING COMMENT 'CAT newOrderFDID - FDID of the allocated account for CAIS linkage',
 allocation_instruction_id STRING COMMENT 'FK to allocation_instruction.instruction_id - which standing instruction drove this allocation (NULL for ad-hoc)',
 manual_flag BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE for MOFA manual allocations',
 affiliate_flag BOOLEAN COMMENT 'CAT affiliateFlag - TRUE if allocated account is an affiliate',
 event_date DATE NOT NULL COMMENT 'Partition key - derived from allocation_timestamp',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Post-execution allocation - CAT MEPA/MEAA/MOFA + FIX 35=J; splits block fill across multiple customer accounts; captures Phase 2d TIDType, correspondent CRD, and new_order_fdid for CAIS linkage'
PARTITIONED BY (event_date)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Allocations - MEPA/MEAA/MOFA + FIX 35=J',
 'compression.codec' = 'zstd',
 'subject_area' = 'allocation',
 'source_lineage' = ' section Allocation Stage - allocation'
);

-- Z-ORDER BY (execution_id, allocated_account_id, allocation_status)

-- ----------------------------------------------------------------------------
-- Entity 2 of 2: allocation_instruction (NEW)
-- Standing or one-time allocation rules - defines how block trades split
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS allocation_instruction (
 instruction_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 account_id STRING NOT NULL COMMENT 'FK to account.account_id - target account',
 party_role_id STRING NOT NULL COMMENT 'FK to party_role.party_role_id - party owning the instruction',
 instrument_id STRING COMMENT 'FK to instrument.instrument_id - scope to specific instrument (NULL = all)',
 asset_class STRING COMMENT 'Scope to specific asset class (NULL = all)',
 allocation_rule STRING NOT NULL COMMENT 'PCT_BASED, QTY_BASED, EQUAL, PRO_RATA',
 target_percentage DECIMAL(8,4) COMMENT 'Target allocation percentage (0-1.0) when allocation_rule = PCT_BASED',
 target_quantity DECIMAL(18,4) COMMENT 'Target allocation quantity when allocation_rule = QTY_BASED',
 priority_rank INT COMMENT 'Priority order when multiple instructions apply (1 = highest)',
 min_allocation DECIMAL(18,4) COMMENT 'Minimum allocation quantity (otherwise skip)',
 max_allocation DECIMAL(18,4) COMMENT 'Maximum allocation quantity (cap)',
 instruction_type STRING COMMENT 'STANDING (reusable), ONE_TIME (single use), SPECIFIC_ORDER (tied to one order)',
 order_event_id STRING COMMENT 'FK to order_event.order_event_id - populated when instruction_type = SPECIFIC_ORDER',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 instruction_status STRING NOT NULL COMMENT 'ACTIVE, SUSPENDED, EXPIRED, CANCELLED',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Allocation instructions - standing or one-time rules that define how block trades are split across accounts; supports percentage-based, quantity-based, and equal allocations with priority ranking'
PARTITIONED BY (allocation_rule)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Allocation instructions - PCT/QTY/EQUAL rules',
 'compression.codec' = 'zstd',
 'subject_area' = 'allocation',
 'source_lineage' = ' section Allocation Stage - allocation_instruction'
);

-- Z-ORDER BY (account_id, party_role_id, instruction_status)

-- ============================================================================
-- END OF FILE - 2 CREATE TABLE statements
-- ============================================================================
