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
-- File: dv2_links.sql
-- Purpose: Data Vault 2.0 Silver - Link Tables (32 links)
-- Pattern: link_hk = SHA-256(hub1_hk || hub2_hk || [hub3_hk])
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Link 1 of 32: link_order_request_party_instrument (3-way)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_order_request_party_instrument (
 link_hk STRING NOT NULL COMMENT 'SHA-256(order_request_hk || party_hk || instrument_hk)',
 order_request_hk STRING NOT NULL COMMENT 'FK to hub_order_request',
 party_hk STRING NOT NULL COMMENT 'FK to hub_party (customer)',
 instrument_hk STRING NOT NULL COMMENT 'FK to hub_instrument',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Customer X requested trade of instrument Y via MEIR/MOIR'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 2 of 32: link_order_request_order
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_order_request_order (
 link_hk STRING NOT NULL,
 order_request_hk STRING NOT NULL COMMENT 'FK to hub_order_request',
 order_hk STRING NOT NULL COMMENT 'FK to hub_order',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Order request resulted in this order (MEIR->MENO chain)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 3 of 32: link_quote_request_party_instrument (3-way)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_quote_request_party_instrument (
 link_hk STRING NOT NULL,
 quote_request_hk STRING NOT NULL COMMENT 'FK to hub_quote_request',
 party_hk STRING NOT NULL COMMENT 'FK to hub_party (client)',
 instrument_hk STRING NOT NULL COMMENT 'FK to hub_instrument',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Client X requested quote on instrument Y (MEQR)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 4 of 32: link_quote_response (3-way: RFQ+Quote+Dealer)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_quote_response (
 link_hk STRING NOT NULL,
 quote_request_hk STRING NOT NULL COMMENT 'FK to hub_quote_request',
 quote_hk STRING NOT NULL COMMENT 'FK to hub_quote',
 party_hk STRING NOT NULL COMMENT 'FK to hub_party (responding dealer)',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Quote sent in response to RFQ (MEQS)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 5 of 32: link_quote_instrument
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_quote_instrument (
 link_hk STRING NOT NULL,
 quote_hk STRING NOT NULL,
 instrument_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Quote is for this instrument'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 6 of 32: link_quote_order
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_quote_order (
 link_hk STRING NOT NULL,
 quote_hk STRING NOT NULL,
 order_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Order hit/lifted this quote (MEQS->MENO)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 7 of 32: link_quote_execution
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_quote_execution (
 link_hk STRING NOT NULL,
 quote_hk STRING NOT NULL,
 execution_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Execution linked to source quote (CAT MEOTQ quote linkage)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 8 of 32: link_rfe_party_instrument (3-way)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_rfe_party_instrument (
 link_hk STRING NOT NULL,
 rfe_hk STRING NOT NULL,
 party_hk STRING NOT NULL,
 instrument_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Client X requested execution of instrument Y (RFE)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 9 of 32: link_order_rfe
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_order_rfe (
 link_hk STRING NOT NULL,
 order_hk STRING NOT NULL,
 rfe_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Order spawned from this RFE'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 10 of 32: link_order_party
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_order_party (
 link_hk STRING NOT NULL,
 order_hk STRING NOT NULL,
 party_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Party placed/received this order'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 11 of 32: link_order_instrument
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_order_instrument (
 link_hk STRING NOT NULL,
 order_hk STRING NOT NULL,
 instrument_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Order is for this instrument'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 12 of 32: link_order_venue
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_order_venue (
 link_hk STRING NOT NULL,
 order_hk STRING NOT NULL,
 venue_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Order sent to this venue'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 13 of 32: link_execution_order
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_execution_order (
 link_hk STRING NOT NULL,
 execution_hk STRING NOT NULL,
 order_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'This fill belongs to this order'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 14 of 32: link_execution_venue
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_execution_venue (
 link_hk STRING NOT NULL,
 execution_hk STRING NOT NULL,
 venue_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Executed at this venue'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 15 of 32: link_execution_counterparty
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_execution_counterparty (
 link_hk STRING NOT NULL,
 execution_hk STRING NOT NULL,
 party_hk STRING NOT NULL COMMENT 'Contra party on the fill',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Contra party on this fill'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 16 of 32: link_allocation_execution
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_allocation_execution (
 link_hk STRING NOT NULL,
 allocation_hk STRING NOT NULL,
 execution_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Fill allocated to accounts (MEPA)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 17 of 32: link_allocation_account
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_allocation_account (
 link_hk STRING NOT NULL,
 allocation_hk STRING NOT NULL,
 account_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Allocated to this account'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 18 of 32: link_confirmation_allocation
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_confirmation_allocation (
 link_hk STRING NOT NULL,
 confirmation_hk STRING NOT NULL,
 allocation_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Confirmation for this allocation'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 19 of 32: link_clearing_execution
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_clearing_execution (
 link_hk STRING NOT NULL,
 clearing_hk STRING NOT NULL,
 execution_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'This trade cleared by CCP'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 20 of 32: link_settlement_allocation
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_settlement_allocation (
 link_hk STRING NOT NULL,
 settlement_hk STRING NOT NULL,
 allocation_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Settlement instruction for this allocation'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 21 of 32: link_party_hierarchy (self-referencing)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_party_hierarchy (
 link_hk STRING NOT NULL,
 parent_party_hk STRING NOT NULL COMMENT 'FK to hub_party (parent)',
 child_party_hk STRING NOT NULL COMMENT 'FK to hub_party (child)',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Parent/child ownership between parties'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 22 of 32: link_party_account
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_party_account (
 link_hk STRING NOT NULL,
 party_hk STRING NOT NULL,
 account_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Party owns/manages account'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 23 of 32: link_party_agreement (3-way: party+counterparty+agreement)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_party_agreement (
 link_hk STRING NOT NULL,
 party_a_hk STRING NOT NULL COMMENT 'FK to hub_party (side A)',
 party_b_hk STRING NOT NULL COMMENT 'FK to hub_party (side B / counterparty)',
 agreement_hk STRING NOT NULL COMMENT 'FK to hub_agreement',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Agreement between two parties'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 24 of 32: link_instrument_underlying (self-referencing)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_instrument_underlying (
 link_hk STRING NOT NULL,
 derivative_hk STRING NOT NULL COMMENT 'FK to hub_instrument (derivative)',
 underlying_hk STRING NOT NULL COMMENT 'FK to hub_instrument (underlying)',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Derivative references underlying instrument'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 25 of 32: link_instrument_venue
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_instrument_venue (
 link_hk STRING NOT NULL,
 instrument_hk STRING NOT NULL,
 venue_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Instrument listed at venue (primary or cross-listing)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 26 of 32: link_netting_set
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_netting_set (
 link_hk STRING NOT NULL,
 agreement_hk STRING NOT NULL,
 clearing_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Netting set under agreement - SA-CCR capital calc anchor'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 27 of 32: link_repo_agreement
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_repo_agreement (
 link_hk STRING NOT NULL,
 repo_hk STRING NOT NULL,
 agreement_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Repo under GMRA agreement'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 28 of 32: link_repo_instrument
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_repo_instrument (
 link_hk STRING NOT NULL,
 repo_hk STRING NOT NULL,
 instrument_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Purchased / collateral securities in repo'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 29 of 32: link_repo_parties (3-way)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_repo_parties (
 link_hk STRING NOT NULL,
 repo_hk STRING NOT NULL,
 seller_party_hk STRING NOT NULL COMMENT 'FK to hub_party (repo seller / cash borrower)',
 buyer_party_hk STRING NOT NULL COMMENT 'FK to hub_party (repo buyer / cash lender)',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Repo counterparties (seller/buyer under GMRA)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 30 of 32: link_loan_agreement
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_loan_agreement (
 link_hk STRING NOT NULL,
 securities_loan_hk STRING NOT NULL,
 agreement_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Securities loan under GMSLA'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 31 of 32: link_loan_instrument
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_loan_instrument (
 link_hk STRING NOT NULL,
 securities_loan_hk STRING NOT NULL,
 instrument_hk STRING NOT NULL,
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Securities on loan'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Link 32 of 32: link_loan_parties (3-way)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS link_loan_parties (
 link_hk STRING NOT NULL,
 securities_loan_hk STRING NOT NULL,
 lender_party_hk STRING NOT NULL COMMENT 'FK to hub_party (lender)',
 borrower_party_hk STRING NOT NULL COMMENT 'FK to hub_party (borrower)',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
COMMENT 'Securities-lending counterparties (lender/borrower under GMSLA)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ============================================================================
-- END OF FILE - 32 CREATE TABLE statements
-- ============================================================================
