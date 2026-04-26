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
-- File: dv2_hubs.sql
-- Purpose: Data Vault 2.0 Silver - Hub Tables (20 hubs)
-- Pattern: {entity}_hk (SHA-256), {entity}_bk (business key), load_date, record_source
-- Hash fn: SHA2(CONCAT_WS('||', business_key, record_source), 256)
-- Retention: 6 years (BCBS 239 / SEC 17a-4)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Hub 1 of 20: hub_party
-- Business key: LEI (ISO 17442) or internal party_id
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_party (
 party_hk STRING NOT NULL COMMENT 'SHA-256(party_bk || record_source) - Hub hash key',
 party_bk STRING NOT NULL COMMENT 'Business key: LEI if available else internal party_id',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp (UTC)',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier (KYC, CRM, etc.)'
)
USING DELTA
COMMENT 'Hub for Party - unique business-key registry for every legal entity, natural person, or institutional counterparty'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 2 of 20: hub_account
-- Business key: Account number
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_account (
 account_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 account_bk STRING NOT NULL COMMENT 'Account number (natural key)',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (OMS / Account Master)'
)
USING DELTA
COMMENT 'Hub for Account - unique customer/proprietary/omnibus accounts'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 3 of 20: hub_desk
-- Business key: Desk code
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_desk (
 desk_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 desk_bk STRING NOT NULL COMMENT 'Desk code (e.g. US_EQUITIES_CASH, EMEA_RATES)',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (Organization Master)'
)
USING DELTA
COMMENT 'Hub for Desk - trading desk / business unit registry'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 4 of 20: hub_agreement
-- Business key: Agreement reference number
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_agreement (
 agreement_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 agreement_bk STRING NOT NULL COMMENT 'Agreement reference number (ISDA/CSA/GMRA/GMSLA/PB)',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (Legal / Docs)'
)
USING DELTA
COMMENT 'Hub for Agreement - master agreements across ISDA, GMRA, GMSLA, Prime Brokerage'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 5 of 20: hub_instrument
-- Business key: ISIN / CUSIP / FIGI (preferred order)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_instrument (
 instrument_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 instrument_bk STRING NOT NULL COMMENT 'Business key: ISIN > CUSIP > FIGI > internal',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (Securities Master)'
)
USING DELTA
COMMENT 'Hub for Instrument - securities master universe'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 6 of 20: hub_venue
-- Business key: MIC code (ISO 10383)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_venue (
 venue_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 venue_bk STRING NOT NULL COMMENT 'ISO 10383 MIC code',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (Market Data / Venue Master)'
)
USING DELTA
COMMENT 'Hub for Venue - trading venues (exchanges, ATS, dark pools, SIs, OTC, CCPs, CSDs)'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 7 of 20: hub_order_request
-- Business key: firm_roe_id (Firm Reportable Order Event ID)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_order_request (
 order_request_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 order_request_bk STRING NOT NULL COMMENT 'firm_roe_id - unique within firm',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (OMS / FIX Gateway)'
)
USING DELTA
COMMENT 'Hub for Order Request - MEIR/MOIR first-touchpoint events'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 8 of 20: hub_quote_request
-- Business key: quote_req_id (FIX tag 131)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_quote_request (
 quote_request_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 quote_request_bk STRING NOT NULL COMMENT 'FIX tag 131 QuoteReqID',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (RFQ Platform / FIX Gateway)'
)
USING DELTA
COMMENT 'Hub for Quote Request - MEQR / FIX 35=R events'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 9 of 20: hub_quote
-- Business key: quote_id (FIX tag 117)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_quote (
 quote_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 quote_bk STRING NOT NULL COMMENT 'FIX tag 117 QuoteID',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (Market Making / Streaming Quote Engine)'
)
USING DELTA
COMMENT 'Hub for Quote - MEQS / FIX 35=S events'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 10 of 20: hub_rfe
-- Business key: RFE reference ID
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_rfe (
 rfe_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 rfe_bk STRING NOT NULL COMMENT 'RFE reference ID',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (Client Portal / FIX)'
)
USING DELTA
COMMENT 'Hub for Request-For-Execution - client execution intent'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 11 of 20: hub_ioi
-- Business key: IOI reference ID
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_ioi (
 ioi_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 ioi_bk STRING NOT NULL COMMENT 'IOI reference ID (FIX tag 23 IOIID)',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (Sales Trading)'
)
USING DELTA
COMMENT 'Hub for Indication of Interest - broker non-binding advertisements'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 12 of 20: hub_order
-- Business key: cat_order_id (SEC-MANDATED - MUST be BK, not surrogate)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_order (
 order_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 order_bk STRING NOT NULL COMMENT 'cat_order_id (SEC Rule 613 mandated - NOT surrogate key)',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (OMS)'
)
USING DELTA
COMMENT 'Hub for Order - SEC-mandated; cat_order_id is the regulatory business key, never a surrogate'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 13 of 20: hub_execution
-- Business key: cat_execution_id or execution_id
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_execution (
 execution_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 execution_bk STRING NOT NULL COMMENT 'cat_execution_id (preferred) or internal execution_id',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (OMS / Matching Engine)'
)
USING DELTA
COMMENT 'Hub for Execution - CAT MEOT/MEOTQ/MEOTS/MEOF/MOOT fills'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 14 of 20: hub_allocation
-- Business key: Allocation reference ID
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_allocation (
 allocation_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 allocation_bk STRING NOT NULL COMMENT 'Allocation reference ID',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (Allocation System)'
)
USING DELTA
COMMENT 'Hub for Allocation - CAT MEPA/MEAA/MOFA post-trade allocations'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 15 of 20: hub_confirmation
-- Business key: Confirmation reference ID
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_confirmation (
 confirmation_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 confirmation_bk STRING NOT NULL COMMENT 'Confirmation reference ID (DTCC CTM / Omgeo TMI)',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (DTCC CTM / Omgeo)'
)
USING DELTA
COMMENT 'Hub for Confirmation - T+1 affirmation records'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 16 of 20: hub_clearing
-- Business key: CCP clearing reference ID
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_clearing (
 clearing_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 clearing_bk STRING NOT NULL COMMENT 'CCP clearing reference ID',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (CCP Gateway - OCC/NSCC/FICC/ICE/CME/LCH)'
)
USING DELTA
COMMENT 'Hub for Clearing - CCP novation records'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 17 of 20: hub_settlement
-- Business key: Settlement instruction ID
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_settlement (
 settlement_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 settlement_bk STRING NOT NULL COMMENT 'Settlement instruction ID',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (Custodian / CSD - DTCC/Euroclear/Clearstream/Fed)'
)
USING DELTA
COMMENT 'Hub for Settlement - DVP/FOP instructions to custodians and CSDs'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 18 of 20: hub_submission
-- Business key: Submission batch ID
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_submission (
 submission_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 submission_bk STRING NOT NULL COMMENT 'Submission batch ID (CAT/TRF/BLUESHEETS)',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (CAT Reporter)'
)
USING DELTA
COMMENT 'Hub for Submission - regulatory-report batch submissions'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 19 of 20: hub_repo
-- Business key: Repo transaction reference
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_repo (
 repo_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 repo_bk STRING NOT NULL COMMENT 'Repo transaction reference',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (Repo Trading System)'
)
USING DELTA
COMMENT 'Hub for Repo - repurchase / reverse-repo transactions under GMRA'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ----------------------------------------------------------------------------
-- Hub 20 of 20: hub_securities_loan
-- Business key: Securities loan reference
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hub_securities_loan (
 securities_loan_hk STRING NOT NULL COMMENT 'SHA-256 hub hash',
 securities_loan_bk STRING NOT NULL COMMENT 'Securities loan reference',
 load_date TIMESTAMP NOT NULL COMMENT 'Pipeline load timestamp',
 record_source STRING NOT NULL COMMENT 'Source system (Securities Lending Desk)'
)
USING DELTA
COMMENT 'Hub for Securities Loan - stock loan transactions under GMSLA'
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'compression.codec' = 'zstd', 'subject_area' = 'data_vault_silver');

-- ============================================================================
-- END OF FILE - 20 CREATE TABLE statements
-- ============================================================================
