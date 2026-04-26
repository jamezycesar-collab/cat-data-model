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
-- File: 04_agreement_model_delta.sql
-- Purpose: Enterprise Agreement Model (9 entities, Delta Lake)
-- Source: cat-refactor-skill/references/agreement-model.md
-- Scope: ISDA Master, CSA, GMRA, GMSLA, Prime Brokerage legal framework
-- Net New: agreement_schedule, collateral_haircut, repo_transaction, securities_loan
-- Notes: VARCHAR(n) -> STRING; BIGINT IDs normalized to STRING (UUID v4) to
-- preserve FK type integrity with party_role (party model) and
-- instrument (instrument model). Audit columns standardized to
-- _created_at / _updated_at GENERATED ALWAYS AS.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Entity 1 of 9: agreement (ENHANCED: 5 -> 18+ cols)
-- Master legal agreement - ISDA Master, CSA, GMRA, GMSLA, PB, Clearing, etc.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agreement (
 agreement_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 agreement_type STRING NOT NULL COMMENT 'ISDA_MASTER, CSA, GMRA, GMSLA, PRIME_BROKERAGE, CLIENT_AGREEMENT, GIVE_UP, CLEARING_AGREEMENT, NDA, MCA',
 agreement_subtype STRING COMMENT 'Version/variant: ISDA_2002, ISDA_1992, GMRA_2011, GMRA_2000, GMSLA_2010, GMSLA_2000',

 -- Parties
 party_role_id STRING NOT NULL COMMENT 'FK to party_role.party_role_id - our side of the agreement',
 counterparty_role_id STRING NOT NULL COMMENT 'FK to party_role.party_role_id - counterparty side',

 -- Agreement terms
 governing_law STRING NOT NULL COMMENT 'Governing law jurisdiction: ENGLISH_LAW, NEW_YORK_LAW, GERMAN_LAW, JAPANESE_LAW, SWISS_LAW',
 agreement_date DATE NOT NULL COMMENT 'Date agreement was signed',
 effective_date DATE NOT NULL COMMENT 'Date agreement becomes effective',
 termination_date DATE COMMENT 'Agreement termination date (NULL = evergreen)',
 agreement_status STRING NOT NULL COMMENT 'DRAFT, NEGOTIATING, ACTIVE, SUSPENDED, TERMINATED, EXPIRED',

 -- Legal references
 agreement_reference STRING NOT NULL COMMENT 'Internal agreement reference number (serves as business key for Data Vault hub)',
 external_reference STRING COMMENT 'Counterparty agreement reference (if different)',
 amendment_count INT NOT NULL DEFAULT 0 COMMENT 'Number of amendments executed',
 last_amendment_date DATE COMMENT 'Date of most recent amendment',

 -- Cross-default and termination
 cross_default_flag BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE if cross-default clause applies across agreements with this counterparty',
 cross_default_threshold DECIMAL(18,2) COMMENT 'Threshold amount for cross-default trigger',
 automatic_early_termination BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE if AET clause is elected (common in ISDA)',

 -- Netting
 close_out_netting_flag BOOLEAN NOT NULL DEFAULT TRUE COMMENT 'TRUE if close-out netting applies (standard in ISDA/GMRA/GMSLA)',
 payment_netting_flag BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE if payment netting is elected',

 -- Operational
 primary_contact_id STRING COMMENT 'FK to party_contact.contact_id - primary legal/ops contact',
 document_repository_ref STRING COMMENT 'Link to document management system (e.g., Openlink, Bloomberg TOMS)',

 -- SCD Type 2 audit
 effective_start_date DATE NOT NULL COMMENT 'SCD2 effective start date',
 effective_end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date (9999-12-31 = current record)',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Master legal agreements governing trading relationships - ISDA, CSA, GMRA, GMSLA, PB, clearing agreements; enhanced from 5 -> 18+ attributes with netting, cross-default, and SCD2'
PARTITIONED BY (agreement_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Master legal agreements - ISDA/CSA/GMRA/GMSLA/PB framework',
 'compression.codec' = 'zstd',
 'subject_area' = 'agreement',
 'source_lineage' = ' + references/agreement-model.md section agreement'
);

-- Z-ORDER BY (agreement_reference, counterparty_role_id, agreement_status)

-- ----------------------------------------------------------------------------
-- Entity 2 of 9: agreement_schedule (NEW)
-- ISDA Schedule / GMRA Annex I / GMSLA Schedule elections & terms
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agreement_schedule (
 schedule_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 agreement_id STRING NOT NULL COMMENT 'FK to agreement.agreement_id',
 schedule_section STRING NOT NULL COMMENT 'Section reference: PART_1, PART_2, PART_3, PART_4, PART_5, ANNEX_I, ANNEX_II',
 election_name STRING NOT NULL COMMENT 'Election: SPECIFIED_ENTITY, THRESHOLD_AMOUNT, CALCULATION_AGENT, BASE_CURRENCY, TERMINATION_CURRENCY, SET_OFF, AUTO_EARLY_TERM, BUY_SELL_BACK_ELECTION, NET_MARGIN, MANUFACTURED_PAYMENTS, VOTING_RIGHTS, AGENCY_PROVISIONS',
 election_value STRING NOT NULL COMMENT 'Value of the election (free-text; reference catalog in governance layer)',
 applies_to_party STRING NOT NULL COMMENT 'PARTY_A, PARTY_B, BOTH',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Elections and terms from agreement schedules (ISDA Schedule, GMRA Annex I, GMSLA Schedule); captures bespoke contractual elections per counterparty'
PARTITIONED BY (schedule_section)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Schedule elections - ISDA/GMRA/GMSLA bespoke terms',
 'compression.codec' = 'zstd',
 'subject_area' = 'agreement',
 'source_lineage' = 'references/agreement-model.md section agreement_schedule'
);

-- Z-ORDER BY (agreement_id, election_name)

-- ----------------------------------------------------------------------------
-- Entity 3 of 9: agreement_netting_set (ENHANCED)
-- Netting sets for SA-CCR capital calculation
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agreement_netting_set (
 netting_set_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 agreement_id STRING NOT NULL COMMENT 'FK to agreement.agreement_id',
 netting_set_type STRING NOT NULL COMMENT 'PAYMENT, CLOSE_OUT, MARGIN',
 netting_set_name STRING NOT NULL COMMENT 'Descriptive name for the netting set',
 eligible_product_types STRING NOT NULL COMMENT 'Comma-separated product types: IRS,CDS,FX_FORWARD,REPO,SEC_LOAN,OPTION,TRS,EQUITY_DERIV',
 eligible_currencies STRING COMMENT 'Currencies eligible for netting (NULL = all; comma-separated ISO 4217)',
 netting_frequency STRING COMMENT 'DAILY, WEEKLY, MONTHLY, AT_MATURITY',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Netting sets under agreements - critical for SA-CCR capital calculation (Basel III/IV) and close-out netting'
PARTITIONED BY (netting_set_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Netting sets - SA-CCR regulatory capital inputs',
 'compression.codec' = 'zstd',
 'subject_area' = 'agreement',
 'source_lineage' = 'references/agreement-model.md section agreement_netting_set'
);

-- Z-ORDER BY (agreement_id, netting_set_type)

-- ----------------------------------------------------------------------------
-- Entity 4 of 9: collateral_agreement (ENHANCED: 8 -> 25+ cols)
-- CSA under ISDA / GMRA margin / GMSLA collateral / CCP margin
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS collateral_agreement (
 collateral_agreement_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 agreement_id STRING NOT NULL COMMENT 'FK to agreement.agreement_id - parent ISDA / GMRA / GMSLA / PB',
 collateral_type STRING NOT NULL COMMENT 'CSA, VM_CSA, IM_CSA, GMRA_MARGIN, GMSLA_COLLATERAL, CCP_MARGIN',

 -- Thresholds (applicable to CSA and GMRA)
 threshold_amount_party_a DECIMAL(18,2) COMMENT 'Unsecured threshold for Party A before margin call triggered',
 threshold_amount_party_b DECIMAL(18,2) COMMENT 'Unsecured threshold for Party B',
 minimum_transfer_amount DECIMAL(18,2) COMMENT 'Minimum margin transfer (MTA) - reduces operational overhead',
 rounding_amount DECIMAL(18,2) COMMENT 'Rounding increment for margin calls',
 rounding_method STRING COMMENT 'UP, DOWN, NEAREST',

 -- Independent Amount (IA) / Initial Margin
 independent_amount_party_a DECIMAL(18,2) COMMENT 'Party A independent amount (effectively initial margin)',
 independent_amount_party_b DECIMAL(18,2) COMMENT 'Party B independent amount',

 -- Eligible collateral
 eligible_collateral_types STRING NOT NULL COMMENT 'CASH, GOV_BONDS, CORP_BONDS, EQUITIES, LETTERS_OF_CREDIT, GOLD (comma-separated)',
 eligible_currencies STRING NOT NULL COMMENT 'Eligible currencies for cash collateral (comma-separated ISO 4217)',

 -- Haircuts
 haircut_schedule_ref STRING COMMENT 'Reference to haircut schedule (external or FK to collateral_haircut)',

 -- Valuation
 valuation_frequency STRING NOT NULL COMMENT 'DAILY, WEEKLY, MONTHLY, INTRADAY',
 valuation_time STRING COMMENT 'Time of day for valuation (e.g., 16:00 NY)',
 valuation_agent STRING NOT NULL COMMENT 'PARTY_A, PARTY_B, SHARED, CCP',

 -- Dispute resolution
 dispute_resolution_method STRING NOT NULL COMMENT 'RECALCULATION, MARKET_QUOTATION, LOSS',
 dispute_resolution_period INT NOT NULL COMMENT 'Days to resolve dispute (typically 2-5 business days)',

 -- Rehypothecation / Right of use
 rehypothecation_flag BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE if receiving party can re-use pledged collateral (common in PB, not in bilateral CSA)',

 -- Substitution
 substitution_rights STRING NOT NULL DEFAULT 'NONE' COMMENT 'NONE, WITH_CONSENT, WITHOUT_CONSENT',

 -- Regulatory
 regulatory_im_flag BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE if subject to UMR (Uncleared Margin Rules) / BCBS-IOSCO Phase 6',

 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Collateral and margin terms - CSA, GMRA margin, GMSLA collateral, CCP margin; covers thresholds, IA, eligible collateral, rehypothecation, and UMR regulatory IM'
PARTITIONED BY (collateral_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Collateral agreement - CSA/GMRA margin/GMSLA collateral',
 'compression.codec' = 'zstd',
 'subject_area' = 'agreement',
 'source_lineage' = 'references/agreement-model.md section collateral_agreement'
);

-- Z-ORDER BY (agreement_id, collateral_type)

-- ----------------------------------------------------------------------------
-- Entity 5 of 9: collateral_haircut (NEW)
-- Haircut schedule for eligible collateral types per agreement
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS collateral_haircut (
 haircut_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 collateral_agreement_id STRING NOT NULL COMMENT 'FK to collateral_agreement.collateral_agreement_id',
 collateral_category STRING NOT NULL COMMENT 'CASH, UST_LESS_1Y, UST_1_5Y, UST_5_10Y, UST_10Y_PLUS, AGENCY_MBS, CORP_IG, CORP_HY, EQUITY_DEVELOPED, EQUITY_EM, GOLD',
 currency STRING COMMENT 'ISO 4217 code - NULL means all currencies',
 haircut_percentage DECIMAL(8,4) NOT NULL COMMENT 'Haircut as decimal (e.g., 0.02 = 2%); market_value × (1 - haircut) = collateral value',
 fx_haircut_percentage DECIMAL(8,4) COMMENT 'Additional FX haircut if collateral currency differs from exposure currency',
 concentration_limit_pct DECIMAL(8,4) COMMENT 'Maximum % of total collateral pool from this category',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Haircut schedules for collateral categories - drives collateral valuation under CSA/GMRA/GMSLA; supports FX haircuts and concentration limits'
PARTITIONED BY (collateral_category)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Collateral haircut schedules',
 'compression.codec' = 'zstd',
 'subject_area' = 'agreement',
 'source_lineage' = 'references/agreement-model.md section collateral_haircut'
);

-- Z-ORDER BY (collateral_agreement_id, collateral_category)

-- ----------------------------------------------------------------------------
-- Entity 6 of 9: margin_call (ENHANCED: 10 -> 22+ cols)
-- Daily margin call records - VM, IM, regulatory, intraday
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS margin_call (
 margin_call_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 collateral_agreement_id STRING NOT NULL COMMENT 'FK to collateral_agreement.collateral_agreement_id',

 -- Call details
 call_date DATE NOT NULL COMMENT 'Business date of margin call',
 call_type STRING NOT NULL COMMENT 'VARIATION, INITIAL, REGULATORY_IM, INTRADAY, RETURN',
 call_direction STRING NOT NULL COMMENT 'DELIVER (we owe) or RECEIVE (they owe)',

 -- Amounts
 exposure_amount DECIMAL(18,2) NOT NULL COMMENT 'Gross exposure before collateral',
 collateral_held DECIMAL(18,2) NOT NULL COMMENT 'Current collateral value held',
 threshold_amount DECIMAL(18,2) NOT NULL COMMENT 'Applicable threshold',
 call_amount DECIMAL(18,2) NOT NULL COMMENT 'Net margin call amount',
 call_currency STRING NOT NULL COMMENT 'ISO 4217 currency code',
 agreed_amount DECIMAL(18,2) COMMENT 'Amount agreed after dispute resolution (if different from call_amount)',

 -- Status and timeline
 call_status STRING NOT NULL COMMENT 'CALCULATED, ISSUED, AGREED, DISPUTED, PARTIALLY_SETTLED, SETTLED, CANCELLED',
 issued_timestamp TIMESTAMP COMMENT 'When margin call was issued to counterparty',
 response_deadline TIMESTAMP COMMENT 'Deadline for counterparty response',
 agreed_timestamp TIMESTAMP COMMENT 'When counterparty agreed to the call',
 settled_timestamp TIMESTAMP COMMENT 'When collateral was actually delivered/received',

 -- Collateral movement
 collateral_delivered_amount DECIMAL(18,2) COMMENT 'Actual collateral delivered',
 collateral_type_delivered STRING COMMENT 'What was delivered: CASH_USD, UST_10Y, EQUITY_INDEX, etc.',

 -- Dispute
 dispute_flag BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE if call was disputed',
 dispute_amount DECIMAL(18,2) COMMENT 'Amount in dispute',
 dispute_resolution_date DATE COMMENT 'Date dispute was resolved',

 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Daily margin call records - VM, IM, regulatory margin, intraday calls; covers ISDA/CSA, GMRA, GMSLA, and CCP margin lifecycle'
PARTITIONED BY (call_date)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Margin call lifecycle - VM/IM/regulatory/intraday',
 'compression.codec' = 'zstd',
 'subject_area' = 'agreement',
 'source_lineage' = 'references/agreement-model.md section margin_call'
);

-- Z-ORDER BY (collateral_agreement_id, call_status)

-- ----------------------------------------------------------------------------
-- Entity 7 of 9: repo_transaction (NEW - GMRA-specific)
-- Classic repo, buy/sell back, open repo, term repo, tri-party
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS repo_transaction (
 repo_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 agreement_id STRING NOT NULL COMMENT 'FK to agreement.agreement_id (agreement_type = GMRA)',

 -- Transaction type
 repo_type STRING NOT NULL COMMENT 'CLASSIC_REPO, BUY_SELL_BACK, OPEN_REPO, TERM_REPO, TRI_PARTY',
 repo_direction STRING NOT NULL COMMENT 'REPO (we sell & agree to repurchase) or REVERSE (we buy & agree to resell)',

 -- Parties
 seller_party_role_id STRING NOT NULL COMMENT 'FK to party_role.party_role_id - seller (cash borrower)',
 buyer_party_role_id STRING NOT NULL COMMENT 'FK to party_role.party_role_id - buyer (cash lender)',

 -- Collateral (purchased securities)
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id - purchased security',
 nominal_quantity DECIMAL(18,4) NOT NULL COMMENT 'Face/nominal amount of securities',
 purchase_price DECIMAL(18,6) NOT NULL COMMENT 'Purchase price (clean price)',
 purchase_date DATE NOT NULL COMMENT 'Settlement date of near leg (purchase)',
 repurchase_date DATE COMMENT 'Settlement date of far leg (repurchase); NULL for open repo',
 repurchase_price DECIMAL(18,6) COMMENT 'Repurchase price; NULL for open repo',

 -- Economics
 repo_rate DECIMAL(8,6) NOT NULL COMMENT 'Repo rate (annualized)',
 repo_interest DECIMAL(18,2) COMMENT 'Repo interest = purchase_price × repo_rate × term / day_count',
 day_count_convention STRING NOT NULL COMMENT 'ACT_360, ACT_365, 30_360',
 cash_currency STRING NOT NULL COMMENT 'ISO 4217 currency of cash leg',

 -- Margin
 initial_margin_pct DECIMAL(8,4) COMMENT 'Initial margin / haircut (e.g., 1.02 = 2% over-collateralization)',
 margin_maintenance_pct DECIMAL(8,4) COMMENT 'Maintenance margin level',

 -- Status
 repo_status STRING NOT NULL COMMENT 'OPEN, SETTLED_NEAR, SETTLED_FAR, ROLLED, TERMINATED_EARLY, DEFAULTED',

 -- Substitution
 substitution_allowed BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE if seller can substitute collateral securities',

 -- Tri-party
 tri_party_agent_id STRING COMMENT 'FK to party.party_id - tri-party agent (BNY Mellon, JP Morgan, Euroclear)',

 event_date DATE NOT NULL COMMENT 'Partition date - typically purchase_date',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Repo and reverse repo transactions under GMRA framework - classic repo, buy/sell back, open repo, term repo, tri-party'
PARTITIONED BY (event_date)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'GMRA repo transactions',
 'compression.codec' = 'zstd',
 'subject_area' = 'agreement',
 'source_lineage' = 'references/agreement-model.md section repo_transaction'
);

-- Z-ORDER BY (agreement_id, instrument_id, repo_status)

-- ----------------------------------------------------------------------------
-- Entity 8 of 9: securities_loan (NEW - GMSLA-specific)
-- Securities lending & borrowing - open loans, term loans, recalls, buy-ins
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS securities_loan (
 loan_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 agreement_id STRING NOT NULL COMMENT 'FK to agreement.agreement_id (agreement_type = GMSLA)',

 -- Transaction type
 loan_type STRING NOT NULL COMMENT 'OPEN_LOAN, TERM_LOAN, EXCLUSIVE_LOAN',
 our_role STRING NOT NULL COMMENT 'LENDER or BORROWER',

 -- Parties
 lender_party_role_id STRING NOT NULL COMMENT 'FK to party_role.party_role_id - securities lender',
 borrower_party_role_id STRING NOT NULL COMMENT 'FK to party_role.party_role_id - securities borrower',
 lending_agent_party_id STRING COMMENT 'FK to party.party_id - agent lender (e.g., BlackRock, State Street)',

 -- Loaned securities
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id - loaned security',
 loan_quantity DECIMAL(18,4) NOT NULL COMMENT 'Quantity of securities on loan',
 loan_value DECIMAL(18,2) NOT NULL COMMENT 'Market value of loaned securities at inception',

 -- Dates
 loan_date DATE NOT NULL COMMENT 'Date loan was initiated (settlement date of delivery)',
 return_date DATE COMMENT 'Agreed return date; NULL for open loans (recallable on demand)',
 actual_return_date DATE COMMENT 'Actual date securities were returned',
 recall_date DATE COMMENT 'Date lender issued recall notice',
 recall_notice_period INT COMMENT 'Standard notice period for recall (business days)',

 -- Economics
 fee_rate DECIMAL(8,6) NOT NULL COMMENT 'Securities lending fee rate (annualized); paid by borrower to lender',
 rebate_rate DECIMAL(8,6) COMMENT 'Rebate rate on cash collateral (= benchmark rate - fee); NULL for non-cash collateral',
 fee_accrued DECIMAL(18,2) COMMENT 'Accrued fee to date',

 -- Collateral
 collateral_type STRING NOT NULL COMMENT 'CASH, NON_CASH, LETTERS_OF_CREDIT, TRI_PARTY',
 collateral_currency STRING COMMENT 'ISO 4217 currency of cash collateral (NULL for non-cash)',
 collateral_margin_pct DECIMAL(8,4) NOT NULL COMMENT 'Required collateral margin (e.g., 1.02 for domestic equities, 1.05 for international)',
 collateral_value DECIMAL(18,2) NOT NULL COMMENT 'Current collateral value held against this loan',

 -- Manufactured payments
 manufactured_payment_flag BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE if dividends/coupons on loaned securities are owed as manufactured payments from borrower to lender',

 -- Voting
 voting_rights_retained BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE if lender retains voting rights (requires recall before record date)',

 -- Status
 loan_status STRING NOT NULL COMMENT 'OPEN, ON_LOAN, RECALLED, PARTIALLY_RETURNED, RETURNED, BUY_IN, DEFAULTED',

 -- Buy-in (failure to return)
 buy_in_flag BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE if buy-in was executed due to borrower failure to return',
 buy_in_date DATE COMMENT 'Date of buy-in execution',
 buy_in_price DECIMAL(18,6) COMMENT 'Price at which buy-in was executed',

 event_date DATE NOT NULL COMMENT 'Partition date - typically loan_date',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Securities lending and borrowing transactions under GMSLA framework - open loans, term loans, recalls, returns, and buy-ins'
PARTITIONED BY (event_date)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'GMSLA securities loans',
 'compression.codec' = 'zstd',
 'subject_area' = 'agreement',
 'source_lineage' = 'references/agreement-model.md section securities_loan'
);

-- Z-ORDER BY (agreement_id, instrument_id, loan_status)

-- ----------------------------------------------------------------------------
-- Entity 9 of 9: commission_schedule (ENHANCED: 8 -> 16+ cols)
-- Commission and fee schedules - per-share, BPS, flat, tiered
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS commission_schedule (
 schedule_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 agreement_id STRING COMMENT 'FK to agreement.agreement_id - NULL if firm-wide default',
 account_id STRING COMMENT 'FK to account.account_id - NULL if agreement-level',

 -- Scope
 instrument_type STRING NOT NULL COMMENT 'EQUITY, OPTION, FIXED_INCOME, FX, REPO, SEC_LOAN, ALL',
 asset_class STRING COMMENT 'Filter by asset class (NULL = all within instrument_type)',
 venue_type STRING COMMENT 'EXCHANGE, ATS, OTC - NULL means all',

 -- Rate
 rate_type STRING NOT NULL COMMENT 'PER_SHARE, BPS, FLAT, PERCENTAGE, TIERED',
 rate_value DECIMAL(12,6) NOT NULL COMMENT 'Rate value (e.g., 0.005 = $0.005/share, or 0.0003 = 3 bps)',
 min_commission DECIMAL(18,2) COMMENT 'Minimum commission per trade',
 max_commission DECIMAL(18,2) COMMENT 'Maximum commission per trade',

 -- Tiered pricing
 tier_threshold DECIMAL(18,2) COMMENT 'Quantity/volume threshold for tier transition',
 tier_rate_above DECIMAL(12,6) COMMENT 'Rate above threshold',

 -- Soft dollar / CSA (Commission Sharing Agreement - distinct from Credit Support Annex)
 soft_dollar_flag BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE if part of a commission sharing arrangement',
 soft_dollar_pct DECIMAL(8,4) COMMENT 'Percentage of commission allocated to soft dollar/research',

 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 record_source STRING NOT NULL COMMENT 'Source system lineage identifier',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Commission and fee schedules by agreement / account / instrument type - per-share, BPS, flat, tiered pricing plus soft-dollar (Commission Sharing Agreement)'
PARTITIONED BY (instrument_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Commission schedules - per-share/BPS/flat/tiered',
 'compression.codec' = 'zstd',
 'subject_area' = 'agreement',
 'source_lineage' = 'references/agreement-model.md section commission_schedule'
);

-- Z-ORDER BY (agreement_id, account_id, instrument_type)

-- ============================================================================
-- END OF FILE - 9 CREATE TABLE statements
-- ============================================================================
