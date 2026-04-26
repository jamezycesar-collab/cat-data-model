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
-- File: 02a_instrument_core_delta.sql
-- Purpose: Enterprise Instrument Model - Core + Cash Subtypes (5 entities, Delta Lake)
-- equity_attributes, option_attributes, fixed_income_attributes enriched;
-- digital_asset_attributes retained.
-- Inheritance: every subtype has instrument_id BIGINT/STRING NOT NULL FK to instrument.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Entity 1 of 5: instrument (supertype) - ENHANCED
-- instrument_group (CASH/DERIVATIVE/STRUCTURED),
-- isda_product_type, notional_currency, is_otc
-- (maturity_date already present; retained)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instrument (
 instrument_id STRING NOT NULL COMMENT 'UUID v4 - Instrument supertype primary key',
 instrument_type STRING NOT NULL COMMENT 'Discriminator: EQUITY, OPTION, FIXED_INCOME, DIGITAL_ASSET, FX, COMMODITY, DERIVATIVE, STRUCTURED (FK-enforceable to ref_instrument_type)',
 instrument_group STRING COMMENT 'NEW - High-level group: CASH (spot securities), DERIVATIVE (OTC/listed contracts), STRUCTURED (multi-component products); drives capital treatment and trade reporting routing',
 primary_symbol STRING NOT NULL COMMENT 'Primary trading symbol (e.g., AAPL, SPY, ES= for futures)',
 instrument_name STRING COMMENT 'Full descriptive name (e.g., Apple Inc. Common Stock)',
 instrument_status STRING NOT NULL COMMENT 'Lifecycle: ACTIVE, SUSPENDED, HALTED, DELISTED, MATURED, DEFAULTED, BANKRUPTCY',
 isda_product_type STRING COMMENT 'NEW - ISDA FpML product taxonomy value (e.g., VanillaOption, IRSwap, CreditDefaultSwap, TotalReturnSwap, FXForward); canonical product ID for OTC derivatives',
 is_otc BOOLEAN DEFAULT false COMMENT 'NEW - TRUE if OTC (off-exchange, bilateral); FALSE for exchange-listed',
 issue_date DATE COMMENT 'Original issue date (IPO for equities, issuance for bonds, trade date for OTC derivatives)',
 maturity_date DATE COMMENT 'Maturity / expiration date (null for perpetual equities)',
 primary_currency STRING NOT NULL DEFAULT 'USD' COMMENT 'ISO 4217 primary trading currency',
 notional_currency STRING COMMENT 'NEW - ISO 4217 currency for notional amount (may differ from trading currency for cross-currency swaps, FX forwards)',
 country_of_risk STRING COMMENT 'ISO 3166-1 alpha-2 country code for credit/political risk',
 created_date DATE NOT NULL COMMENT 'Record creation date',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Instrument supertype (ISO 20022 Security) - enriched with instrument_group (CASH/DERIVATIVE/STRUCTURED), ISDA product taxonomy, notional currency, and OTC indicator for enterprise coverage'
PARTITIONED BY (instrument_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Instrument supertype with ISDA/FpML product taxonomy and OTC flag',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- Z-ORDER BY (instrument_id, primary_symbol)
-- LIQUID CLUSTERING BY (instrument_group, instrument_status, is_otc)

-- ----------------------------------------------------------------------------
-- Entity 2 of 5: equity_attributes (subtype) - ENHANCED
-- dividend_frequency, voting_rights (already boolean - renamed), free_float_pct, index_membership
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS equity_attributes (
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id - PK of 1:1 subtype (inheritance)',
 share_class STRING COMMENT 'Share class: A, B, C, D, COMMON, PREFERRED, TRACKING, CUMULATIVE_PREFERRED, CONVERTIBLE_PREFERRED',
 voting_rights_flag BOOLEAN COMMENT 'Whether shares carry voting rights (renamed from voting_rights_indicator boolean convention)',
 dividend_type STRING COMMENT 'CASH, STOCK, NONE, SPECIAL, SPECIAL_RECURRING',
 dividend_frequency STRING COMMENT 'NEW - Declared cadence: QUARTERLY, SEMI_ANNUAL, ANNUAL, MONTHLY, IRREGULAR, NONE',
 par_value DECIMAL(18,8) COMMENT 'Par / stated value per share',
 shares_outstanding BIGINT COMMENT 'Total shares outstanding (as of reference date)',
 shares_float BIGINT COMMENT 'Public float shares (excludes restricted/insiders)',
 free_float_pct DECIMAL(7,4) COMMENT 'NEW - Free float percentage (0.0000 to 100.0000); drives index eligibility and liquidity assessment',
 market_cap_tier STRING COMMENT 'Category: MEGA (>200B), LARGE (10-200B), MID (2-10B), SMALL (300M-2B), MICRO (50-300M), NANO (<50M)',
 index_membership STRING COMMENT 'NEW - Comma-separated list of major index memberships (SP500, RUSSELL2000, DJIA, NASDAQ100, FTSE100, STOXX50); stored as string due to small, slowly-changing cardinality',
 primary_exchange_mic STRING NOT NULL COMMENT 'ISO 10383 Market Identifier Code (e.g., XNYS for NYSE); FK to ref_mic',
 nms_indicator BOOLEAN NOT NULL DEFAULT true COMMENT 'NMS stock flag; required for CAT reporting (Rule 613)',
 reg_sho_threshold_flag BOOLEAN DEFAULT false COMMENT 'Reg SHO threshold security flag (renamed)',
 adr_flag BOOLEAN DEFAULT false COMMENT 'American Depositary Receipt flag (renamed)',
 etf_flag BOOLEAN DEFAULT false COMMENT 'Exchange-Traded Fund/Product flag (renamed)',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Equity-specific attributes (inheritance subtype) - enriched with dividend frequency, free float %, and index membership for liquidity / index attribution'
PARTITIONED BY (primary_exchange_mic)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Equity dimension with dividend frequency, free float, and index membership',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- Inheritance FK: CONSTRAINT fk_equity_instrument FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id) [enforced in ]
-- Z-ORDER BY (instrument_id, primary_exchange_mic)
-- LIQUID CLUSTERING BY (nms_indicator, market_cap_tier, etf_flag)

-- ----------------------------------------------------------------------------
-- Entity 3 of 5: option_attributes (subtype) - ENHANCED
-- exercise_style (already present), settlement_method (rename settlement_type),
-- contract_multiplier (already present), barrier_type, barrier_level
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS option_attributes (
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id - PK of 1:1 subtype',
 underlying_instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id - underlying security (self-ref via instrument)',
 strike_price DECIMAL(18,8) NOT NULL COMMENT 'Strike / exercise price (absolute, not adjusted for splits/dividends)',
 expiration_date DATE NOT NULL COMMENT 'Option expiration date',
 put_call_indicator STRING NOT NULL COMMENT 'PUT or CALL',
 exercise_style STRING NOT NULL COMMENT 'AMERICAN (anytime before expiry), EUROPEAN (at expiry only), BERMUDAN (discrete dates)',
 contract_multiplier DECIMAL(18,4) NOT NULL DEFAULT 100 COMMENT 'Contract multiplier (100 for standard equity options, 10 for mini, 1000 for some FX)',
 settlement_method STRING NOT NULL COMMENT 'Renamed from settlement_type per : PHYSICAL (deliver underlying) or CASH (cash settle intrinsic)',
 option_type STRING NOT NULL COMMENT 'STANDARD, NON_STANDARD, FLEX, BINARY, MINI, WEEKLY, QUARTERLY, LEAPS',
 barrier_type STRING COMMENT 'NEW - For exotic/barrier options: KNOCK_IN, KNOCK_OUT, UP_AND_IN, UP_AND_OUT, DOWN_AND_IN, DOWN_AND_OUT, DOUBLE_BARRIER, NONE',
 barrier_level DECIMAL(18,8) COMMENT 'NEW - Barrier trigger level (null for vanilla options); used with barrier_type for exotic pricing',
 occ_symbol STRING COMMENT 'OCC clearing symbol (21-char format: ROOT[6] + YYMMDD[6] + C/P[1] + STRIKE[8])',
 series_name STRING COMMENT 'Full series description with maturity + strike',
 open_interest BIGINT COMMENT 'Current open interest in contracts',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Option-specific attributes (inheritance subtype) - enriched with barrier_type / barrier_level for exotic options (knock-in/out)'
PARTITIONED BY (expiration_date)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Option dimension with exercise style, settlement method, and barrier features',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- Inheritance FK: CONSTRAINT fk_option_instrument FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
-- Self-FK: CONSTRAINT fk_option_underlying FOREIGN KEY (underlying_instrument_id) REFERENCES instrument(instrument_id)
-- Z-ORDER BY (underlying_instrument_id, expiration_date, strike_price, put_call_indicator)

-- ----------------------------------------------------------------------------
-- Entity 4 of 5: fixed_income_attributes (subtype) - ENHANCED
-- day_count_convention (already present), payment_frequency (rename coupon_frequency),
-- accrued_interest_calc, callable_flag (rename), puttable_flag (rename), benchmark_spread (already via spread_bps)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fixed_income_attributes (
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id - PK of 1:1 subtype',
 issuer_party_id STRING COMMENT 'FK to party.party_id - bond issuer',
 coupon_rate DECIMAL(10,6) COMMENT 'Annual coupon rate as decimal (e.g., 0.052500 for 5.25%)',
 payment_frequency STRING COMMENT 'Renamed from coupon_frequency per : ANNUAL, SEMI_ANNUAL, QUARTERLY, MONTHLY, ZERO_COUPON, FLOATING, VARIABLE',
 face_value DECIMAL(18,4) COMMENT 'Par / face value per unit (typically 1000 or 100)',
 day_count_convention STRING COMMENT 'ISDA 2006 day count (FK to ref_day_count): 30_360, ACT_360, ACT_365_FIXED, ACT_ACT_ISDA, ACT_365L, 30E_360, BUS_252',
 accrued_interest_calc STRING COMMENT 'NEW - Method for accrued interest: LINEAR, COMPOUNDED, YIELD_BASED, MARKET_CONVENTION; interacts with day_count for settlement calc',
 credit_rating STRING COMMENT 'Composite credit rating (denormalized snapshot; authoritative data in party_credit_rating/instrument rating)',
 rating_agency STRING COMMENT 'Rating agency for credit_rating snapshot: SP, MOODYS, FITCH, DBRS, JCRA',
 seniority STRING COMMENT 'SENIOR_SECURED, SENIOR_UNSECURED, SUBORDINATED, JUNIOR, MEZZANINE, EQUITY_TRANCHE',
 callable_flag BOOLEAN DEFAULT false COMMENT 'Whether bond is callable by issuer (renamed from callable_indicator)',
 puttable_flag BOOLEAN DEFAULT false COMMENT 'Whether bondholder has put option (renamed from puttable_indicator)',
 convertible_flag BOOLEAN DEFAULT false COMMENT 'Convertible to equity flag (renamed from convertible_indicator)',
 bond_type STRING COMMENT 'TREASURY, AGENCY, CORPORATE, MUNICIPAL, SOVEREIGN, STRUCTURED, COVERED, PERPETUAL, MBS, ABS',
 benchmark_index STRING COMMENT 'Reference rate (for FRN): SOFR, EURIBOR, SONIA, TONA, CDOR, LIBOR (legacy)',
 benchmark_spread DECIMAL(10,4) COMMENT 'NEW - Benchmark spread as decimal (e.g., 0.0150 for +150 bps); supersedes spread_bps naming; spread_bps retained for backward compat',
 spread_bps INT COMMENT 'DEPRECATED - retained for source compat; prefer benchmark_spread (decimal)',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Fixed income attributes (inheritance subtype) - enriched with payment_frequency, accrued_interest_calc, and benchmark_spread'
PARTITIONED BY (bond_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Fixed income dimension with day count, payment frequency, and call/put features',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- Inheritance FK: CONSTRAINT fk_fi_instrument FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
-- Issuer FK: CONSTRAINT fk_fi_issuer FOREIGN KEY (issuer_party_id) REFERENCES party(party_id)
-- Z-ORDER BY (instrument_id, maturity_date, coupon_rate)

-- ----------------------------------------------------------------------------
-- Entity 5 of 5: digital_asset_attributes (subtype) - UNCHANGED PER 
-- Retained from source with inheritance FK discipline documented
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS digital_asset_attributes (
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id - PK of 1:1 subtype',
 blockchain_network STRING NOT NULL COMMENT 'ETHEREUM, BITCOIN, SOLANA, POLYGON, AVALANCHE, CARDANO, NEAR, APTOS, ARBITRUM, OPTIMISM, BASE',
 token_standard STRING COMMENT 'ERC20, ERC721, ERC1155, SPL, BEP20, TRC20, NATIVE',
 contract_address STRING COMMENT 'Smart contract address on-chain (e.g., 0x-prefixed 40-char for EVM)',
 consensus_mechanism STRING COMMENT 'PROOF_OF_STAKE, PROOF_OF_WORK, DELEGATED_POS, HYBRID, BYZANTINE_FAULT_TOLERANT',
 total_supply DECIMAL(38,18) COMMENT 'Maximum token supply (null = uncapped)',
 circulating_supply DECIMAL(38,18) COMMENT 'Current circulating supply',
 security_classification STRING COMMENT 'SEC classification: SECURITY, COMMODITY, UTILITY, MIXED, UNDETERMINED; drives Howey / Reves analysis',
 decimal_precision INT COMMENT 'Token decimals (18 for ETH, 8 for BTC, 6 for USDC on some chains)',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Digital asset attributes (inheritance subtype) - unchanged per ; retained for tokens, crypto, NFTs'
PARTITIONED BY (blockchain_network)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Digital asset dimension (tokens, crypto, NFTs)',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- Inheritance FK: CONSTRAINT fk_da_instrument FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
-- Z-ORDER BY (blockchain_network, contract_address)

-- ============================================================================
-- END OF FILE - 5 CREATE TABLE statements
-- Inheritance preserved: all 4 subtypes have instrument_id NOT NULL with FK
-- ============================================================================
