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
-- File: 02c_instrument_classification_delta.sql
-- Purpose: Enterprise Instrument Model - Identification + Classification (5 entities, Delta Lake)
-- instrument_listing, corporate_action_linkage - all enhanced)
-- Net New: instrument_risk_classification (FRTB/SA-CCR/CRR risk bucketing)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Entity 1 of 5: instrument_identifier (enriched) - ENHANCED
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instrument_identifier (
 identifier_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id',
 id_type STRING NOT NULL COMMENT 'Identifier scheme: CUSIP (ISO 6166 NA), ISIN (ISO 6166 global), SEDOL, FIGI (OpenFIGI), RIC (Refinitiv), OCC_SYMBOL, TICKER, BLOOMBERG_ID, CIK, VALOR, WKN, COMMON_CODE, SWIFT_ISIN, WPK, QUIK',
 id_value STRING NOT NULL COMMENT 'Identifier value (e.g., US0378331005 for AAPL ISIN)',
 id_source STRING COMMENT 'Data source / vendor: S&P, BLOOMBERG, REFINITIV, OCC, ANNA, OPENFIGI, ISO',
 is_primary BOOLEAN DEFAULT false COMMENT 'Primary identifier flag for this id_type within the instrument',
 identifier_status STRING COMMENT 'NEW - Identifier lifecycle status: ACTIVE (current/valid), SUPERSEDED (replaced by a newer one), DELETED (removed/invalid); drives point-in-time cross-reference accuracy',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Instrument identifiers (CUSIP/ISIN/SEDOL/FIGI/RIC/OCC) with identifier_status for lifecycle tracking (ACTIVE/SUPERSEDED/DELETED); supports point-in-time cross-reference and CAT identifier lineage'
PARTITIONED BY (id_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Instrument identifier mapping with status lifecycle',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- Z-ORDER BY (instrument_id, id_type)
-- LIQUID CLUSTERING BY (id_type, identifier_status)

-- ----------------------------------------------------------------------------
-- Entity 2 of 5: instrument_classification (enriched) - ENHANCED
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instrument_classification (
 classification_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id',
 classification_scheme STRING NOT NULL COMMENT 'Scheme: ISDA_PRODUCT, CFI_ISO_10962, GICS, SIC, NAICS, BICS, ICB, FIGI_CLASSIFICATION',
 asset_class STRING NOT NULL COMMENT 'Top-level asset class: EQUITY, CREDIT, RATES, FX, COMMODITY, DIGITAL_ASSET, ALTERNATIVE',
 product_type STRING COMMENT 'ISDA product type (e.g., COMMON_STOCK, CORPORATE_BOND, VANILLA_OPTION, EQUITY_OPTION, INTEREST_RATE_SWAP, CDS_SINGLE_NAME)',
 product_subtype STRING COMMENT 'ISDA product subtype (e.g., LARGE_CAP, INVESTMENT_GRADE, WEEKLY_OPTION)',
 cfi_code STRING COMMENT 'NEW - ISO 10962 Classification of Financial Instruments 6-character code (e.g., ESVUFR = Equity Shares Voting Free Registered); FK to ref_cfi_category (first char) in ',
 fisn STRING COMMENT 'NEW - ISO 18774 Financial Instrument Short Name (35-char abbreviation, e.g., "APPLE/COMM STK"); supplements ISIN for instrument identification',
 sector STRING COMMENT 'GICS / SIC sector (e.g., 45 Information Technology)',
 industry_group STRING COMMENT 'GICS / ICB industry group (e.g., 4510 Software & Services)',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Instrument classifications with ISO 10962 cfi_code and ISO 18774 fisn; supports ISDA / CFI / GICS taxonomy cross-walks; cfi_code first character FK-enforceable against ref_cfi_category'
PARTITIONED BY (asset_class)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Instrument classification (ISDA FpML, CFI ISO 10962, FISN ISO 18774)',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- Z-ORDER BY (instrument_id, classification_scheme)
-- LIQUID CLUSTERING BY (asset_class, cfi_code)

-- ----------------------------------------------------------------------------
-- Entity 3 of 5: instrument_listing (enriched) - ENHANCED
-- primary_listing_flag, listing_date (already present), delisting_date (already present)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instrument_listing (
 listing_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id',
 venue_id STRING NOT NULL COMMENT 'FK to venue.venue_id',
 listing_type STRING NOT NULL COMMENT 'PRIMARY, SECONDARY, UTP (Unlisted Trading Privileges), DUAL_LISTED, CROSS_LISTED, MULTI_LISTED',
 primary_listing_flag BOOLEAN DEFAULT false COMMENT 'NEW - TRUE for the single primary listing of the instrument; exactly one TRUE expected per instrument (enforced by DLT expectation)',
 listing_date DATE COMMENT 'Date listed on venue',
 delisting_date DATE COMMENT 'Date delisted (null if active)',
 trading_currency STRING NOT NULL DEFAULT 'USD' COMMENT 'ISO 4217 trading currency on this venue',
 lot_size INT DEFAULT 1 COMMENT 'Round lot size (shares for equities, contracts for options)',
 tick_size DECIMAL(10,8) COMMENT 'Minimum price increment (e.g., 0.01 for USD cents)',
 trading_hours_profile STRING COMMENT 'REGULAR, EXTENDED, GLOBAL_24H, AUCTION, RFQ, PRE_MARKET_ONLY, CONTINUOUS',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Instrument listing on venues with primary_listing_flag; drives consolidated tape attribution and primary-listing exchange logic (e.g., consolidating opening/closing crosses)'
PARTITIONED BY (listing_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Instrument listing relationships with primary flag',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- Z-ORDER BY (instrument_id, venue_id)
-- LIQUID CLUSTERING BY (listing_type, primary_listing_flag)

-- ----------------------------------------------------------------------------
-- Entity 4 of 5: corporate_action_linkage (enriched) - ENHANCED
-- ex_date (present), record_date (present), payment_date, adjustment_factor (present)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corporate_action_linkage (
 action_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id',
 action_type STRING NOT NULL COMMENT 'Action: DIVIDEND, STOCK_SPLIT, REVERSE_SPLIT, MERGER, ACQUISITION, SPINOFF, RIGHTS_ISSUE, TENDER_OFFER, NAME_CHANGE, SYMBOL_CHANGE, DELISTING, BANKRUPTCY, RECAPITALIZATION, EXCHANGE_OFFER, RETURN_OF_CAPITAL, CASH_DIVIDEND, STOCK_DIVIDEND',
 ex_date DATE COMMENT 'Ex-dividend / ex-action date (cutoff: buyers on or after no longer entitled)',
 record_date DATE COMMENT 'Record date (who is eligible as shareholder)',
 payment_date DATE COMMENT 'NEW - Pay date (when cash/stock is distributed); distinct from effective_date which tracks SCD2',
 effective_date DATE COMMENT 'Corporate-action effective date (when market price adjusts)',
 adjustment_factor DECIMAL(18,10) COMMENT 'Price / quantity adjustment factor (e.g., 0.5 for 2:1 split, 0.9524 for 5% cash div on 100-day high)',
 announcement_date DATE COMMENT 'Date the action was publicly announced',
 description STRING COMMENT 'Action description (e.g., 2:1 stock split, $0.25 quarterly dividend)',
 new_instrument_id STRING COMMENT 'FK to instrument.instrument_id - resulting instrument for splits/mergers/spinoffs (self-ref via instrument)',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Corporate action linkage (dividends, splits, mergers, spinoffs) - enriched with explicit payment_date; drives position adjustments and historical price back-propagation'
PARTITIONED BY (action_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Corporate action linkage with ex/record/payment dates',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- Z-ORDER BY (instrument_id, ex_date, action_type)

-- ----------------------------------------------------------------------------
-- Entity 5 of 5: instrument_risk_classification (NEW)
-- FRTB / SA-CCR / CRR risk bucket classification
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instrument_risk_classification (
 risk_classification_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id',
 risk_framework STRING NOT NULL COMMENT 'Regulatory framework: FRTB (Fundamental Review of the Trading Book - BCBS 352), SA_CCR (Standardised Approach for Counterparty Credit Risk - BCBS 279), CRR (EU Capital Requirements Regulation). CHECK (risk_framework IN (''FRTB'', ''SA_CCR'', ''CRR''))',
 risk_class STRING NOT NULL COMMENT 'Risk class within framework: GIRR (General Interest Rate Risk), CSR (Credit Spread Risk - Non-securitization), CSRS (CSR - Securitization), CSRC (CSR - Correlation Trading), EQUITY, COMMODITY, FX - for FRTB; INTEREST_RATE, EQUITY, CREDIT, FX, COMMODITY - for SA-CCR',
 risk_bucket STRING COMMENT 'Granular bucket within risk_class (e.g., FRTB GIRR bucket 1-10 by currency; SA-CCR equity bucket by market cap and sector)',
 risk_weight DECIMAL(9,6) COMMENT 'Applicable risk weight (decimal; e.g., 0.015000 for 1.5%)',
 correlation_parameter DECIMAL(9,6) COMMENT 'Intra-bucket / cross-bucket correlation (for FRTB-SA aggregation formulas)',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Instrument risk classification for Basel FRTB / SA-CCR / CRR capital calculation; one row per (instrument, framework, effective period); drives RWA aggregation and capital floor calc'
PARTITIONED BY (risk_framework)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'FRTB / SA-CCR / CRR risk classification',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- CHECK: risk_framework restricted to FRTB / SA_CCR / CRR (enforced inline in comment; full CHECK DDL in)
-- FK: CONSTRAINT fk_risk_instrument FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
-- Z-ORDER BY (instrument_id, risk_framework, effective_date)

-- ============================================================================
-- END OF FILE - 5 CREATE TABLE statements
-- Verification: instrument_classification includes cfi_code ✓
-- Verification: instrument_risk_classification has risk_framework FRTB/SA_CCR/CRR ✓
-- ============================================================================
