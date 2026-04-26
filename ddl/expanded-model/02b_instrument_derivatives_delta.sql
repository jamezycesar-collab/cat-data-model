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
-- File: 02b_instrument_derivatives_delta.sql
-- Purpose: Enterprise Instrument Model - Derivative + Structured (4 NEW entities, Delta Lake)
-- Net New: derivative_contract, structured_product, instrument_leg, instrument_underlying
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Entity 1 of 4: derivative_contract (NEW)
-- OTC derivative master terms (not exchange-listed)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS derivative_contract (
 derivative_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id - 1:1 OTC derivative subtype',
 contract_type STRING NOT NULL COMMENT 'OTC derivative type: SWAP, FORWARD, SWAPTION, CDS, TRS, IRS, OIS, CCS, FRA, COMMODITY_FORWARD, FX_FORWARD, VARIANCE_SWAP',
 contract_subtype STRING COMMENT 'Further classification: RECEIVER_SWAP, PAYER_SWAP, SINGLE_NAME_CDS, INDEX_CDS, STANDARDIZED_CDS, BESPOKE_CDS',
 trade_date DATE NOT NULL COMMENT 'Trade execution date (when contract was entered into)',
 effective_date DATE NOT NULL COMMENT 'Effective date (when legs start accruing)',
 termination_date DATE COMMENT 'Termination / maturity date (null for open repo / perpetual)',
 notional_amount DECIMAL(28,6) NOT NULL COMMENT 'Notional principal amount (drives payment calc; not a cash exchange amount)',
 notional_currency STRING NOT NULL COMMENT 'ISO 4217 currency of notional_amount',
 day_count STRING COMMENT 'ISDA day count convention (FK to ref_day_count): ACT_360, ACT_365_FIXED, 30_360, ACT_ACT_ISDA',
 payment_frequency STRING COMMENT 'Fixed leg payment cadence: ANNUAL, SEMI_ANNUAL, QUARTERLY, MONTHLY, AT_MATURITY',
 underlying_instrument_id STRING COMMENT 'FK to instrument.instrument_id - underlying (self-reference via instrument); null for rates products, populated for equity/credit/commodity derivatives',
 fixed_rate DECIMAL(12,8) COMMENT 'Fixed leg rate (e.g., 0.04500000 for 4.5%); null for float-float swaps',
 floating_index STRING COMMENT 'Floating leg reference (SOFR, ESTR, SONIA, TONA, EURIBOR, CDOR)',
 spread DECIMAL(10,6) COMMENT 'Spread over floating index (e.g., 0.002500 for +25 bps)',
 clearing_venue STRING COMMENT 'CCP if centrally cleared (LCH, CME, ICE_CLEAR, JSCC, EUREX); null for bilateral',
 isda_master_agreement_id STRING COMMENT 'FK to agreement.agreement_id for governing ISDA Master; enforces netting set membership',
 effective_start_date DATE NOT NULL COMMENT 'SCD2 effective start',
 effective_end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 effective end',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'OTC derivative master terms - swaps, forwards, CDS, TRS; governed by ISDA Master; supports SA-CCR capital, EMIR/CFTC trade reporting, and SEF/MTF routing decisions'
PARTITIONED BY (contract_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'OTC derivative contract master terms (ISDA taxonomy)',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- Inheritance FK: CONSTRAINT fk_deriv_instrument FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
-- Self-reference FK: CONSTRAINT fk_deriv_underlying FOREIGN KEY (underlying_instrument_id) REFERENCES instrument(instrument_id)
-- Agreement FK: CONSTRAINT fk_deriv_isda FOREIGN KEY (isda_master_agreement_id) REFERENCES agreement(agreement_id)
-- Z-ORDER BY (instrument_id, contract_type, trade_date)

-- ----------------------------------------------------------------------------
-- Entity 2 of 4: structured_product (NEW)
-- Multi-component structured notes, CLNs, CDOs
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS structured_product (
 structured_product_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id - 1:1 subtype',
 product_structure STRING NOT NULL COMMENT 'Structure type: PRINCIPAL_PROTECTED, REVERSE_CONVERTIBLE, CLN (Credit-Linked Note), CDO, CLO, AUTOCALLABLE, ELN (Equity-Linked Note), RANGE_ACCRUAL, YIELD_ENHANCEMENT, CAPITAL_PROTECTED',
 issue_price DECIMAL(18,8) COMMENT 'Initial issue price per unit',
 issue_date DATE COMMENT 'Product issue date',
 issuer_party_id STRING NOT NULL COMMENT 'FK to party.party_id - issuing entity',
 arranger_party_id STRING COMMENT 'FK to party.party_id - arranger / structurer (may differ from issuer)',
 payoff_formula STRING COMMENT 'Plain-text or formal (e.g., MathML) description of payoff; critical for pricing reproducibility',
 principal_protection_pct DECIMAL(5,2) COMMENT 'Percentage of principal protected at maturity (0.00 to 100.00)',
 barrier_level DECIMAL(18,8) COMMENT 'Barrier level for knock-in/knock-out features',
 coupon_formula STRING COMMENT 'Coupon formula for structured notes (e.g., max(0, ref_return - strike))',
 strike_level DECIMAL(18,8) COMMENT 'Strike for embedded option',
 observation_frequency STRING COMMENT 'Frequency of observation for autocallables: DAILY, WEEKLY, MONTHLY, QUARTERLY, ANNUAL',
 effective_start_date DATE NOT NULL COMMENT 'SCD2 effective start',
 effective_end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 effective end',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Structured products - principal-protected, reverse convertible, CLN, CDO, autocallable; each links to multiple underlyings via instrument_underlying'
PARTITIONED BY (product_structure)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Structured product dimension with issuer and payoff metadata',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- FK: CONSTRAINT fk_sp_instrument FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
-- FK: CONSTRAINT fk_sp_issuer FOREIGN KEY (issuer_party_id) REFERENCES party(party_id)
-- Z-ORDER BY (instrument_id, issue_date)

-- ----------------------------------------------------------------------------
-- Entity 3 of 4: instrument_leg (NEW)
-- Individual legs of multi-leg instruments (IR swap pay/receive legs, spread legs)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instrument_leg (
 leg_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id - parent multi-leg instrument',
 leg_number INT NOT NULL COMMENT 'Sequential leg number within parent instrument (1, 2, 3,...)',
 leg_type STRING NOT NULL COMMENT 'Leg role: PAY (outgoing cashflow leg), RECEIVE (incoming cashflow leg), BUY, SELL',
 notional_amount DECIMAL(28,6) COMMENT 'Leg-specific notional (may differ across legs, e.g., amortizing swap)',
 rate_type STRING NOT NULL COMMENT 'Interest rate nature: FIXED, FLOATING, INFLATION_LINKED, ZERO_COUPON, EQUITY_LINKED',
 fixed_rate DECIMAL(12,8) COMMENT 'Fixed rate for FIXED legs (e.g., 0.04500000 for 4.5%)',
 floating_index STRING COMMENT 'Floating rate reference: SOFR, ESTR, SONIA, TONA, EURIBOR (FK-enforceable to ref_benchmark_index)',
 spread DECIMAL(10,6) COMMENT 'Spread over floating index in decimal (0.0025 = +25 bps)',
 currency STRING NOT NULL COMMENT 'ISO 4217 leg currency (may differ across legs for cross-currency swaps)',
 day_count STRING COMMENT 'ISDA day count for this leg (can differ per leg)',
 payment_frequency STRING COMMENT 'Leg payment cadence: ANNUAL, SEMI_ANNUAL, QUARTERLY, MONTHLY',
 side STRING COMMENT 'BUY, SELL - for spread/option legs (option combos)',
 ratio DECIMAL(10,6) COMMENT 'Leg ratio for ratio spreads (e.g., 2 for a 1-by-2 call spread)',
 effective_start_date DATE NOT NULL COMMENT 'SCD2 effective start',
 effective_end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 effective end',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Individual legs of multi-leg instruments - swap pay/receive legs, option spread legs, futures spreads; drives leg-level cashflow projection and leg-level risk decomposition'
PARTITIONED BY (leg_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Instrument leg detail for multi-leg products',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- FK: CONSTRAINT fk_leg_instrument FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
-- Z-ORDER BY (instrument_id, leg_number)

-- ----------------------------------------------------------------------------
-- Entity 4 of 4: instrument_underlying (NEW)
-- Links derivatives / structured products to their underlyings (self-ref via instrument)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instrument_underlying (
 underlying_linkage_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id - derivative / structured product (parent)',
 underlying_instrument_id STRING NOT NULL COMMENT 'FK to instrument.instrument_id - underlying (self-reference via instrument)',
 underlying_type STRING NOT NULL COMMENT 'Relationship type: SINGLE (single-name), BASKET (weighted basket of components), INDEX (indexed to published index), CORRELATION, WORST_OF, BEST_OF',
 weight DECIMAL(12,8) COMMENT 'Component weight in basket (0.00000000 to 1.00000000); sum across basket should = 1',
 reference_price DECIMAL(18,8) COMMENT 'Reference / initial price used for strike calc or barrier assessment',
 reference_date DATE COMMENT 'Date reference_price was fixed (strike date or inception)',
 observation_source STRING COMMENT 'Source for observed prices: BLOOMBERG, REFINITIV, EXCHANGE_CLOSE, OFFICIAL_FIXING',
 effective_start_date DATE NOT NULL COMMENT 'SCD2 effective start',
 effective_end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 effective end',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Links derivatives and structured products to their underlyings with weight and reference price; supports SINGLE / BASKET / INDEX and WORST_OF / BEST_OF relationships via self-reference to instrument'
PARTITIONED BY (underlying_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Instrument underlying linkage (basket / index / single-name)',
 'compression.codec' = 'zstd',
 'subject_area' = 'instrument',
 'source_lineage' = 'instrument'
);

-- Self-reference FKs to instrument:
-- CONSTRAINT fk_under_instrument FOREIGN KEY (instrument_id) REFERENCES instrument(instrument_id)
-- CONSTRAINT fk_under_underlying FOREIGN KEY (underlying_instrument_id) REFERENCES instrument(instrument_id)
-- Z-ORDER BY (instrument_id, underlying_instrument_id)

-- ============================================================================
-- END OF FILE - 4 CREATE TABLE statements
-- Verification: derivative_contract has underlying_instrument_id (self-ref) ✓
-- Verification: instrument_leg has instrument_id FK ✓
-- Verification: instrument_underlying self-refs via instrument_id + underlying_instrument_id ✓
-- ============================================================================
