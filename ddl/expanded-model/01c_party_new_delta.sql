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
-- File: 01c_party_new_delta.sql
-- Purpose: Enterprise Party Model - NEW Party Entities (5 entities, Delta Lake)
-- Net New: party_contact, party_regulatory_status, party_credit_rating,
-- beneficial_ownership (UBO chain - recursive CTE ready), desk
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Entity 1 of 5: party_contact (NEW)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_contact (
 contact_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 party_id STRING NOT NULL COMMENT 'FK to party.party_id',
 contact_type STRING NOT NULL COMMENT 'Contact channel: PHONE_OFFICE, PHONE_MOBILE, FAX, EMAIL_BUSINESS, EMAIL_COMPLIANCE, SWIFT_BIC, TELEX',
 contact_value STRING NOT NULL COMMENT 'Channel-specific value (E.164 phone, RFC 5322 email, 8/11-char SWIFT BIC); PII - mask non-prod',
 is_primary BOOLEAN DEFAULT false COMMENT 'Primary contact flag for this contact_type within the party',
 contact_purpose STRING COMMENT 'Business purpose: TRADING, SETTLEMENT, OPERATIONS, COMPLIANCE, LEGAL, GENERAL',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date (when this contact became active)',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Party contact details - phone, email, fax, SWIFT BIC - per contact_type and purpose; PII-sensitive; supports trading/settlement/compliance channel routing'
PARTITIONED BY (contact_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'pii_columns' = 'contact_value',
 'description' = 'Party contact methods with purpose and primary flag',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (party_id, contact_type)

-- ----------------------------------------------------------------------------
-- Entity 2 of 5: party_regulatory_status (NEW)
-- Covers dictionary phantom W-01 (regulatory_id_xref replacement)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_regulatory_status (
 regulatory_status_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 party_id STRING NOT NULL COMMENT 'FK to party.party_id',
 regulator_code STRING NOT NULL COMMENT 'Regulator identifier (FK to ref_regulator): SEC, FINRA, FCA, BaFin, ESMA, MAS, JFSA, CFTC, ASIC, OSC',
 jurisdiction STRING COMMENT 'ISO 3166-1 alpha-2 jurisdiction of registration',
 registration_number STRING NOT NULL COMMENT 'Regulator-assigned registration number (e.g., SEC file number, FINRA CRD, FCA FRN)',
 registration_type STRING COMMENT 'License class: BROKER_DEALER, INVESTMENT_ADVISER, FUTURES_COMMISSION_MERCHANT, MARKET_MAKER, ATS, CLEARING_MEMBER, MTF, OTF, SWAP_DEALER, MSBSP',
 registration_status STRING NOT NULL COMMENT 'Status: ACTIVE, PENDING, SUSPENDED, REVOKED, WITHDRAWN, LAPSED',
 registration_date DATE COMMENT 'Date of original registration / license grant',
 expiry_date DATE COMMENT 'Date license expires (null = indefinite)',
 last_review_date DATE COMMENT 'Date of most recent regulatory review / examination',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date of this status version',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Party regulatory registrations per jurisdiction and regulator - supersedes W-01 phantom regulatory_id_xref; drives cross-border gating and license-validity checks'
PARTITIONED BY (regulator_code)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Regulatory registration status per regulator and jurisdiction',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (party_id, regulator_code, registration_status)

-- ----------------------------------------------------------------------------
-- Entity 3 of 5: party_credit_rating (NEW)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_credit_rating (
 rating_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 party_id STRING NOT NULL COMMENT 'FK to party.party_id',
 agency STRING NOT NULL COMMENT 'Rating agency: S&P, MOODYS, FITCH, DBRS, KROLL, AM_BEST, JCR, R&I (FK-enforceable against ref_credit_rating_scale.agency)',
 rating_type STRING NOT NULL COMMENT 'Rating category: LT_ISSUER_CREDIT, ST_ISSUER_CREDIT, SENIOR_UNSECURED, SUBORDINATED, COUNTERPARTY, FINANCIAL_STRENGTH',
 rating STRING NOT NULL COMMENT 'Agency-native rating notation (FK to ref_credit_rating_scale): e.g., AAA, AA+, BBB-, Baa2, Ba1',
 rating_numeric INT COMMENT 'Normalized numeric rank for cross-agency comparison (1=AAA..22=D)',
 outlook STRING COMMENT 'Forward outlook: STABLE, POSITIVE, NEGATIVE, DEVELOPING',
 watch_status STRING COMMENT 'Credit watch: NONE, POSITIVE, NEGATIVE, EVOLVING',
 rating_date DATE NOT NULL COMMENT 'Date rating was issued or last affirmed',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date (when rating took effect)',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date (when rating was withdrawn / superseded)',
 source_report_url STRING COMMENT 'URL or document ID of the rating report for evidence linkage',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Party credit ratings across NRSROs (S&P/Moody''s/Fitch/DBRS/Kroll/AM Best/JCR/R&I) - supports counterparty limits, capital calc, and credit-event surveillance'
PARTITIONED BY (agency)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Counterparty credit ratings per agency with normalized numeric rank',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (party_id, agency, rating_date)

-- ----------------------------------------------------------------------------
-- Entity 4 of 5: beneficial_ownership (NEW)
-- section 1A Beneficial Ownership Resolution Pattern - recursive CTE ready
-- Self-referencing via owner_party_id and owned_party_id (both FK to party)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS beneficial_ownership (
 ownership_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 owner_party_id STRING NOT NULL COMMENT 'FK to party.party_id - OWNER side (controlling party); self-referencing with owned_party_id',
 owned_party_id STRING NOT NULL COMMENT 'FK to party.party_id - OWNED side (controlled party); self-referencing with owner_party_id',
 ownership_percentage DECIMAL(7,4) NOT NULL COMMENT 'Ownership percentage (0.0000 to 100.0000); used in recursive CTE multiplication for UBO chain',
 control_flag BOOLEAN NOT NULL DEFAULT false COMMENT 'TRUE if owner has de-facto control irrespective of percentage (board/voting/contractual)',
 chain_level INT COMMENT 'Depth in ownership chain (1 = direct, 2 = through one intermediary,...); POPULATED BY RECURSIVE CTE - raw records may leave NULL',
 ultimate_beneficial_owner_flag BOOLEAN NOT NULL DEFAULT false COMMENT 'TRUE when this row represents the terminal UBO (owner has no further owners); resolved by section 1A recursive CTE',
 beneficial_ownership_source STRING COMMENT 'Evidence source: 10-PERCENT-RULE, FORM_13D, FORM_13G, CUSTOMER_KYC, SEC_REGISTRATION, COMPANIES_HOUSE, CORPORATE_REGISTRY',
 declaration_date DATE COMMENT 'Date the ownership was disclosed / declared',
 verification_date DATE COMMENT 'Date of last ownership verification (KYC refresh)',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Beneficial ownership chain - self-referencing table designed for recursive CTE traversal per section 1A Beneficial Ownership Resolution Pattern; supports 10-percent rule, 13D/13G, and 5th AMLD UBO disclosure'
PARTITIONED BY (ultimate_beneficial_owner_flag)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'UBO chain - recursive CTE traversal ready (chain_level populated via WITH RECURSIVE)',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'traversal_pattern' = 'RECURSIVE_CTE',
 'source_lineage' = 'party'
);

-- SELF-REFERENCING FKs (enforced via Unity Catalog constraints in):
-- CONSTRAINT fk_bo_owner FOREIGN KEY (owner_party_id) REFERENCES party(party_id)
-- CONSTRAINT fk_bo_owned FOREIGN KEY (owned_party_id) REFERENCES party(party_id)
-- Z-ORDER BY (owned_party_id, chain_level, effective_date) - optimized for UBO traversal

-- ----------------------------------------------------------------------------
-- Entity 5 of 5: desk (NEW)
-- Trading desk / business unit hierarchy. Covers dictionary phantom "desk" gap.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS desk (
 desk_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 desk_name STRING NOT NULL COMMENT 'Desk / business unit name (e.g., US EQUITIES CASH, FX SPOT, RATES DERIVATIVES)',
 desk_code STRING COMMENT 'Short code / ticker for the desk (uppercase, unique within firm)',
 desk_type STRING NOT NULL COMMENT 'Desk category: SALES, TRADING, RESEARCH, OPERATIONS, COMPLIANCE, MIDDLE_OFFICE, PRIME_SERVICES, MARKET_MAKING',
 asset_class STRING COMMENT 'Primary asset class (EQUITY, FIXED_INCOME, FX, COMMODITY, DERIVATIVES, MULTI_ASSET, CRYPTO)',
 parent_desk_id STRING COMMENT 'FK to desk.desk_id - parent desk for hierarchy; NULL = top-level division',
 legal_entity_id STRING NOT NULL COMMENT 'FK to party.party_id (for the legal_entity subtype) - booking entity for the desk',
 cost_center STRING COMMENT 'Finance cost center code (for P&L allocation)',
 location STRING COMMENT 'Primary office location (e.g., NYC, LDN, HKG, TYO)',
 information_barrier_id STRING COMMENT 'Information-barrier / Chinese-wall group ID - desks sharing an ID may freely communicate; across IDs require escalation',
 desk_status STRING NOT NULL COMMENT 'Status: ACTIVE, DISBANDED, MERGED, REORGANIZED',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Trading desk / business unit hierarchy (self-referencing via parent_desk_id); drives desk attribution, information-barrier enforcement, P&L allocation, and MAR/STOR surveillance partitioning'
PARTITIONED BY (desk_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Desk hierarchy with info-barrier grouping and booking-entity linkage',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (legal_entity_id, desk_type)
-- parent_desk_id self-referencing FK enforced in 

-- ============================================================================
-- END OF FILE - 5 CREATE TABLE statements
-- Verification: grep -c "CREATE TABLE" should return 5
-- beneficial_ownership has chain_level column for recursive traversal: CHECK
-- beneficial_ownership self-refs owner_party_id + owned_party_id to party: CHECK
-- ============================================================================
