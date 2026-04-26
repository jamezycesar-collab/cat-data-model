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
-- File: 01a_party_core_delta.sql
-- Purpose: Enterprise Party Model - Core Entities (5 entities, Delta Lake)
-- BSBS-G-SIB, ownership %, role capacity, desk linkage)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Entity 1 of 5: party (supertype) - ENHANCED
-- ultimate_parent_party_id, domicile_country, tax_residency
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party (
 party_id STRING NOT NULL COMMENT 'UUID v4 - Party supertype primary key',
 party_type STRING NOT NULL COMMENT 'Discriminator enum: LEGAL_ENTITY, NATURAL_PERSON',
 party_status STRING NOT NULL COMMENT 'Lifecycle status: ACTIVE, INACTIVE, SUSPENDED, DECEASED',
 ultimate_parent_party_id STRING COMMENT 'NEW - FK to party.party_id identifying ultimate parent in ownership hierarchy (self-reference); NULL for standalone UBO; resolved by beneficial_ownership recursive CTE',
 domicile_country STRING COMMENT 'NEW - ISO 3166-1 alpha-2 country of legal domicile (regulatory residence); used for Basel BCBS 239 reporting and cross-border gating',
 tax_residency STRING COMMENT 'NEW - ISO 3166-1 alpha-2 primary tax residency country (FATCA/CRS residency); may differ from domicile and citizenship',
 created_date DATE NOT NULL COMMENT 'Record creation date in source system',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date - when this version became active',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date - 9999-12-31 for current version',
 ingestion_timestamp TIMESTAMP NOT NULL COMMENT 'Platform ingestion timestamp (UTC) - standardized from ingestion_ts',
 source_system STRING NOT NULL COMMENT 'Originating system identifier (e.g., CRM-PROD-01, ADM-FEE-01)',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit: record creation timestamp',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit: record update timestamp'
)
USING DELTA
COMMENT 'Enterprise party supertype (ISO 20022 Party) - legal entities and natural persons with enterprise enrichments: ultimate parent resolution, domicile country, and tax residency for cross-border and UBO reporting'
PARTITIONED BY (created_date)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'delta.logRetentionDuration' = 'interval 180 days',
 'delta.deletedFileRetentionDuration' = 'interval 30 days',
 'delta.minReaderVersion' = '3',
 'delta.minWriterVersion' = '7',
 'classification' = 'internal',
 'pii_columns' = 'none',
 'description' = 'Enterprise party supertype implementing ISO 20022 Party with UBO chain, domicile, and tax residency',
 'compression.codec' = 'zstd',
 'owner' = 'data-platform-team',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Indexing recommendations:
-- Z-ORDER BY (party_id, effective_date) - temporal lookups
-- LIQUID CLUSTERING BY (party_type, party_status, domicile_country) - equality & range filters

-- ----------------------------------------------------------------------------
-- Entity 2 of 5: legal_entity (subtype) - ENHANCED
-- fiscal_year_end, gsib_indicator, entity_legal_form (ISO 20275)
-- (incorporation_date already present in source; retained for completeness)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS legal_entity (
 party_id STRING NOT NULL COMMENT 'FK to party.party_id - PK of 1:1 subtype (inheritance from supertype)',
 lei STRING COMMENT 'Legal Entity Identifier (ISO 17442) - 20-char globally unique code issued by LOU',
 ein STRING COMMENT 'Employer Identification Number (US IRS) - 9 digits format XX-XXXXXXX',
 entity_name STRING NOT NULL COMMENT 'Legal entity name as registered at incorporation authority (PII - mask non-prod)',
 dba_name STRING COMMENT 'Doing-business-as / trading name (PII - mask non-prod)',
 entity_type STRING NOT NULL COMMENT 'High-level classification: CORPORATION, PARTNERSHIP, LLC, LP, TRUST, FUND, GOVERNMENT, SPV',
 entity_legal_form STRING COMMENT 'NEW - ISO 20275 Entity Legal Form (ELF) code; granular legal form (~3000 codes covering all jurisdictions, e.g., XTIQ=US Corporation, 8888=Other)',
 incorporation_country STRING COMMENT 'ISO 3166-1 alpha-2 country code of incorporation',
 incorporation_state STRING COMMENT 'State/province code of incorporation (Delaware, Cayman, etc.)',
 incorporation_date DATE COMMENT 'Date of incorporation / founding / registration',
 fiscal_year_end STRING COMMENT 'NEW - Fiscal year-end in MM-DD format (e.g., 12-31 for calendar year, 03-31 for Japanese FY); required for Pillar 3 and consolidated reporting',
 gsib_indicator BOOLEAN COMMENT 'NEW - Global Systemically Important Bank flag (FSB designation); drives Basel III G-SIB surcharge, TLAC, stress-test gating',
 regulatory_status STRING COMMENT 'REGISTERED_BD, REGISTERED_IA, EXEMPT, FOREIGN, UNREGISTERED',
 sic_code STRING COMMENT 'Standard Industrial Classification code (4 digits)',
 naics_code STRING COMMENT 'North American Industry Classification System code (6 digits)',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Legal entity subtype (ISO 20022 LegalEntity) - enhanced with ISO 20275 entity_legal_form, fiscal_year_end for Pillar 3, and gsib_indicator for G-SIB surcharge identification'
PARTITIONED BY (incorporation_country)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'pii_columns' = 'entity_name,dba_name',
 'description' = 'Legal entity dimension with ISO 20275 ELF and G-SIB flag',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (lei, ein) - regulatory lookup hotspots
-- PII masking: apply external-view redaction for entity_name, dba_name in non-prod

-- ----------------------------------------------------------------------------
-- Entity 3 of 5: natural_person (subtype) - ENHANCED
-- nationality, mifid_classification
-- (tax_id_type already present in source; retained for completeness)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS natural_person (
 party_id STRING NOT NULL COMMENT 'FK to party.party_id - PK of 1:1 subtype',
 first_name STRING COMMENT 'PII - given name (mask non-prod via external view)',
 middle_name STRING COMMENT 'PII - middle name/initial (mask non-prod)',
 last_name STRING COMMENT 'PII - family name/surname (mask non-prod via external view)',
 name_suffix STRING COMMENT 'Generational/honorific suffix: Jr, Sr, II, III',
 date_of_birth DATE COMMENT 'PII - DOB; use YYYY-01-15 placeholder in non-prod',
 nationality STRING COMMENT 'NEW - ISO 3166-1 alpha-2 nationality code; MiFID II National ID construction (e.g., concatenated with passport number per MiFIR RTS 22)',
 citizenship_country STRING COMMENT 'ISO 3166-1 alpha-2 country of citizenship (may be multiple; see party_identifier for additional)',
 tax_residency_country STRING COMMENT 'ISO 3166-1 alpha-2 primary tax residency (CRS/FATCA); may differ from citizenship',
 tax_id_type STRING COMMENT 'Tax identifier scheme: SSN (US), ITIN (US), FOREIGN_TIN, EIN',
 tax_id_encrypted STRING COMMENT 'Encrypted tax ID (AES-256-GCM); decrypt only in secure context',
 mifid_classification STRING COMMENT 'NEW - MiFID II client classification: RETAIL, PROFESSIONAL_ELECTIVE, PROFESSIONAL_PER_SE, ELIGIBLE_COUNTERPARTY; drives suitability/appropriateness obligations',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Natural person subtype - individuals with trading, beneficial ownership, or signatory roles; enhanced with nationality (for MiFIR National ID), MiFID II classification, and encrypted tax identifiers'
PARTITIONED BY (citizenship_country)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'pii_columns' = 'first_name,middle_name,last_name,date_of_birth,tax_id_encrypted,nationality',
 'description' = 'Natural person dimension with MiFID II classification and MiFIR National ID inputs',
 'compression.codec' = 'zstd',
 'encryption' = 'AES-256-GCM for tax_id_encrypted',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- PII CRITICAL: first_name, last_name, date_of_birth, tax_id_encrypted, nationality
-- Create masked view natural_person_masked for analytics and non-prod consumption

-- ----------------------------------------------------------------------------
-- Entity 4 of 5: party_role (enriched) - ENHANCED
-- role_capacity (PRINCIPAL / AGENCY / RISKLESS_PRINCIPAL), desk_id
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_role (
 party_role_id STRING NOT NULL COMMENT 'UUID v4 - Party role primary key',
 party_id STRING NOT NULL COMMENT 'FK to party.party_id',
 role_type STRING NOT NULL COMMENT 'Role taxonomy (FK to ref_party_role): ACCOUNT_HOLDER, COUNTERPARTY, BROKER_DEALER, CUSTODIAN, BENEFICIAL_OWNER, AUTHORIZED_TRADER, MARKET_MAKER, EXCHANGE_OPERATOR, CLEARING_MEMBER, PRIME_BROKER, INTRODUCING_BROKER, CORRESPONDENT, SETTLEMENT_AGENT, ISSUER, LENDER, BORROWER, CCP',
 role_capacity STRING COMMENT 'NEW - Trading capacity for this role (FIX tag 528): PRINCIPAL (principal trading), AGENCY (pure agent), RISKLESS_PRINCIPAL (facilitation with offsetting fill)',
 role_status STRING NOT NULL COMMENT 'ACTIVE, INACTIVE, SUSPENDED, PENDING, REVOKED',
 role_context STRING COMMENT 'Additional context - e.g., desk name, division, trading floor identifier',
 desk_id STRING COMMENT 'NEW - FK to desk.desk_id; links role to trading desk / business unit for MAR/STOR and info-barrier resolution',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date - when role assignment became active',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Party role assignments (ISO 20022 PartyRole) - polymorphic role-to-party mapping enhanced with FIX tag 528 role_capacity and desk linkage for MAR surveillance and information-barrier enforcement'
PARTITIONED BY (role_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Party role assignments with capacity and desk linkage',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (party_id, role_type) - common access pattern
-- LIQUID CLUSTERING BY (role_status, desk_id)

-- ----------------------------------------------------------------------------
-- Entity 5 of 5: party_relationship (enriched) - ENHANCED
-- control_type, relationship_category
-- (ownership_percentage already present in source; retained)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_relationship (
 relationship_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 parent_party_role_id STRING NOT NULL COMMENT 'FK to party_role.party_role_id - parent/superior side (e.g., parent company, controlling party)',
 child_party_role_id STRING NOT NULL COMMENT 'FK to party_role.party_role_id - child/subordinate side (e.g., subsidiary, controlled party)',
 relationship_type STRING NOT NULL COMMENT 'Relationship taxonomy: PARENT_SUBSIDIARY, CONTROL_PERSON, AUTHORIZED_TRADER_OF, BENEFICIAL_OWNER_OF, PRIME_BROKER_CLIENT, CLEARING_RELATIONSHIP, CUSTODY_RELATIONSHIP, CORRESPONDENT_RELATIONSHIP, GIVE_UP, INTRODUCING_BROKER_RELATIONSHIP',
 relationship_category STRING COMMENT 'NEW - High-level grouping: OWNERSHIP, CONTROL, AUTHORIZATION, COMMERCIAL, REGULATORY; enables filtering for Form 13F/13G/13D and Rule 144A compliance',
 control_type STRING COMMENT 'NEW - For CONTROL-category relationships: VOTING_CONTROL, ECONOMIC_CONTROL, BOARD_CONTROL, CONTRACTUAL_CONTROL, JOINT_VENTURE; drives 10-percent-owner and control-person disclosure',
 relationship_status STRING NOT NULL COMMENT 'ACTIVE, TERMINATED, SUSPENDED, PENDING',
 ownership_percentage DECIMAL(7,4) COMMENT 'Ownership percentage for BENEFICIAL_OWNER_OF (0.0000 to 100.0000); DECIMAL(7,4) precision for sub-basis-point reporting',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Party relationships - hierarchies and associations enriched with relationship_category (OWNERSHIP/CONTROL/AUTHORIZATION) and control_type (VOTING/ECONOMIC/BOARD/CONTRACTUAL) for 13F/13D/13G and control-person reporting'
PARTITIONED BY (relationship_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Party relationships with control typology',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (parent_party_role_id, child_party_role_id) - hierarchy traversal
-- LIQUID CLUSTERING BY (relationship_category, relationship_status)

-- ============================================================================
-- END OF FILE - 5 CREATE TABLE statements
-- Verification: grep -c "CREATE TABLE" should return 5
-- ============================================================================
-- ============================================================================
-- File: 01b_party_attributes_delta.sql
-- Purpose: Enterprise Party Model - Attribute Entities (4 entities, Delta Lake)
-- SIC/NAICS/GICS/FATF on classification; account_purpose/margin_type/
-- custodian_party_id on account.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Entity 1 of 4: party_identifier (enriched) - ENHANCED
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_identifier (
 identifier_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 party_id STRING NOT NULL COMMENT 'FK to party.party_id',
 id_type STRING NOT NULL COMMENT 'Identifier scheme: LEI, EIN, SSN, CRD, IMID, MPID, FDID, LTID, SWIFT_BIC, DTCC_PARTICIPANT, NFA_ID, IARD, SEC_FILE_NUMBER, CFTC_ID, PASSPORT, NATIONAL_ID',
 id_value STRING NOT NULL COMMENT 'Identifier value - encrypted for SSN/TIN/passport; plaintext for LEI/EIN',
 id_issuer STRING COMMENT 'NEW - Issuing organization / LOU / government agency (e.g., GLEIF-accredited LOU, IRS, UK Passport Office)',
 id_country STRING COMMENT 'NEW - ISO 3166-1 alpha-2 country of issuance (jurisdiction of the identifier)',
 issuing_authority STRING COMMENT 'Authority that issued the identifier (e.g., SEC, FINRA, DTCC, SWIFT); may equal id_issuer for regulatory IDs',
 verification_date DATE COMMENT 'NEW - Date the identifier was last verified/refreshed (KYC refresh cycle driver; null = not verified)',
 is_primary BOOLEAN DEFAULT false COMMENT 'Primary identifier flag for this id_type within the party (exactly one TRUE per party+id_type)',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Party identifiers with issuer/country provenance and KYC verification_date; supports regulatory cross-reference (LEI, CRD, MPID, IMID, FDID/LTID for CAT)'
PARTITIONED BY (id_type)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Party identifier catalog with issuer and verification provenance',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (party_id, id_type, id_value) - KYC/regulatory lookup
-- LIQUID CLUSTERING BY (id_type, id_country)

-- ----------------------------------------------------------------------------
-- Entity 2 of 4: party_address (enriched) - ENHANCED
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_address (
 address_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 party_id STRING NOT NULL COMMENT 'FK to party.party_id',
 address_type STRING NOT NULL COMMENT 'Physical/logical address type: LEGAL, MAILING, TRADING_DESK, REGISTERED_AGENT, PRINCIPAL_OFFICE, BRANCH, CONTACT',
 address_purpose STRING COMMENT 'NEW - Usage purpose: REGISTERED (jurisdictional/legal record), OPERATIONAL (daily operations), MAILING (statements/correspondence), LEGAL (service of process); an address_type may have multiple purposes over time',
 address_line_1 STRING COMMENT 'PII - street address (mask non-prod)',
 address_line_2 STRING COMMENT 'PII - suite/floor/unit',
 city STRING COMMENT 'City/municipality',
 state_province STRING COMMENT 'State/province/region abbreviation',
 postal_code STRING COMMENT 'ZIP / postal code',
 country_code STRING NOT NULL COMMENT 'ISO 3166-1 alpha-2 (e.g., US, GB, JP) - FK-enforceable against ref_country',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Party addresses with address_purpose distinguishing REGISTERED / OPERATIONAL / MAILING / LEGAL use; PII-sensitive with non-prod masking obligations'
PARTITIONED BY (country_code)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'pii_columns' = 'address_line_1,address_line_2,city,state_province,postal_code',
 'description' = 'Address dimension (PII-sensitive) with purpose tagging',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (party_id, address_type, address_purpose)
-- PII: address fields require non-prod masking via external view

-- ----------------------------------------------------------------------------
-- Entity 3 of 4: party_classification (enriched) - ENHANCED
-- sic_code, naics_code, gics_sector, fatf_risk_rating
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_classification (
 classification_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 party_id STRING NOT NULL COMMENT 'FK to party.party_id',
 classification_scheme STRING NOT NULL COMMENT 'Scheme: MIFID_CLIENT (CAT), CFTC_CLASSIFICATION, SEC_13F, FATCA_STATUS, CRS_STATUS, INSTITUTIONAL_TYPE, CAT_CUSTOMER_TYPE, KYC_RATING, SANCTIONS_SCREENING',
 classification_value STRING NOT NULL COMMENT 'Scheme-specific value (e.g., PROFESSIONAL, RETAIL, INSTITUTIONAL, FFI, PASSIVE_NFFE)',
 sic_code STRING COMMENT 'NEW - Standard Industrial Classification code (4 digits); enables sector-based surveillance and sanctions screening',
 naics_code STRING COMMENT 'NEW - North American Industry Classification System code (6 digits); NA replacement for SIC',
 gics_sector STRING COMMENT 'NEW - MSCI/S&P Global Industry Classification Standard sector (e.g., 10=Energy, 25=Consumer Discretionary, 40=Financials)',
 fatf_risk_rating STRING COMMENT 'NEW - FATF AML/CTF risk classification: HIGH, MEDIUM, LOW; drives EDD (Enhanced Due Diligence) gating and transaction monitoring thresholds',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Party classifications - regulatory (MiFID/SEC/CFTC/FATCA/CRS) and commercial (SIC/NAICS/GICS) classifications with FATF AML risk tier'
PARTITIONED BY (classification_scheme)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'description' = 'Classification mappings with SIC/NAICS/GICS and FATF risk tiering',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (party_id, classification_scheme)
-- LIQUID CLUSTERING BY (fatf_risk_rating, gics_sector)

-- ----------------------------------------------------------------------------
-- Entity 4 of 4: account (enriched) - ENHANCED
-- account_purpose, base_currency (already present), margin_type, custodian_party_id
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS account (
 account_id STRING NOT NULL COMMENT 'UUID v4 - Primary key',
 account_holder_role_id STRING NOT NULL COMMENT 'FK to party_role.party_role_id (role_type = ACCOUNT_HOLDER)',
 account_number STRING NOT NULL COMMENT 'Firm-assigned account number; unique within firm',
 account_type STRING NOT NULL COMMENT 'Account category: CUSTOMER_CASH, CUSTOMER_MARGIN, PROPRIETARY, MARKET_MAKING, ERROR_ACCOUNT, AVERAGE_PRICE, ALLOCATION, OMNIBUS, GIVE_UP, PRIME_BROKERAGE, CUSTODY',
 account_subtype STRING COMMENT 'Further classification: IRA, 401K, JOINT, UGMA, INDIVIDUAL, EDGE, SMA, UMA, LLC, PARTNERSHIP',
 account_purpose STRING COMMENT 'NEW - Intended purpose: TRADING, INVESTMENT, HEDGING, SEGREGATED, FIRM_CAPITAL, CUSTODY_ONLY, CLEARING; drives regulatory segregation and reg-capital treatment',
 firm_designated_id STRING COMMENT 'CAIS FDID - Firm Designated ID reported to CAT for this account (CAT Rule 613)',
 account_name STRING COMMENT 'PII - account name/nickname; mask non-prod',
 account_status STRING NOT NULL COMMENT 'Lifecycle: OPEN, CLOSED, FROZEN, RESTRICTED, DORMANT, LIQUIDATING',
 opening_date DATE COMMENT 'Date account was opened',
 closing_date DATE COMMENT 'Date account was closed (null if still open)',
 base_currency STRING DEFAULT 'USD' COMMENT 'ISO 4217 base currency of account (NEW in SKILL but DEFAULT retained)',
 margin_eligible_flag BOOLEAN DEFAULT false COMMENT 'Whether account can trade on margin (renamed from margin_eligible boolean convention)',
 margin_type STRING COMMENT 'NEW - Margin regime: REG_T (Fed Reg T, 50%), PORTFOLIO_MARGIN (SEC-approved PM, risk-based), PROPRIETARY (firm margin), CASH (no margin), CUSTOMER_PORTFOLIO_MARGIN',
 options_level INT DEFAULT 0 COMMENT 'Options approval: 0=none, 1=covered calls, 2=spreads, 3=puts, 4=uncovered writes, 5=complex spreads',
 custodian_party_id STRING COMMENT 'NEW - FK to party.party_id identifying custodian for this account; NULL for self-custody',
 effective_date DATE NOT NULL COMMENT 'SCD2 effective date',
 end_date DATE NOT NULL DEFAULT DATE'9999-12-31' COMMENT 'SCD2 end date',
 _created_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit',
 _updated_at TIMESTAMP GENERATED ALWAYS AS (current_timestamp) COMMENT 'CDF audit'
)
USING DELTA
COMMENT 'Customer and proprietary accounts - enriched with account_purpose, margin_type (Reg T / Portfolio Margin), and custodian_party_id for tri-party custody linkage'
PARTITIONED BY (account_status)
TBLPROPERTIES (
 'delta.autoOptimize.optimizeWrite' = 'true',
 'delta.autoOptimize.autoCompact' = 'true',
 'delta.columnMapping.mode' = 'name',
 'delta.enableChangeDataFeed' = 'true',
 'pii_columns' = 'account_name,account_number',
 'description' = 'Account dimension (customer, proprietary, firm) with margin regime and custodian linkage',
 'compression.codec' = 'zstd',
 'subject_area' = 'party',
 'source_lineage' = 'party'
);

-- Z-ORDER BY (account_holder_role_id, account_number)
-- LIQUID CLUSTERING BY (account_type, account_status, margin_type)
-- PII: account_name and account_number should be masked in non-prod

-- ============================================================================
-- END OF FILE - 4 CREATE TABLE statements
-- Verification: grep -c "CREATE TABLE" should return 4
-- ============================================================================
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
