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

-- Party model core entities (Delta Lake): party supertype plus the four
-- direct subtypes. Handles ownership hierarchy via self-reference and
-- carries the identifiers we need for BSBS G-SIB, MiFID II, and ISO 20275.

-- party (supertype)
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
