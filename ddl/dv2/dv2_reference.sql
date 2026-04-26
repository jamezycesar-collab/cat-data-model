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
-- File: dv2_reference.sql
-- Purpose: Data Vault 2.0 Silver - Reference Tables (code lists & taxonomies)
-- Pattern: Reference data in DV2 is typically NOT hashed (no hub/sat split).
-- Two accepted shapes:
-- (a) Pass-through: replicate the 16 Bronze ref_* tables into Silver
-- as "ref_*_dv" with load_date + record_source + hash_diff.
-- (b) Reference Hubs for IDs that participate in joins (e.g., hub_mic,
-- hub_currency) when a Satellite needs to describe them.
-- This file uses shape (a) for the full 16 ref tables PLUS shape (b)
-- for the 3 most cross-referenced (mic, currency, country).
-- ============================================================================

-- ============================================================================
-- PART A - 16 REFERENCE TABLES (DV2 Silver, pass-through with lineage columns)
-- Consumers: DLT expectations, CHECK constraints (via materialized view),
-- Gold aggregations.
-- Retention: permanent (source authority-versioned via effective dates).
-- ============================================================================

-- Ref 1 of 16 - CAT event types (50 FINRA CAT event codes)
CREATE TABLE IF NOT EXISTS ref_cat_event_type_dv (
 cat_event_type_code STRING NOT NULL,
 cat_event_type_name STRING NOT NULL,
 cat_event_type_description STRING,
 category STRING COMMENT 'ORDER | ROUTING | EXECUTION | QUOTE | ALLOCATION | OPTIONS',
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL COMMENT 'FINRA CAT NMS Plan',
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 2 - ISO 10383 MIC codes
CREATE TABLE IF NOT EXISTS ref_mic_dv (
 mic_code STRING NOT NULL,
 mic_name STRING NOT NULL,
 mic_description STRING,
 operating_mic STRING,
 mic_type STRING COMMENT 'OPRT | SGMT',
 country_code STRING,
 city STRING,
 website STRING,
 oprt_sgmt STRING,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL COMMENT 'ISO 10383 (SWIFT)',
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 3 - ISO 3166-1 country codes
CREATE TABLE IF NOT EXISTS ref_country_dv (
 country_code STRING NOT NULL COMMENT 'ISO 3166-1 alpha-2',
 country_name STRING NOT NULL,
 country_description STRING,
 alpha3_code STRING COMMENT 'ISO 3166-1 alpha-3',
 numeric_code STRING,
 region STRING COMMENT 'AMER | EMEA | APAC',
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL COMMENT 'ISO 3166-1',
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 4 - ISO 4217 currencies
CREATE TABLE IF NOT EXISTS ref_currency_dv (
 currency_code STRING NOT NULL COMMENT 'ISO 4217',
 currency_name STRING NOT NULL,
 currency_description STRING,
 numeric_code STRING,
 minor_unit INT,
 settlement_currency_flag BOOLEAN,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL COMMENT 'ISO 4217',
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 5 - ISO 10962 CFI category/group
CREATE TABLE IF NOT EXISTS ref_cfi_category_dv (
 cfi_code STRING NOT NULL COMMENT '6-char ISO 10962',
 cfi_name STRING NOT NULL,
 cfi_description STRING,
 asset_class STRING,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL COMMENT 'ISO 10962',
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 6 - ISO 20275 ELF codes
CREATE TABLE IF NOT EXISTS ref_entity_legal_form_dv (
 elf_code STRING NOT NULL,
 elf_name STRING NOT NULL,
 elf_description STRING,
 country_code STRING,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL COMMENT 'ISO 20275 (GLEIF)',
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 7 - Order type enumeration (FIX 40)
CREATE TABLE IF NOT EXISTS ref_order_type_dv (
 order_type_code STRING NOT NULL,
 order_type_name STRING NOT NULL,
 order_type_description STRING,
 fix_numeric_code STRING,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL COMMENT 'FIX Protocol',
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 8 - Time In Force (FIX 59)
CREATE TABLE IF NOT EXISTS ref_time_in_force_dv (
 tif_code STRING NOT NULL,
 tif_name STRING NOT NULL,
 tif_description STRING,
 fix_numeric_code STRING,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL COMMENT 'FIX Protocol',
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 9 - Handling Instructions (FIX 21)
CREATE TABLE IF NOT EXISTS ref_handling_instruction_dv (
 handling_code STRING NOT NULL,
 handling_name STRING NOT NULL,
 handling_description STRING,
 fix_numeric_code STRING,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL,
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 10 - Party Role (FIX 452)
CREATE TABLE IF NOT EXISTS ref_party_role_dv (
 party_role_code STRING NOT NULL,
 party_role_name STRING NOT NULL,
 party_role_description STRING,
 fix_numeric_code STRING,
 cat_role_equivalent STRING,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL,
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 11 - Instrument Type
CREATE TABLE IF NOT EXISTS ref_instrument_type_dv (
 instrument_type_code STRING NOT NULL,
 instrument_type_name STRING NOT NULL,
 instrument_type_description STRING,
 asset_class STRING,
 fix_security_type STRING COMMENT 'FIX tag 167',
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL,
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 12 - Day Count Conventions (ISDA)
CREATE TABLE IF NOT EXISTS ref_day_count_dv (
 day_count_code STRING NOT NULL,
 day_count_name STRING NOT NULL,
 day_count_description STRING,
 isda_definition STRING,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL COMMENT 'ISDA',
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 13 - Settlement Venue (DTCC, Euroclear, Clearstream, Fed)
CREATE TABLE IF NOT EXISTS ref_settlement_venue_dv (
 settlement_venue_code STRING NOT NULL,
 settlement_venue_name STRING NOT NULL,
 settlement_venue_description STRING,
 country_code STRING,
 venue_type STRING COMMENT 'CSD | ICSD | CLEARING_BANK | CCP',
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL,
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 14 - Regulators
CREATE TABLE IF NOT EXISTS ref_regulator_dv (
 regulator_code STRING NOT NULL,
 regulator_name STRING NOT NULL,
 regulator_description STRING,
 country_code STRING,
 regulator_type STRING COMMENT 'SRO | CENTRAL_BANK | PRUDENTIAL | CONDUCT',
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL,
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 15 - Credit rating scales (Moody's / S&P / Fitch)
CREATE TABLE IF NOT EXISTS ref_credit_rating_scale_dv (
 rating_scale_code STRING NOT NULL,
 rating_scale_name STRING NOT NULL,
 rating_agency STRING NOT NULL COMMENT 'MOODYS | SP | FITCH | DBRS',
 numeric_equivalent INT COMMENT 'Investment grade >= 10',
 investment_grade_flag BOOLEAN,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL,
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Ref 16 - Tax Jurisdictions
CREATE TABLE IF NOT EXISTS ref_tax_jurisdiction_dv (
 tax_jurisdiction_code STRING NOT NULL,
 tax_jurisdiction_name STRING NOT NULL,
 tax_jurisdiction_description STRING,
 country_code STRING,
 fatca_status STRING,
 crs_status STRING,
 effective_start_date DATE NOT NULL,
 effective_end_date DATE NOT NULL,
 is_active BOOLEAN NOT NULL,
 source_authority STRING NOT NULL COMMENT 'OECD / IRS',
 source_version STRING,
 load_date TIMESTAMP NOT NULL,
 hash_diff STRING NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- ============================================================================
-- PART B - REFERENCE HUBS (3 most cross-referenced identifiers)
-- These let us bring Satellites onto code values when the codes themselves
-- have evolving descriptive attrs (e.g., MIC renames, country currency changes).
-- ============================================================================

-- Reference Hub 1 - hub_mic
CREATE TABLE IF NOT EXISTS hub_mic (
 mic_hk STRING NOT NULL COMMENT 'SHA-256(mic_bk || record_source)',
 mic_bk STRING NOT NULL COMMENT 'ISO 10383 MIC code',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
PARTITIONED BY (record_source)
COMMENT 'Reference Hub for MIC - enables SCD2 Sat for MIC renames'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Reference Hub 2 - hub_currency
CREATE TABLE IF NOT EXISTS hub_currency (
 currency_hk STRING NOT NULL,
 currency_bk STRING NOT NULL COMMENT 'ISO 4217',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- Reference Hub 3 - hub_country
CREATE TABLE IF NOT EXISTS hub_country (
 country_hk STRING NOT NULL,
 country_bk STRING NOT NULL COMMENT 'ISO 3166-1 alpha-2',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
) USING DELTA
PARTITIONED BY (record_source)
TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'compression.codec'='zstd', 'subject_area'='data_vault_silver_reference');

-- ============================================================================
-- END OF REFERENCE
-- Summary:
-- 16 Reference Tables (DV Silver form with load_date + hash_diff + record_source)
-- 3 Reference Hubs (hub_mic, hub_currency, hub_country)
-- ============================================================================
