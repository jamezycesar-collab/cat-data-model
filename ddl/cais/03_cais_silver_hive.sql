-- CAIS Silver-layer DDL (Apache Hive).
-- Hive mirror of 01_cais_silver_delta.sql.
-- Notes: Hive does not enforce PRIMARY KEY / FOREIGN KEY / CHECK constraints;
-- they are documented as comments only. Enforcement is upstream in DLT.


CREATE TABLE IF NOT EXISTS silver.hub_cais_fdid (
    cais_fdid_hk                STRING COMMENT 'PK',
    cais_fdid_bk_firmDesignatedID  STRING,
    cais_fdid_bk_catReporterCRD    BIGINT,
    load_dts                    TIMESTAMP,
    record_source               STRING,
    dv2_source_hk               STRING
)
STORED AS PARQUET
TBLPROPERTIES ('comment' = 'DV2 Hub: CAIS FDID records (Hive mirror).');


CREATE TABLE IF NOT EXISTS silver.hub_cais_customer (
    cais_customer_hk            STRING,
    cais_customer_bk_recordID   STRING,
    cais_customer_bk_catReporterCRD BIGINT,
    customer_type               STRING COMMENT 'CHECK in (NATURAL_PERSON, LEGAL_ENTITY)',
    load_dts                    TIMESTAMP,
    record_source               STRING,
    dv2_source_hk               STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.hub_cais_large_trader (
    cais_large_trader_hk        STRING,
    large_trader_id             STRING,
    large_trader_type           STRING COMMENT 'CHECK in (LTID, ULTID)',
    load_dts                    TIMESTAMP,
    record_source               STRING,
    dv2_source_hk               STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.link_cais_fdid_customer (
    cais_fdid_customer_hk       STRING,
    cais_fdid_hk                STRING,
    cais_customer_hk            STRING,
    customer_role               STRING COMMENT 'CHECK in (TRDHOLDER, AUTH3RD, AUTHREP)',
    role_start_date             DATE,
    role_end_date               DATE,
    role_end_reason             STRING,
    has_discretion              BOOLEAN,
    load_dts                    TIMESTAMP,
    record_source               STRING,
    dv2_source_hk               STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.link_cais_fdid_large_trader (
    cais_fdid_lt_hk             STRING,
    cais_fdid_hk                STRING,
    cais_large_trader_hk        STRING,
    ltid_effective_date         DATE,
    ltid_end_date               DATE,
    ltid_end_reason             STRING,
    load_dts                    TIMESTAMP,
    record_source               STRING,
    dv2_source_hk               STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.link_cais_submission (
    cais_submission_hk          STRING,
    submission_filename         STRING,
    cat_submitter_id            BIGINT,
    cat_reporter_crd            BIGINT,
    file_generation_date        DATE,
    file_number                 INT,
    submission_action           STRING COMMENT 'CHECK in (NEW, UPDATE, REFRESH, FIRM_CORRECTION, REPAIR, REPLACEMENT, MASS_TRANSFER)',
    file_type                   STRING COMMENT 'CHECK in (CAIS_DATA_FILE, TRANSFORMED_IDENTIFIERS_FILE)',
    paired_filename             STRING,
    load_dts                    TIMESTAMP,
    record_source               STRING,
    dv2_source_hk               STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.sat_cais_fdid_state (
    cais_fdid_hk                STRING,
    load_dts                    TIMESTAMP,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING,
    fdid_type                   STRING,
    account_types_array         ARRAY<STRING>,
    account_name_extended       STRING,
    dvp_custodian_ids           ARRAY<STRING>,
    clearing_broker_ids         ARRAY<STRING>,
    branch_office_crd           BIGINT,
    registered_rep_crds         ARRAY<BIGINT>,
    fdid_date                   DATE,
    fdid_end_date               DATE,
    fdid_end_reason             STRING,
    replaced_by_fdid            STRING,
    prior_cat_reporter_crd      BIGINT,
    prior_cat_reporter_fdid     STRING,
    correspondent_crd           BIGINT,
    record_source               STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.sat_cais_customer_state (
    cais_customer_hk            STRING,
    load_dts                    TIMESTAMP,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING,
    year_of_birth               INT,
    has_discretion              BOOLEAN,
    ein                         STRING,
    legal_entity_type           STRING,
    nationality_code            STRING,
    is_authorized_trader        BOOLEAN,
    record_source               STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.sat_cais_fdid_address (
    cais_fdid_hk                STRING,
    addr_type                   STRING COMMENT 'CHECK in (ADDRESS1..ADDRESS4)',
    load_dts                    TIMESTAMP,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING,
    addr_line_1                 STRING,
    addr_line_2                 STRING,
    addr_line_3                 STRING,
    addr_line_4                 STRING,
    city                        STRING,
    region_code                 STRING,
    country_code                STRING,
    postal_code                 STRING,
    record_source               STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.sat_cais_customer_address (
    cais_customer_hk            STRING,
    addr_type                   STRING,
    load_dts                    TIMESTAMP,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING,
    addr_line_1                 STRING,
    addr_line_2                 STRING,
    addr_line_3                 STRING,
    addr_line_4                 STRING,
    city                        STRING,
    region_code                 STRING,
    country_code                STRING,
    postal_code                 STRING,
    record_source               STRING
)
STORED AS PARQUET;
