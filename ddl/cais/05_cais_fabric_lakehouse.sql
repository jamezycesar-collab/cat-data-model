-- CAIS Silver + Gold DDL (Microsoft Fabric Lakehouse).
-- Mirror of 01_cais_silver_delta.sql + 02_cais_gold_delta.sql for
-- Fabric Lakehouse (Spark SQL on OneLake / Delta Lake).
--
-- Fabric Lakehouse supports the same Delta-flavoured Spark SQL syntax as
-- Databricks Delta, so this file is mostly a copy of the Delta variant
-- with the USING DELTA clauses kept intact and identity columns simplified.


-- ===== Silver =====

CREATE TABLE IF NOT EXISTS silver.hub_cais_fdid (
    cais_fdid_hk                   STRING NOT NULL,
    cais_fdid_bk_firmDesignatedID  STRING NOT NULL,
    cais_fdid_bk_catReporterCRD    BIGINT NOT NULL,
    load_dts                       TIMESTAMP NOT NULL,
    record_source                  STRING NOT NULL,
    dv2_source_hk                  STRING NOT NULL
)
USING DELTA
COMMENT 'DV2 Hub: CAIS FDID records (Fabric Lakehouse). BK = (catReporterCRD, firmDesignatedID).';


CREATE TABLE IF NOT EXISTS silver.hub_cais_customer (
    cais_customer_hk                  STRING NOT NULL,
    cais_customer_bk_recordID         STRING NOT NULL,
    cais_customer_bk_catReporterCRD   BIGINT NOT NULL,
    customer_type                     STRING NOT NULL,
    load_dts                          TIMESTAMP NOT NULL,
    record_source                     STRING NOT NULL,
    dv2_source_hk                     STRING NOT NULL,
    CONSTRAINT chk_cais_customer_type CHECK (customer_type IN ('NATURAL_PERSON', 'LEGAL_ENTITY'))
)
USING DELTA
COMMENT 'DV2 Hub: CAIS Customer (Fabric Lakehouse). PII held in paired TID file.';


CREATE TABLE IF NOT EXISTS silver.hub_cais_large_trader (
    cais_large_trader_hk           STRING NOT NULL,
    large_trader_id                STRING NOT NULL,
    large_trader_type              STRING NOT NULL,
    load_dts                       TIMESTAMP NOT NULL,
    record_source                  STRING NOT NULL,
    dv2_source_hk                  STRING NOT NULL,
    CONSTRAINT chk_cais_lt_type CHECK (large_trader_type IN ('LTID', 'ULTID'))
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.link_cais_fdid_customer (
    cais_fdid_customer_hk          STRING NOT NULL,
    cais_fdid_hk                   STRING NOT NULL,
    cais_customer_hk               STRING NOT NULL,
    customer_role                  STRING NOT NULL,
    role_start_date                DATE NOT NULL,
    role_end_date                  DATE,
    role_end_reason                STRING,
    has_discretion                 BOOLEAN,
    load_dts                       TIMESTAMP NOT NULL,
    record_source                  STRING NOT NULL,
    dv2_source_hk                  STRING NOT NULL,
    CONSTRAINT chk_link_cais_role CHECK (customer_role IN ('TRDHOLDER', 'AUTH3RD', 'AUTHREP'))
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.link_cais_fdid_large_trader (
    cais_fdid_lt_hk                STRING NOT NULL,
    cais_fdid_hk                   STRING NOT NULL,
    cais_large_trader_hk           STRING NOT NULL,
    ltid_effective_date            DATE NOT NULL,
    ltid_end_date                  DATE,
    ltid_end_reason                STRING,
    load_dts                       TIMESTAMP NOT NULL,
    record_source                  STRING NOT NULL,
    dv2_source_hk                  STRING NOT NULL
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.link_cais_submission (
    cais_submission_hk             STRING NOT NULL,
    submission_filename            STRING NOT NULL,
    cat_submitter_id               BIGINT NOT NULL,
    cat_reporter_crd               BIGINT NOT NULL,
    file_generation_date           DATE NOT NULL,
    file_number                    INT,
    submission_action              STRING NOT NULL,
    file_type                      STRING NOT NULL,
    paired_filename                STRING,
    load_dts                       TIMESTAMP NOT NULL,
    record_source                  STRING NOT NULL,
    dv2_source_hk                  STRING NOT NULL,
    CONSTRAINT chk_cais_action CHECK (submission_action IN
        ('NEW','UPDATE','REFRESH','FIRM_CORRECTION','REPAIR','REPLACEMENT','MASS_TRANSFER')),
    CONSTRAINT chk_cais_filetype CHECK (file_type IN
        ('CAIS_DATA_FILE','TRANSFORMED_IDENTIFIERS_FILE'))
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.sat_cais_fdid_state (
    cais_fdid_hk                   STRING NOT NULL,
    load_dts                       TIMESTAMP NOT NULL,
    load_end_dts                   TIMESTAMP,
    hash_diff                      STRING NOT NULL,
    fdid_type                      STRING NOT NULL,
    account_types_array            ARRAY<STRING>,
    account_name_extended          STRING,
    dvp_custodian_ids              ARRAY<STRING>,
    clearing_broker_ids            ARRAY<STRING>,
    branch_office_crd              BIGINT,
    registered_rep_crds            ARRAY<BIGINT>,
    fdid_date                      DATE NOT NULL,
    fdid_end_date                  DATE,
    fdid_end_reason                STRING,
    replaced_by_fdid               STRING,
    prior_cat_reporter_crd         BIGINT,
    prior_cat_reporter_fdid        STRING,
    correspondent_crd              BIGINT,
    record_source                  STRING NOT NULL,
    CONSTRAINT chk_sat_cais_fdid_type CHECK (fdid_type IN ('ACCOUNT', 'RELATIONSHIP', 'ENTITYID')),
    CONSTRAINT chk_sat_cais_end_reason CHECK (fdid_end_reason IS NULL OR fdid_end_reason IN
        ('CORRECTION','ENDED','INACTIVE','REPLACED','OTHER','TRANSFER'))
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.sat_cais_customer_state (
    cais_customer_hk               STRING NOT NULL,
    load_dts                       TIMESTAMP NOT NULL,
    load_end_dts                   TIMESTAMP,
    hash_diff                      STRING NOT NULL,
    year_of_birth                  INT,
    has_discretion                 BOOLEAN,
    ein                            STRING,
    legal_entity_type              STRING,
    nationality_code               STRING,
    is_authorized_trader           BOOLEAN,
    record_source                  STRING NOT NULL
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.sat_cais_fdid_address (
    cais_fdid_hk                   STRING NOT NULL,
    addr_type                      STRING NOT NULL,
    load_dts                       TIMESTAMP NOT NULL,
    load_end_dts                   TIMESTAMP,
    hash_diff                      STRING NOT NULL,
    addr_line_1                    STRING NOT NULL,
    addr_line_2                    STRING,
    addr_line_3                    STRING,
    addr_line_4                    STRING,
    city                           STRING NOT NULL,
    region_code                    STRING,
    country_code                   STRING NOT NULL,
    postal_code                    STRING,
    record_source                  STRING NOT NULL,
    CONSTRAINT chk_sat_cais_addr_type CHECK (addr_type IN ('ADDRESS1','ADDRESS2','ADDRESS3','ADDRESS4'))
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.sat_cais_customer_address (
    cais_customer_hk               STRING NOT NULL,
    addr_type                      STRING NOT NULL,
    load_dts                       TIMESTAMP NOT NULL,
    load_end_dts                   TIMESTAMP,
    hash_diff                      STRING NOT NULL,
    addr_line_1                    STRING NOT NULL,
    addr_line_2                    STRING,
    addr_line_3                    STRING,
    addr_line_4                    STRING,
    city                           STRING NOT NULL,
    region_code                    STRING,
    country_code                   STRING NOT NULL,
    postal_code                    STRING,
    record_source                  STRING NOT NULL,
    CONSTRAINT chk_sat_cais_cust_addr_type CHECK (addr_type IN ('ADDRESS1','ADDRESS2','ADDRESS3','ADDRESS4'))
)
USING DELTA;


-- ===== Gold =====

CREATE TABLE IF NOT EXISTS gold.dim_cais_fdid_type (
    cais_fdid_type_sk           BIGINT GENERATED ALWAYS AS IDENTITY,
    cais_fdid_type_bk           STRING NOT NULL,
    description                 STRING NOT NULL,
    CONSTRAINT chk_cais_fdid_type CHECK (cais_fdid_type_bk IN ('ACCOUNT','RELATIONSHIP','ENTITYID'))
)
USING DELTA;


CREATE TABLE IF NOT EXISTS gold.dim_cais_account_type (
    cais_account_type_sk        BIGINT GENERATED ALWAYS AS IDENTITY,
    cais_account_type_bk        STRING NOT NULL,
    description                 STRING NOT NULL,
    is_pii_sensitive            BOOLEAN NOT NULL,
    CONSTRAINT chk_cais_acct_type CHECK (cais_account_type_bk IN
        ('AVERAGE','DVP/RVP','EDUCATION','ENTITYID','ERROR','FIRM','INSTITUTION',
         'MARKET','MARGIN','OPTION','OTHER','RELATIONSHIP','RETIREMENT','UGMA/UTMA'))
)
USING DELTA;


CREATE TABLE IF NOT EXISTS gold.fact_cais_fdid (
    cais_fdid_sk                BIGINT GENERATED ALWAYS AS IDENTITY,
    as_of_date                  DATE NOT NULL,
    date_sk                     BIGINT NOT NULL,
    cais_fdid_type_sk           BIGINT NOT NULL,
    cat_reporter_party_sk       BIGINT NOT NULL,
    firm_designated_id          STRING NOT NULL,
    cat_reporter_crd            BIGINT NOT NULL,
    correspondent_crd           BIGINT,
    fdid_date                   DATE NOT NULL,
    fdid_end_date               DATE,
    fdid_end_reason             STRING,
    branch_office_crd           BIGINT,
    customer_count              INT NOT NULL,
    has_active_trdholder        BOOLEAN NOT NULL,
    has_active_ltid             BOOLEAN NOT NULL,
    last_submission_id          STRING NOT NULL,
    last_submission_action      STRING NOT NULL,
    source_file                 STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT chk_fact_cais_fdid_end_reason CHECK (fdid_end_reason IS NULL OR fdid_end_reason IN
        ('CORRECTION','ENDED','INACTIVE','REPLACED','OTHER','TRANSFER'))
)
USING DELTA
PARTITIONED BY (as_of_date);


CREATE TABLE IF NOT EXISTS gold.fact_cais_customer (
    cais_customer_sk            BIGINT GENERATED ALWAYS AS IDENTITY,
    as_of_date                  DATE NOT NULL,
    date_sk                     BIGINT NOT NULL,
    cat_reporter_party_sk       BIGINT NOT NULL,
    customer_record_id          STRING NOT NULL,
    customer_type               STRING NOT NULL,
    fdid_count                  INT NOT NULL,
    has_pii_in_tid              BOOLEAN NOT NULL,
    last_submission_id          STRING NOT NULL,
    last_submission_action      STRING NOT NULL,
    source_file                 STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT chk_cais_customer_type CHECK (customer_type IN ('NATURAL_PERSON','LEGAL_ENTITY'))
)
USING DELTA
PARTITIONED BY (as_of_date);


CREATE TABLE IF NOT EXISTS gold.fact_cais_submission (
    cais_submission_sk          BIGINT GENERATED ALWAYS AS IDENTITY,
    submission_filename         STRING NOT NULL,
    submission_dts              TIMESTAMP NOT NULL,
    cat_submitter_id            BIGINT NOT NULL,
    cat_reporter_crd            BIGINT NOT NULL,
    cat_reporter_party_sk       BIGINT NOT NULL,
    file_type                   STRING NOT NULL,
    submission_action           STRING NOT NULL,
    paired_filename             STRING,
    fdid_record_count           INT,
    natural_person_record_count INT,
    legal_entity_record_count   INT,
    accepted                    BOOLEAN NOT NULL,
    error_count                 INT NOT NULL,
    rejection_count             INT NOT NULL,
    inconsistency_count         INT NOT NULL,
    quality_outcome             STRING,
    source_file                 STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT chk_cais_sub_filetype CHECK (file_type IN ('CAIS_DATA_FILE','TRANSFORMED_IDENTIFIERS_FILE')),
    CONSTRAINT chk_cais_sub_action CHECK (submission_action IN
        ('NEW','UPDATE','REFRESH','FIRM_CORRECTION','REPAIR','REPLACEMENT','MASS_TRANSFER'))
)
USING DELTA;


CREATE TABLE IF NOT EXISTS gold.fact_cais_inconsistency (
    cais_inconsistency_sk       BIGINT GENERATED ALWAYS AS IDENTITY,
    detected_dts                TIMESTAMP NOT NULL,
    cais_submission_sk          BIGINT NOT NULL,
    inconsistency_code          STRING NOT NULL,
    severity                    STRING NOT NULL,
    affected_record_id          STRING NOT NULL,
    affected_record_type        STRING NOT NULL,
    description                 STRING,
    resolved_dts                TIMESTAMP,
    resolution_submission_sk    BIGINT,
    CONSTRAINT chk_cais_inc_severity CHECK (severity IN ('MATERIAL','NON_MATERIAL')),
    CONSTRAINT chk_cais_inc_record_type CHECK (affected_record_type IN ('FDID','CUSTOMER'))
)
USING DELTA;
