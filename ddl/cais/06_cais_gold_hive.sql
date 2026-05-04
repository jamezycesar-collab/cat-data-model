-- CAIS Gold-layer DDL (Apache Hive).
-- Hive does not enforce PRIMARY KEY / FOREIGN KEY / CHECK constraints;
-- they are documented as comments only.

CREATE TABLE IF NOT EXISTS gold.dim_cais_fdid_type (
    cais_fdid_type_sk           BIGINT,
    cais_fdid_type_bk           STRING COMMENT 'CHECK in ACCOUNT,RELATIONSHIP,ENTITYID',
    description                 STRING
)
STORED AS PARQUET
TBLPROPERTIES ('comment' = 'Gold dim: CAIS FDID type (3 static values).');


CREATE TABLE IF NOT EXISTS gold.dim_cais_account_type (
    cais_account_type_sk        BIGINT,
    cais_account_type_bk        STRING COMMENT 'CHECK in 14 spec values',
    description                 STRING,
    is_pii_sensitive            BOOLEAN
)
STORED AS PARQUET
TBLPROPERTIES ('comment' = 'Gold dim: CAIS accountType enumeration (14 static values).');


CREATE TABLE IF NOT EXISTS gold.fact_cais_fdid (
    cais_fdid_sk                BIGINT,
    date_sk                     BIGINT,
    cais_fdid_type_sk           BIGINT,
    cat_reporter_party_sk       BIGINT,
    firm_designated_id          STRING,
    cat_reporter_crd            BIGINT,
    correspondent_crd           BIGINT,
    fdid_date                   DATE,
    fdid_end_date               DATE,
    fdid_end_reason             STRING,
    branch_office_crd           BIGINT,
    customer_count              INT,
    has_active_trdholder        BOOLEAN,
    has_active_ltid             BOOLEAN,
    last_submission_id          STRING,
    last_submission_action      STRING,
    last_refresh_date           DATE,
    source_file                 STRING,
    dv2_source_hk               STRING,
    quality_outcome             STRING
)
PARTITIONED BY (as_of_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('comment' = 'Gold fact: snapshot row per FDID per as-of date.');


CREATE TABLE IF NOT EXISTS gold.fact_cais_customer (
    cais_customer_sk            BIGINT,
    date_sk                     BIGINT,
    cat_reporter_party_sk       BIGINT,
    customer_record_id          STRING,
    customer_type               STRING COMMENT 'CHECK in NATURAL_PERSON,LEGAL_ENTITY',
    fdid_count                  INT,
    has_pii_in_tid              BOOLEAN,
    last_submission_id          STRING,
    last_submission_action      STRING,
    source_file                 STRING,
    dv2_source_hk               STRING,
    quality_outcome             STRING
)
PARTITIONED BY (as_of_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('comment' = 'Gold fact: snapshot row per Customer per as-of date.');


CREATE TABLE IF NOT EXISTS gold.fact_cais_submission (
    cais_submission_sk          BIGINT,
    submission_filename         STRING,
    submission_dts              TIMESTAMP,
    cat_submitter_id            BIGINT,
    cat_reporter_crd            BIGINT,
    cat_reporter_party_sk       BIGINT,
    file_type                   STRING,
    submission_action           STRING,
    paired_filename             STRING,
    fdid_record_count           INT,
    natural_person_record_count INT,
    legal_entity_record_count   INT,
    accepted                    BOOLEAN,
    error_count                 INT,
    rejection_count             INT,
    inconsistency_count         INT,
    quality_outcome             STRING,
    source_file                 STRING,
    dv2_source_hk               STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS gold.fact_cais_inconsistency (
    cais_inconsistency_sk       BIGINT,
    detected_dts                TIMESTAMP,
    cais_submission_sk          BIGINT,
    inconsistency_code          STRING,
    severity                    STRING COMMENT 'CHECK in MATERIAL,NON_MATERIAL',
    affected_record_id          STRING,
    affected_record_type        STRING COMMENT 'CHECK in FDID,CUSTOMER',
    description                 STRING,
    resolved_dts                TIMESTAMP,
    resolution_submission_sk    BIGINT
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS gold.fact_cais_outstanding_rejection (
    cais_rejection_sk           BIGINT,
    rejected_dts                TIMESTAMP,
    cais_submission_sk          BIGINT,
    error_roe_id                STRING,
    rejection_code              STRING,
    severity                    STRING COMMENT 'CHECK in DATA_VALIDATION,FILE_INTEGRITY,SCHEMA_VIOLATION,OTHER',
    affected_record_id          STRING,
    affected_record_type        STRING COMMENT 'CHECK in FDID,CUSTOMER,FILE',
    description                 STRING,
    repair_due_dts              TIMESTAMP,
    resolved_dts                TIMESTAMP,
    resolution_submission_sk    BIGINT
)
STORED AS PARQUET;


CREATE VIEW IF NOT EXISTS gold.vw_cais_overdue_inconsistencies AS
SELECT
    i.cais_inconsistency_sk,
    i.detected_dts,
    i.inconsistency_code,
    i.severity,
    i.affected_record_id,
    i.affected_record_type,
    i.description,
    i.cais_submission_sk,
    sub.submission_filename,
    sub.cat_reporter_crd,
    DATEDIFF(CURRENT_DATE, TO_DATE(i.detected_dts)) AS days_since_detected,
    CASE
        WHEN DATEDIFF(CURRENT_DATE, TO_DATE(i.detected_dts)) > 5 THEN TRUE
        ELSE FALSE
    END AS is_overdue
FROM gold.fact_cais_inconsistency i
JOIN gold.fact_cais_submission sub
    ON i.cais_submission_sk = sub.cais_submission_sk
WHERE i.resolved_dts IS NULL;
