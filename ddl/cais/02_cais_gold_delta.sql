-- CAIS Gold-layer DDL (Delta Lake).
--
-- The existing model had a fact_cais_snapshots table modeled around
-- "events". CAIS doesn't have events; it has paired-file submissions
-- that produce or update FDID/Customer state. This file replaces
-- fact_cais_snapshots with a correctly-shaped structure:
--
-- dim_cais_account_type - SCD2 dim aligned to spec accountType enum
-- dim_cais_fdid_type - small static dim for ACCOUNT/RELATIONSHIP/ENTITYID
-- fact_cais_fdid - one row per FDID per as-of date (snapshot semantics)
-- fact_cais_customer - one row per Customer per as-of date
-- fact_cais_submission - one row per CAIS file submission
-- fact_cais_inconsistency - tracking material inconsistencies per Section 6.4


CREATE TABLE IF NOT EXISTS gold.dim_cais_fdid_type (
 cais_fdid_type_sk BIGINT GENERATED ALWAYS AS IDENTITY,
 cais_fdid_type_bk STRING NOT NULL, -- ACCOUNT, RELATIONSHIP, ENTITYID
 description STRING NOT NULL,
 CONSTRAINT pk_dim_cais_fdid_type PRIMARY KEY (cais_fdid_type_sk),
 CONSTRAINT chk_cais_fdid_type CHECK (cais_fdid_type_bk IN ('ACCOUNT', 'RELATIONSHIP', 'ENTITYID'))
)
USING DELTA
COMMENT 'Gold dim: CAIS FDID type. Small static dim from spec.';


CREATE TABLE IF NOT EXISTS gold.dim_cais_account_type (
 cais_account_type_sk BIGINT GENERATED ALWAYS AS IDENTITY,
 cais_account_type_bk STRING NOT NULL,
 description STRING NOT NULL,
 is_pii_sensitive BOOLEAN NOT NULL,
 CONSTRAINT pk_dim_cais_acct_type PRIMARY KEY (cais_account_type_sk),
 CONSTRAINT chk_cais_acct_type CHECK (cais_account_type_bk IN (
 'AVERAGE', 'DVP/RVP', 'EDUCATION', 'ENTITYID', 'ERROR', 'FIRM',
 'INSTITUTION', 'MARKET', 'MARGIN', 'OPTION', 'OTHER',
 'RELATIONSHIP', 'RETIREMENT', 'UGMA/UTMA'))
)
USING DELTA
COMMENT 'Gold dim: CAIS accountType enumeration. 14 values per spec Section 4.1.';


CREATE TABLE IF NOT EXISTS gold.fact_cais_fdid (
 cais_fdid_sk BIGINT GENERATED ALWAYS AS IDENTITY,
 as_of_date DATE NOT NULL,
 -- conformed dim FKs
 date_sk BIGINT NOT NULL,
 cais_fdid_type_sk BIGINT NOT NULL,
 cat_reporter_party_sk BIGINT NOT NULL, -- the Industry Member reporting
 -- CAIS-specific
 firm_designated_id STRING NOT NULL,
 cat_reporter_crd BIGINT NOT NULL,
 correspondent_crd BIGINT,
 fdid_date DATE NOT NULL,
 fdid_end_date DATE,
 fdid_end_reason STRING,
 branch_office_crd BIGINT,
 customer_count INT NOT NULL,
 has_active_trdholder BOOLEAN NOT NULL,
 has_active_ltid BOOLEAN NOT NULL,
 -- lineage
 last_submission_id STRING NOT NULL,
 last_submission_action STRING NOT NULL,
 source_file STRING NOT NULL,
 dv2_source_hk STRING NOT NULL,
 quality_outcome STRING,
 CONSTRAINT pk_fact_cais_fdid PRIMARY KEY (cais_fdid_sk),
 CONSTRAINT fk_fact_cais_fdid_date FOREIGN KEY (date_sk)
 REFERENCES gold.dim_date (date_sk),
 CONSTRAINT fk_fact_cais_fdid_type FOREIGN KEY (cais_fdid_type_sk)
 REFERENCES gold.dim_cais_fdid_type (cais_fdid_type_sk),
 CONSTRAINT fk_fact_cais_fdid_party FOREIGN KEY (cat_reporter_party_sk)
 REFERENCES gold.dim_party (party_sk),
 CONSTRAINT chk_fact_cais_fdid_end_reason CHECK (fdid_end_reason IS NULL OR fdid_end_reason IN
 ('CORRECTION', 'ENDED', 'INACTIVE', 'REPLACED', 'OTHER', 'TRANSFER'))
)
USING DELTA
PARTITIONED BY (as_of_date)
COMMENT 'Gold fact: snapshot-grain row per FDID per as-of date.';


CREATE TABLE IF NOT EXISTS gold.fact_cais_customer (
 cais_customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
 as_of_date DATE NOT NULL,
 date_sk BIGINT NOT NULL,
 cat_reporter_party_sk BIGINT NOT NULL,
 customer_record_id STRING NOT NULL,
 customer_type STRING NOT NULL, -- NATURAL_PERSON or LEGAL_ENTITY
 fdid_count INT NOT NULL,
 has_pii_in_tid BOOLEAN NOT NULL, -- whether paired TID file contains PII
 last_submission_id STRING NOT NULL,
 last_submission_action STRING NOT NULL,
 source_file STRING NOT NULL,
 dv2_source_hk STRING NOT NULL,
 quality_outcome STRING,
 CONSTRAINT pk_fact_cais_customer PRIMARY KEY (cais_customer_sk),
 CONSTRAINT fk_fact_cais_cust_date FOREIGN KEY (date_sk)
 REFERENCES gold.dim_date (date_sk),
 CONSTRAINT fk_fact_cais_cust_party FOREIGN KEY (cat_reporter_party_sk)
 REFERENCES gold.dim_party (party_sk),
 CONSTRAINT chk_cais_customer_type CHECK (customer_type IN ('NATURAL_PERSON', 'LEGAL_ENTITY'))
)
USING DELTA
PARTITIONED BY (as_of_date)
COMMENT 'Gold fact: snapshot-grain row per Customer per as-of date.';


CREATE TABLE IF NOT EXISTS gold.fact_cais_submission (
 cais_submission_sk BIGINT GENERATED ALWAYS AS IDENTITY,
 submission_filename STRING NOT NULL,
 submission_dts TIMESTAMP NOT NULL,
 cat_submitter_id BIGINT NOT NULL,
 cat_reporter_crd BIGINT NOT NULL,
 cat_reporter_party_sk BIGINT NOT NULL,
 file_type STRING NOT NULL,
 submission_action STRING NOT NULL,
 paired_filename STRING,
 fdid_record_count INT,
 natural_person_record_count INT,
 legal_entity_record_count INT,
 accepted BOOLEAN NOT NULL,
 error_count INT NOT NULL,
 rejection_count INT NOT NULL,
 inconsistency_count INT NOT NULL,
 quality_outcome STRING,
 source_file STRING NOT NULL,
 dv2_source_hk STRING NOT NULL,
 CONSTRAINT pk_fact_cais_sub PRIMARY KEY (cais_submission_sk),
 CONSTRAINT chk_cais_sub_filetype CHECK (file_type IN
 ('CAIS_DATA_FILE', 'TRANSFORMED_IDENTIFIERS_FILE')),
 CONSTRAINT chk_cais_sub_action CHECK (submission_action IN
 ('NEW', 'UPDATE', 'REFRESH', 'FIRM_CORRECTION', 'REPAIR', 'REPLACEMENT', 'MASS_TRANSFER'))
)
USING DELTA
COMMENT 'Gold fact: per-file submission registry covering both CAIS Data and Transformed Identifiers files.';


CREATE TABLE IF NOT EXISTS gold.fact_cais_inconsistency (
 cais_inconsistency_sk BIGINT GENERATED ALWAYS AS IDENTITY,
 detected_dts TIMESTAMP NOT NULL,
 cais_submission_sk BIGINT NOT NULL,
 inconsistency_code STRING NOT NULL,
 severity STRING NOT NULL, -- MATERIAL or NON_MATERIAL
 affected_record_id STRING NOT NULL,
 affected_record_type STRING NOT NULL, -- FDID or CUSTOMER
 description STRING,
 resolved_dts TIMESTAMP,
 resolution_submission_sk BIGINT,
 CONSTRAINT pk_fact_cais_inc PRIMARY KEY (cais_inconsistency_sk),
 CONSTRAINT fk_fact_cais_inc_sub FOREIGN KEY (cais_submission_sk)
 REFERENCES gold.fact_cais_submission (cais_submission_sk),
 CONSTRAINT chk_cais_inc_severity CHECK (severity IN ('MATERIAL', 'NON_MATERIAL')),
 CONSTRAINT chk_cais_inc_record_type CHECK (affected_record_type IN ('FDID', 'CUSTOMER'))
)
USING DELTA
COMMENT 'Gold fact: tracks material inconsistencies per CAIS spec Section 6.4.';


-- Tier 9.3 additions: Outstanding Rejection tracking + overdue-inconsistency view

CREATE TABLE IF NOT EXISTS gold.fact_cais_outstanding_rejection (
    cais_rejection_sk           BIGINT GENERATED ALWAYS AS IDENTITY,
    rejected_dts                TIMESTAMP NOT NULL,
    cais_submission_sk          BIGINT NOT NULL,
    error_roe_id                STRING NOT NULL,        -- the errorROEID assigned by CAT
    rejection_code              STRING NOT NULL,        -- code from CAIS spec Appendix B (data validation errors)
    severity                    STRING NOT NULL,        -- DATA_VALIDATION, FILE_INTEGRITY, etc.
    affected_record_id          STRING NOT NULL,        -- firmDesignatedID or customerRecordID
    affected_record_type        STRING NOT NULL,        -- FDID or CUSTOMER or FILE
    description                 STRING,
    repair_due_dts              TIMESTAMP NOT NULL,     -- per CAIS Section 6.4.2 deadline
    resolved_dts                TIMESTAMP,
    resolution_submission_sk    BIGINT,
    CONSTRAINT pk_fact_cais_rejection PRIMARY KEY (cais_rejection_sk),
    CONSTRAINT fk_fact_cais_rej_sub FOREIGN KEY (cais_submission_sk)
        REFERENCES gold.fact_cais_submission (cais_submission_sk),
    CONSTRAINT chk_cais_rej_severity CHECK (severity IN
        ('DATA_VALIDATION', 'FILE_INTEGRITY', 'SCHEMA_VIOLATION', 'OTHER')),
    CONSTRAINT chk_cais_rej_record_type CHECK (affected_record_type IN ('FDID', 'CUSTOMER', 'FILE'))
)
USING DELTA
COMMENT 'Gold fact: tracks Outstanding Rejection notifications from CAT per CAIS spec Section 6.5.';


-- View: Overdue inconsistencies (past their repair deadline per Section 6.4.3)
CREATE OR REPLACE VIEW gold.vw_cais_overdue_inconsistencies AS
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
    -- Section 6.4.3: Material inconsistencies must be resolved within
    -- the deadline (typically 5 business days after notification).
    -- Days_overdue is computed against current_date for active monitoring.
    DATEDIFF(CURRENT_DATE(), DATE(i.detected_dts)) AS days_since_detected,
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), DATE(i.detected_dts)) > 5 THEN TRUE
        ELSE FALSE
    END AS is_overdue
FROM gold.fact_cais_inconsistency i
JOIN gold.fact_cais_submission sub
    ON i.cais_submission_sk = sub.cais_submission_sk
WHERE i.resolved_dts IS NULL;
