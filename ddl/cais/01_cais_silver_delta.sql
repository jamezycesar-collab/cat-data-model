-- CAIS (Customer & Account Information System) Silver-layer DDL (Delta Lake).
--
-- Source: Full CAIS Technical Specifications v2.2.0r4 Sections 4.1, 5.1.3.
--
-- CAIS is FILE-BASED (not event-based). The reportable units are:
--   * CAIS Data File - contains FDID Records, Customer Records, Address Records
--   * Transformed Identifiers File (TID) - contains TID values, segregated for PII
--
-- The two files are submitted as a paired set. CAIS records are state snapshots,
-- not deltas; each submission replaces the prior state of a record.
--
-- DV2 mapping:
--   hub_cais_fdid              - one hub per FDID (firmDesignatedID + CATReporterCRD)
--   hub_cais_customer          - one hub per Customer (customerRecordID)
--   link_cais_fdid_customer    - relates FDID to its Customers (with role)
--   link_cais_fdid_lt          - relates FDID to LTID/ULTID large-trader records
--   link_cais_submission       - records the file submission that produced a state
--   sat_cais_fdid_state        - SCD2 attributes for FDID (type, account types, dates, etc)
--   sat_cais_customer_state    - SCD2 attributes for Customer
--   sat_cais_fdid_address      - SCD2 address records for FDID (1..4 per FDID)
--   sat_cais_customer_address  - SCD2 address records for Customer (1..4 per Customer)


CREATE TABLE IF NOT EXISTS silver.hub_cais_fdid (
    cais_fdid_hk                STRING NOT NULL,        -- SHA-256(catReporterCRD || firmDesignatedID)
    cais_fdid_bk_firmDesignatedID  STRING NOT NULL,
    cais_fdid_bk_catReporterCRD    BIGINT NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_hub_cais_fdid PRIMARY KEY (cais_fdid_hk)
)
USING DELTA
COMMENT 'DV2 Hub: CAIS FDID records. Business key is (catReporterCRD, firmDesignatedID).';


CREATE TABLE IF NOT EXISTS silver.hub_cais_customer (
    cais_customer_hk            STRING NOT NULL,
    cais_customer_bk_recordID   STRING NOT NULL,        -- customerRecordID, unique per CAT Reporter CRD
    cais_customer_bk_catReporterCRD BIGINT NOT NULL,
    customer_type               STRING NOT NULL,        -- NATURAL_PERSON or LEGAL_ENTITY
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_hub_cais_customer PRIMARY KEY (cais_customer_hk),
    CONSTRAINT chk_cais_customer_type CHECK (customer_type IN ('NATURAL_PERSON', 'LEGAL_ENTITY'))
)
USING DELTA
COMMENT 'DV2 Hub: CAIS Customer records. PII is in the paired Transformed Identifiers File, not here.';


CREATE TABLE IF NOT EXISTS silver.hub_cais_large_trader (
    cais_large_trader_hk        STRING NOT NULL,
    large_trader_id             STRING NOT NULL,        -- LTID or ULTID
    large_trader_type           STRING NOT NULL,        -- LTID or ULTID
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_hub_cais_lt PRIMARY KEY (cais_large_trader_hk),
    CONSTRAINT chk_cais_lt_type CHECK (large_trader_type IN ('LTID', 'ULTID'))
)
USING DELTA
COMMENT 'DV2 Hub: CAIS large trader IDs (per SEC Rule 13h-1).';


CREATE TABLE IF NOT EXISTS silver.link_cais_fdid_customer (
    cais_fdid_customer_hk       STRING NOT NULL,
    cais_fdid_hk                STRING NOT NULL,
    cais_customer_hk            STRING NOT NULL,
    customer_role               STRING NOT NULL,        -- TRDHOLDER, AUTH3RD, AUTHREP
    role_start_date             DATE NOT NULL,
    role_end_date               DATE,
    role_end_reason             STRING,
    has_discretion              BOOLEAN,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_link_cais_fc PRIMARY KEY (cais_fdid_customer_hk),
    CONSTRAINT fk_link_cais_fc_fdid     FOREIGN KEY (cais_fdid_hk)
        REFERENCES silver.hub_cais_fdid (cais_fdid_hk),
    CONSTRAINT fk_link_cais_fc_customer FOREIGN KEY (cais_customer_hk)
        REFERENCES silver.hub_cais_customer (cais_customer_hk),
    CONSTRAINT chk_link_cais_role CHECK (customer_role IN ('TRDHOLDER', 'AUTH3RD', 'AUTHREP'))
)
USING DELTA
COMMENT 'DV2 Link: FDID to Customer with role. SCD2 windows tracked via role_start_date / role_end_date.';


CREATE TABLE IF NOT EXISTS silver.link_cais_fdid_large_trader (
    cais_fdid_lt_hk             STRING NOT NULL,
    cais_fdid_hk                STRING NOT NULL,
    cais_large_trader_hk        STRING NOT NULL,
    ltid_effective_date         DATE NOT NULL,
    ltid_end_date               DATE,
    ltid_end_reason             STRING,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_link_cais_flt PRIMARY KEY (cais_fdid_lt_hk),
    CONSTRAINT fk_link_cais_flt_fdid FOREIGN KEY (cais_fdid_hk)
        REFERENCES silver.hub_cais_fdid (cais_fdid_hk),
    CONSTRAINT fk_link_cais_flt_lt   FOREIGN KEY (cais_large_trader_hk)
        REFERENCES silver.hub_cais_large_trader (cais_large_trader_hk)
)
USING DELTA
COMMENT 'DV2 Link: FDID to LTID/ULTID with effective dates.';


CREATE TABLE IF NOT EXISTS silver.link_cais_submission (
    cais_submission_hk          STRING NOT NULL,
    submission_filename         STRING NOT NULL,
    cat_submitter_id            BIGINT NOT NULL,
    cat_reporter_crd            BIGINT NOT NULL,
    file_generation_date        DATE NOT NULL,
    file_number                 INT,
    submission_action           STRING NOT NULL,        -- NEW UPDATE REFRESH FIRM_CORRECTION REPAIR REPLACEMENT MASS_TRANSFER
    file_type                   STRING NOT NULL,        -- CAIS_DATA_FILE or TRANSFORMED_IDENTIFIERS_FILE
    paired_filename             STRING,                 -- name of the paired file in the submission
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_link_cais_sub PRIMARY KEY (cais_submission_hk),
    CONSTRAINT chk_cais_action CHECK (submission_action IN
        ('NEW', 'UPDATE', 'REFRESH', 'FIRM_CORRECTION', 'REPAIR', 'REPLACEMENT', 'MASS_TRANSFER')),
    CONSTRAINT chk_cais_filetype CHECK (file_type IN
        ('CAIS_DATA_FILE', 'TRANSFORMED_IDENTIFIERS_FILE'))
)
USING DELTA
COMMENT 'DV2 Link: CAIS file submission registry. Each submission produces or updates FDID/Customer state.';


CREATE TABLE IF NOT EXISTS silver.sat_cais_fdid_state (
    cais_fdid_hk                STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING NOT NULL,
    fdid_type                   STRING NOT NULL,        -- ACCOUNT, RELATIONSHIP, ENTITYID
    account_types_array         ARRAY<STRING>,          -- one or more accountType values
    account_name_extended       STRING,
    dvp_custodian_ids           ARRAY<STRING>,
    clearing_broker_ids         ARRAY<STRING>,
    branch_office_crd           BIGINT,
    registered_rep_crds         ARRAY<BIGINT>,
    fdid_date                   DATE NOT NULL,
    fdid_end_date               DATE,
    fdid_end_reason             STRING,                 -- CORRECTION ENDED INACTIVE REPLACED OTHER TRANSFER
    replaced_by_fdid            STRING,
    prior_cat_reporter_crd      BIGINT,
    prior_cat_reporter_fdid     STRING,
    correspondent_crd           BIGINT,
    record_source               STRING NOT NULL,
    CONSTRAINT pk_sat_cais_fdid PRIMARY KEY (cais_fdid_hk, load_dts),
    CONSTRAINT fk_sat_cais_fdid FOREIGN KEY (cais_fdid_hk)
        REFERENCES silver.hub_cais_fdid (cais_fdid_hk),
    CONSTRAINT chk_sat_cais_fdid_type CHECK (fdid_type IN ('ACCOUNT', 'RELATIONSHIP', 'ENTITYID')),
    CONSTRAINT chk_sat_cais_end_reason CHECK (fdid_end_reason IS NULL OR fdid_end_reason IN
        ('CORRECTION', 'ENDED', 'INACTIVE', 'REPLACED', 'OTHER', 'TRANSFER'))
)
USING DELTA
COMMENT 'DV2 Satellite: SCD2 state of FDID Records.';


CREATE TABLE IF NOT EXISTS silver.sat_cais_customer_state (
    cais_customer_hk            STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING NOT NULL,
    -- Natural Person fields (PII held in TID file, not here)
    year_of_birth               INT,
    has_discretion              BOOLEAN,
    -- Legal Entity fields
    ein                         STRING,                 -- Employer Identification Number
    legal_entity_type           STRING,
    -- Common
    nationality_code            STRING,
    is_authorized_trader        BOOLEAN,
    record_source               STRING NOT NULL,
    CONSTRAINT pk_sat_cais_customer PRIMARY KEY (cais_customer_hk, load_dts),
    CONSTRAINT fk_sat_cais_customer FOREIGN KEY (cais_customer_hk)
        REFERENCES silver.hub_cais_customer (cais_customer_hk)
)
USING DELTA
COMMENT 'DV2 Satellite: SCD2 state of Customer Records (excluding PII held in paired TID file).';


CREATE TABLE IF NOT EXISTS silver.sat_cais_fdid_address (
    cais_fdid_hk                STRING NOT NULL,
    addr_type                   STRING NOT NULL,        -- ADDRESS1, ADDRESS2, ADDRESS3, ADDRESS4
    load_dts                    TIMESTAMP NOT NULL,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING NOT NULL,
    addr_line_1                 STRING NOT NULL,
    addr_line_2                 STRING,
    addr_line_3                 STRING,
    addr_line_4                 STRING,
    city                        STRING NOT NULL,
    region_code                 STRING,
    country_code                STRING NOT NULL,
    postal_code                 STRING,
    record_source               STRING NOT NULL,
    CONSTRAINT pk_sat_cais_fdid_addr PRIMARY KEY (cais_fdid_hk, addr_type, load_dts),
    CONSTRAINT fk_sat_cais_fdid_addr FOREIGN KEY (cais_fdid_hk)
        REFERENCES silver.hub_cais_fdid (cais_fdid_hk),
    CONSTRAINT chk_sat_cais_addr_type CHECK (addr_type IN ('ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'ADDRESS4'))
)
USING DELTA
COMMENT 'DV2 Satellite: FDID addresses. 1..4 per FDID, ADDRESS1 always populated.';


CREATE TABLE IF NOT EXISTS silver.sat_cais_customer_address (
    cais_customer_hk            STRING NOT NULL,
    addr_type                   STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING NOT NULL,
    addr_line_1                 STRING NOT NULL,
    addr_line_2                 STRING,
    addr_line_3                 STRING,
    addr_line_4                 STRING,
    city                        STRING NOT NULL,
    region_code                 STRING,
    country_code                STRING NOT NULL,
    postal_code                 STRING,
    record_source               STRING NOT NULL,
    CONSTRAINT pk_sat_cais_cust_addr PRIMARY KEY (cais_customer_hk, addr_type, load_dts),
    CONSTRAINT fk_sat_cais_cust_addr FOREIGN KEY (cais_customer_hk)
        REFERENCES silver.hub_cais_customer (cais_customer_hk),
    CONSTRAINT chk_sat_cais_cust_addr_type CHECK (addr_type IN ('ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'ADDRESS4'))
)
USING DELTA
COMMENT 'DV2 Satellite: Customer addresses (effective from July 12 2023, at least one required).';
