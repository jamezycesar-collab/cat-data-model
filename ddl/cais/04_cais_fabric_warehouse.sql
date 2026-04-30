-- CAIS Silver + Gold DDL (Microsoft Fabric Warehouse / T-SQL).
-- T-SQL adjustments from the Delta variant:
--   * STRING -> VARCHAR(N) sized to spec maxima (40 char FDID, 64 char hashes)
--   * TIMESTAMP -> DATETIME2(7)
--   * BOOLEAN -> BIT
--   * BIGINT GENERATED ALWAYS AS IDENTITY -> IDENTITY(1,1)
--   * ARRAY<STRING> -> NVARCHAR(MAX) JSON-encoded (T-SQL has no array type)
--   * NOT ENFORCED on PK / FK / CHECK because Fabric Warehouse permits the syntax but doesn't enforce
--     (mirrors the multi-leg Fabric Warehouse pattern in 04_multileg_fabric_warehouse.sql)


-- ===== Silver =====

CREATE TABLE silver.hub_cais_fdid (
    cais_fdid_hk                  VARCHAR(64) NOT NULL,
    cais_fdid_bk_firmDesignatedID VARCHAR(40) NOT NULL,
    cais_fdid_bk_catReporterCRD   BIGINT NOT NULL,
    load_dts                      DATETIME2(7) NOT NULL,
    record_source                 VARCHAR(64) NOT NULL,
    dv2_source_hk                 VARCHAR(64) NOT NULL,
    CONSTRAINT pk_hub_cais_fdid PRIMARY KEY NONCLUSTERED (cais_fdid_hk) NOT ENFORCED
);

CREATE TABLE silver.hub_cais_customer (
    cais_customer_hk                VARCHAR(64) NOT NULL,
    cais_customer_bk_recordID       VARCHAR(40) NOT NULL,
    cais_customer_bk_catReporterCRD BIGINT NOT NULL,
    customer_type                   VARCHAR(16) NOT NULL,
    load_dts                        DATETIME2(7) NOT NULL,
    record_source                   VARCHAR(64) NOT NULL,
    dv2_source_hk                   VARCHAR(64) NOT NULL,
    CONSTRAINT pk_hub_cais_customer PRIMARY KEY NONCLUSTERED (cais_customer_hk) NOT ENFORCED,
    CONSTRAINT chk_cais_customer_type CHECK (customer_type IN ('NATURAL_PERSON', 'LEGAL_ENTITY'))
);

CREATE TABLE silver.hub_cais_large_trader (
    cais_large_trader_hk          VARCHAR(64) NOT NULL,
    large_trader_id               VARCHAR(13) NOT NULL,
    large_trader_type             VARCHAR(8)  NOT NULL,
    load_dts                      DATETIME2(7) NOT NULL,
    record_source                 VARCHAR(64) NOT NULL,
    dv2_source_hk                 VARCHAR(64) NOT NULL,
    CONSTRAINT pk_hub_cais_lt PRIMARY KEY NONCLUSTERED (cais_large_trader_hk) NOT ENFORCED,
    CONSTRAINT chk_cais_lt_type CHECK (large_trader_type IN ('LTID', 'ULTID'))
);

CREATE TABLE silver.link_cais_fdid_customer (
    cais_fdid_customer_hk         VARCHAR(64) NOT NULL,
    cais_fdid_hk                  VARCHAR(64) NOT NULL,
    cais_customer_hk              VARCHAR(64) NOT NULL,
    customer_role                 VARCHAR(16) NOT NULL,
    role_start_date               DATE NOT NULL,
    role_end_date                 DATE,
    role_end_reason               VARCHAR(32),
    has_discretion                BIT,
    load_dts                      DATETIME2(7) NOT NULL,
    record_source                 VARCHAR(64) NOT NULL,
    dv2_source_hk                 VARCHAR(64) NOT NULL,
    CONSTRAINT pk_link_cais_fc PRIMARY KEY NONCLUSTERED (cais_fdid_customer_hk) NOT ENFORCED,
    CONSTRAINT chk_link_cais_role CHECK (customer_role IN ('TRDHOLDER', 'AUTH3RD', 'AUTHREP'))
);

CREATE TABLE silver.link_cais_fdid_large_trader (
    cais_fdid_lt_hk               VARCHAR(64) NOT NULL,
    cais_fdid_hk                  VARCHAR(64) NOT NULL,
    cais_large_trader_hk          VARCHAR(64) NOT NULL,
    ltid_effective_date           DATE NOT NULL,
    ltid_end_date                 DATE,
    ltid_end_reason               VARCHAR(32),
    load_dts                      DATETIME2(7) NOT NULL,
    record_source                 VARCHAR(64) NOT NULL,
    dv2_source_hk                 VARCHAR(64) NOT NULL,
    CONSTRAINT pk_link_cais_flt PRIMARY KEY NONCLUSTERED (cais_fdid_lt_hk) NOT ENFORCED
);

CREATE TABLE silver.link_cais_submission (
    cais_submission_hk            VARCHAR(64) NOT NULL,
    submission_filename           VARCHAR(256) NOT NULL,
    cat_submitter_id              BIGINT NOT NULL,
    cat_reporter_crd              BIGINT NOT NULL,
    file_generation_date          DATE NOT NULL,
    file_number                   INT,
    submission_action             VARCHAR(32) NOT NULL,
    file_type                     VARCHAR(32) NOT NULL,
    paired_filename               VARCHAR(256),
    load_dts                      DATETIME2(7) NOT NULL,
    record_source                 VARCHAR(64) NOT NULL,
    dv2_source_hk                 VARCHAR(64) NOT NULL,
    CONSTRAINT pk_link_cais_sub PRIMARY KEY NONCLUSTERED (cais_submission_hk) NOT ENFORCED,
    CONSTRAINT chk_cais_action CHECK (submission_action IN
        ('NEW','UPDATE','REFRESH','FIRM_CORRECTION','REPAIR','REPLACEMENT','MASS_TRANSFER')),
    CONSTRAINT chk_cais_filetype CHECK (file_type IN
        ('CAIS_DATA_FILE','TRANSFORMED_IDENTIFIERS_FILE'))
);

CREATE TABLE silver.sat_cais_fdid_state (
    cais_fdid_hk                  VARCHAR(64) NOT NULL,
    load_dts                      DATETIME2(7) NOT NULL,
    load_end_dts                  DATETIME2(7),
    hash_diff                     VARCHAR(64) NOT NULL,
    fdid_type                     VARCHAR(16) NOT NULL,
    account_types_json            VARCHAR(MAX),
    account_name_extended         VARCHAR(200),
    dvp_custodian_ids_json        VARCHAR(MAX),
    clearing_broker_ids_json      VARCHAR(MAX),
    branch_office_crd             BIGINT,
    registered_rep_crds_json      VARCHAR(MAX),
    fdid_date                     DATE NOT NULL,
    fdid_end_date                 DATE,
    fdid_end_reason               VARCHAR(32),
    replaced_by_fdid              VARCHAR(40),
    prior_cat_reporter_crd        BIGINT,
    prior_cat_reporter_fdid       VARCHAR(40),
    correspondent_crd             BIGINT,
    record_source                 VARCHAR(64) NOT NULL,
    CONSTRAINT pk_sat_cais_fdid PRIMARY KEY NONCLUSTERED (cais_fdid_hk, load_dts) NOT ENFORCED,
    CONSTRAINT chk_sat_cais_fdid_type CHECK (fdid_type IN ('ACCOUNT', 'RELATIONSHIP', 'ENTITYID')),
    CONSTRAINT chk_sat_cais_end_reason CHECK (fdid_end_reason IS NULL OR fdid_end_reason IN
        ('CORRECTION','ENDED','INACTIVE','REPLACED','OTHER','TRANSFER'))
);

CREATE TABLE silver.sat_cais_customer_state (
    cais_customer_hk              VARCHAR(64) NOT NULL,
    load_dts                      DATETIME2(7) NOT NULL,
    load_end_dts                  DATETIME2(7),
    hash_diff                     VARCHAR(64) NOT NULL,
    year_of_birth                 INT,
    has_discretion                BIT,
    ein                           VARCHAR(20),
    legal_entity_type             VARCHAR(64),
    nationality_code              VARCHAR(2),
    is_authorized_trader          BIT,
    record_source                 VARCHAR(64) NOT NULL,
    CONSTRAINT pk_sat_cais_customer PRIMARY KEY NONCLUSTERED (cais_customer_hk, load_dts) NOT ENFORCED
);

CREATE TABLE silver.sat_cais_fdid_address (
    cais_fdid_hk                  VARCHAR(64) NOT NULL,
    addr_type                     VARCHAR(16) NOT NULL,
    load_dts                      DATETIME2(7) NOT NULL,
    load_end_dts                  DATETIME2(7),
    hash_diff                     VARCHAR(64) NOT NULL,
    addr_line_1                   VARCHAR(40) NOT NULL,
    addr_line_2                   VARCHAR(40),
    addr_line_3                   VARCHAR(40),
    addr_line_4                   VARCHAR(40),
    city                          VARCHAR(100) NOT NULL,
    region_code                   VARCHAR(40),
    country_code                  VARCHAR(2) NOT NULL,
    postal_code                   VARCHAR(15),
    record_source                 VARCHAR(64) NOT NULL,
    CONSTRAINT pk_sat_cais_fdid_addr PRIMARY KEY NONCLUSTERED (cais_fdid_hk, addr_type, load_dts) NOT ENFORCED,
    CONSTRAINT chk_sat_cais_addr_type CHECK (addr_type IN ('ADDRESS1','ADDRESS2','ADDRESS3','ADDRESS4'))
);

CREATE TABLE silver.sat_cais_customer_address (
    cais_customer_hk              VARCHAR(64) NOT NULL,
    addr_type                     VARCHAR(16) NOT NULL,
    load_dts                      DATETIME2(7) NOT NULL,
    load_end_dts                  DATETIME2(7),
    hash_diff                     VARCHAR(64) NOT NULL,
    addr_line_1                   VARCHAR(40) NOT NULL,
    addr_line_2                   VARCHAR(40),
    addr_line_3                   VARCHAR(40),
    addr_line_4                   VARCHAR(40),
    city                          VARCHAR(100) NOT NULL,
    region_code                   VARCHAR(40),
    country_code                  VARCHAR(2) NOT NULL,
    postal_code                   VARCHAR(15),
    record_source                 VARCHAR(64) NOT NULL,
    CONSTRAINT pk_sat_cais_cust_addr PRIMARY KEY NONCLUSTERED (cais_customer_hk, addr_type, load_dts) NOT ENFORCED,
    CONSTRAINT chk_sat_cais_cust_addr_type CHECK (addr_type IN ('ADDRESS1','ADDRESS2','ADDRESS3','ADDRESS4'))
);

-- ===== Gold =====

CREATE TABLE gold.dim_cais_fdid_type (
    cais_fdid_type_sk             BIGINT IDENTITY(1,1) NOT NULL,
    cais_fdid_type_bk             VARCHAR(16) NOT NULL,
    description                   VARCHAR(200) NOT NULL,
    CONSTRAINT pk_dim_cais_fdid_type PRIMARY KEY NONCLUSTERED (cais_fdid_type_sk) NOT ENFORCED,
    CONSTRAINT chk_dim_cais_fdid_type CHECK (cais_fdid_type_bk IN ('ACCOUNT','RELATIONSHIP','ENTITYID'))
);

CREATE TABLE gold.dim_cais_account_type (
    cais_account_type_sk          BIGINT IDENTITY(1,1) NOT NULL,
    cais_account_type_bk          VARCHAR(16) NOT NULL,
    description                   VARCHAR(MAX) NOT NULL,
    is_pii_sensitive              BIT NOT NULL,
    CONSTRAINT pk_dim_cais_acct_type PRIMARY KEY NONCLUSTERED (cais_account_type_sk) NOT ENFORCED,
    CONSTRAINT chk_dim_cais_acct_type CHECK (cais_account_type_bk IN
        ('AVERAGE','DVP/RVP','EDUCATION','ENTITYID','ERROR','FIRM','INSTITUTION',
         'MARKET','MARGIN','OPTION','OTHER','RELATIONSHIP','RETIREMENT','UGMA/UTMA'))
);

CREATE TABLE gold.fact_cais_fdid (
    cais_fdid_sk                  BIGINT IDENTITY(1,1) NOT NULL,
    as_of_date                    DATE NOT NULL,
    date_sk                       BIGINT NOT NULL,
    cais_fdid_type_sk             BIGINT NOT NULL,
    cat_reporter_party_sk         BIGINT NOT NULL,
    firm_designated_id            VARCHAR(40) NOT NULL,
    cat_reporter_crd              BIGINT NOT NULL,
    correspondent_crd             BIGINT,
    fdid_date                     DATE NOT NULL,
    fdid_end_date                 DATE,
    fdid_end_reason               VARCHAR(32),
    branch_office_crd             BIGINT,
    customer_count                INT NOT NULL,
    has_active_trdholder          BIT NOT NULL,
    has_active_ltid               BIT NOT NULL,
    last_submission_id            VARCHAR(64) NOT NULL,
    last_submission_action        VARCHAR(32) NOT NULL,
    source_file                   VARCHAR(256) NOT NULL,
    dv2_source_hk                 VARCHAR(64) NOT NULL,
    quality_outcome               VARCHAR(16),
    CONSTRAINT pk_fact_cais_fdid PRIMARY KEY NONCLUSTERED (cais_fdid_sk) NOT ENFORCED,
    CONSTRAINT chk_fact_cais_fdid_end_reason CHECK (fdid_end_reason IS NULL OR fdid_end_reason IN
        ('CORRECTION','ENDED','INACTIVE','REPLACED','OTHER','TRANSFER'))
);

CREATE TABLE gold.fact_cais_customer (
    cais_customer_sk              BIGINT IDENTITY(1,1) NOT NULL,
    as_of_date                    DATE NOT NULL,
    date_sk                       BIGINT NOT NULL,
    cat_reporter_party_sk         BIGINT NOT NULL,
    customer_record_id            VARCHAR(40) NOT NULL,
    customer_type                 VARCHAR(16) NOT NULL,
    fdid_count                    INT NOT NULL,
    has_pii_in_tid                BIT NOT NULL,
    last_submission_id            VARCHAR(64) NOT NULL,
    last_submission_action        VARCHAR(32) NOT NULL,
    source_file                   VARCHAR(256) NOT NULL,
    dv2_source_hk                 VARCHAR(64) NOT NULL,
    quality_outcome               VARCHAR(16),
    CONSTRAINT pk_fact_cais_customer PRIMARY KEY NONCLUSTERED (cais_customer_sk) NOT ENFORCED,
    CONSTRAINT chk_cais_customer_type CHECK (customer_type IN ('NATURAL_PERSON','LEGAL_ENTITY'))
);

CREATE TABLE gold.fact_cais_submission (
    cais_submission_sk            BIGINT IDENTITY(1,1) NOT NULL,
    submission_filename           VARCHAR(256) NOT NULL,
    submission_dts                DATETIME2(7) NOT NULL,
    cat_submitter_id              BIGINT NOT NULL,
    cat_reporter_crd              BIGINT NOT NULL,
    cat_reporter_party_sk         BIGINT NOT NULL,
    file_type                     VARCHAR(32) NOT NULL,
    submission_action             VARCHAR(32) NOT NULL,
    paired_filename               VARCHAR(256),
    fdid_record_count             INT,
    natural_person_record_count   INT,
    legal_entity_record_count     INT,
    accepted                      BIT NOT NULL,
    error_count                   INT NOT NULL,
    rejection_count               INT NOT NULL,
    inconsistency_count           INT NOT NULL,
    quality_outcome               VARCHAR(16),
    source_file                   VARCHAR(256) NOT NULL,
    dv2_source_hk                 VARCHAR(64) NOT NULL,
    CONSTRAINT pk_fact_cais_sub PRIMARY KEY NONCLUSTERED (cais_submission_sk) NOT ENFORCED,
    CONSTRAINT chk_cais_sub_filetype CHECK (file_type IN ('CAIS_DATA_FILE','TRANSFORMED_IDENTIFIERS_FILE')),
    CONSTRAINT chk_cais_sub_action CHECK (submission_action IN
        ('NEW','UPDATE','REFRESH','FIRM_CORRECTION','REPAIR','REPLACEMENT','MASS_TRANSFER'))
);

CREATE TABLE gold.fact_cais_inconsistency (
    cais_inconsistency_sk         BIGINT IDENTITY(1,1) NOT NULL,
    detected_dts                  DATETIME2(7) NOT NULL,
    cais_submission_sk            BIGINT NOT NULL,
    inconsistency_code            VARCHAR(16) NOT NULL,
    severity                      VARCHAR(16) NOT NULL,
    affected_record_id            VARCHAR(40) NOT NULL,
    affected_record_type          VARCHAR(16) NOT NULL,
    description                   VARCHAR(MAX),
    resolved_dts                  DATETIME2(7),
    resolution_submission_sk      BIGINT,
    CONSTRAINT pk_fact_cais_inc PRIMARY KEY NONCLUSTERED (cais_inconsistency_sk) NOT ENFORCED,
    CONSTRAINT chk_cais_inc_severity CHECK (severity IN ('MATERIAL','NON_MATERIAL')),
    CONSTRAINT chk_cais_inc_record_type CHECK (affected_record_type IN ('FDID','CUSTOMER'))
);
