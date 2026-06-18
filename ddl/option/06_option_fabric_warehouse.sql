-- Simple-option Silver + Gold DDL (Microsoft Fabric Warehouse / T-SQL).

-- ===== Silver =====

CREATE TABLE silver.hub_option_order (
    option_order_hk             VARCHAR(64) NOT NULL,
    option_order_bk_orderID     VARCHAR(64) NOT NULL,
    option_order_bk_keyDate     DATE NOT NULL,
    option_order_bk_imid        VARCHAR(8)  NOT NULL,
    load_dts                    DATETIME2(7) NOT NULL,
    record_source               VARCHAR(64) NOT NULL,
    dv2_source_hk               VARCHAR(64) NOT NULL,
    CONSTRAINT pk_hub_option_order PRIMARY KEY NONCLUSTERED (option_order_hk) NOT ENFORCED
);

CREATE TABLE silver.hub_option_instrument (
    option_instrument_hk        VARCHAR(64) NOT NULL,
    option_id_bk                VARCHAR(22) NOT NULL,
    is_flex                     BIT NOT NULL,
    load_dts                    DATETIME2(7) NOT NULL,
    record_source               VARCHAR(64) NOT NULL,
    dv2_source_hk               VARCHAR(64) NOT NULL,
    CONSTRAINT pk_hub_option_instrument PRIMARY KEY NONCLUSTERED (option_instrument_hk) NOT ENFORCED
);

CREATE TABLE silver.link_option_event (
    option_event_hk             VARCHAR(64) NOT NULL,
    option_order_hk             VARCHAR(64) NOT NULL,
    option_instrument_hk        VARCHAR(64) NOT NULL,
    event_code                  VARCHAR(8)  NOT NULL,
    event_timestamp             DATETIME2(7) NOT NULL,
    event_date                  DATE NOT NULL,
    event_type_hk               VARCHAR(64) NOT NULL,
    venue_hk                    VARCHAR(64),
    sender_party_hk             VARCHAR(64),
    receiver_party_hk           VARCHAR(64),
    load_dts                    DATETIME2(7) NOT NULL,
    record_source               VARCHAR(64) NOT NULL,
    dv2_source_hk               VARCHAR(64) NOT NULL,
    CONSTRAINT pk_link_oe PRIMARY KEY NONCLUSTERED (option_event_hk) NOT ENFORCED,
    CONSTRAINT chk_link_oe_code CHECK (event_code IN (
        'MONO','MONOS','MOOR','MOMR','MOCR','MOORS','MOMRS','MOCRS','MOOA',
        'MOIR','MOIM','MOIC','MOIMR','MOICR',
        'MOCO','MOCOM','MOCOC',
        'MOOM','MOOMS','MOOMR','MOOJ','MOOC','MOOCR',
        'MONQ','MORQ','MOQR','MOQC','MOQM',
        'MOOT','MOOF','MOOFS','MOFA','MOPA','MOAA','MOOE'))
);

CREATE TABLE silver.link_option_child_order (
    option_child_order_hk       VARCHAR(64) NOT NULL,
    parent_option_order_hk      VARCHAR(64) NOT NULL,
    child_option_order_hk       VARCHAR(64) NOT NULL,
    load_dts                    DATETIME2(7) NOT NULL,
    record_source               VARCHAR(64) NOT NULL,
    dv2_source_hk               VARCHAR(64) NOT NULL,
    CONSTRAINT pk_link_oco PRIMARY KEY NONCLUSTERED (option_child_order_hk) NOT ENFORCED
);

CREATE TABLE silver.sat_option_order_state (
    option_order_hk             VARCHAR(64) NOT NULL,
    load_dts                    DATETIME2(7) NOT NULL,
    load_end_dts                DATETIME2(7),
    hash_diff                   VARCHAR(64) NOT NULL,
    side                        VARCHAR(8),
    order_status                VARCHAR(32),
    order_quantity              DECIMAL(38, 18),
    leaves_quantity             DECIMAL(38, 18),
    limit_price                 DECIMAL(38, 18),
    net_price                   DECIMAL(38, 18),
    time_in_force               VARCHAR(8),
    trading_session             VARCHAR(16),
    handling_instructions       VARCHAR(32),
    open_close_indicator        VARCHAR(2),
    representative_ind          VARCHAR(2),
    solicitation_flag           BIT,
    affiliate_flag              BIT,
    rfq_id                      VARCHAR(64),
    record_source               VARCHAR(64) NOT NULL,
    CONSTRAINT pk_sat_oo PRIMARY KEY NONCLUSTERED (option_order_hk, load_dts) NOT ENFORCED
);

CREATE TABLE silver.sat_option_event_state (
    option_event_hk             VARCHAR(64) NOT NULL,
    load_dts                    DATETIME2(7) NOT NULL,
    load_end_dts                DATETIME2(7),
    hash_diff                   VARCHAR(64) NOT NULL,
    event_qty                   DECIMAL(38, 18),
    event_price                 DECIMAL(38, 18),
    cancel_qty                  DECIMAL(38, 18),
    cancel_flag                 BIT,
    cancel_timestamp            DATETIME2(7),
    rejected_flag               BIT,
    initiator                   VARCHAR(8),
    capacity                    VARCHAR(8),
    market_center_id            VARCHAR(8),
    side_details_ind            VARCHAR(4),
    clearing_firm               VARCHAR(16),
    fulfillment_id              VARCHAR(64),
    fulfillment_link_type       VARCHAR(8),
    occ_clearing_member_id      VARCHAR(40),
    raw_payload_json            VARCHAR(MAX),
    record_source               VARCHAR(64) NOT NULL,
    CONSTRAINT pk_sat_oe PRIMARY KEY NONCLUSTERED (option_event_hk, load_dts) NOT ENFORCED
);

-- ===== Gold =====

CREATE TABLE gold.fact_option_order_events (
    option_event_sk             BIGINT IDENTITY(1,1) NOT NULL,
    event_dts                   DATETIME2(7) NOT NULL,
    event_date                  DATE NOT NULL,
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT NOT NULL,
    party_sk                    BIGINT NOT NULL,
    trader_sk                   BIGINT,
    desk_sk                     BIGINT,
    account_sk                  BIGINT,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT NOT NULL,
    option_id                   VARCHAR(22) NOT NULL,
    cat_order_id                VARCHAR(64) NOT NULL,
    cat_event_code              VARCHAR(8)  NOT NULL,
    side                        VARCHAR(8),
    price                       DECIMAL(38, 18),
    quantity                    DECIMAL(38, 18),
    leaves_qty                  DECIMAL(38, 18),
    cancel_qty                  DECIMAL(38, 18),
    order_type                  VARCHAR(12),
    time_in_force               VARCHAR(8),
    trading_session             VARCHAR(16),
    open_close_indicator        VARCHAR(2),
    handling_instructions       VARCHAR(32),
    net_price                   DECIMAL(38, 18),
    parent_order_id             VARCHAR(64),
    prior_order_id              VARCHAR(64),
    initiator                   VARCHAR(8),
    trigger_price               DECIMAL(38, 18),
    source_file                 VARCHAR(256) NOT NULL,
    source_batch_id             VARCHAR(64)  NOT NULL,
    dv2_source_hk               VARCHAR(64)  NOT NULL,
    quality_outcome             VARCHAR(16),
    CONSTRAINT pk_fact_option_event PRIMARY KEY NONCLUSTERED (option_event_sk) NOT ENFORCED,
    CONSTRAINT chk_fact_oe_code CHECK (cat_event_code IN (
        'MONO','MONOS','MOOR','MOMR','MOCR','MOORS','MOMRS','MOCRS','MOOA',
        'MOIR','MOIM','MOIC','MOIMR','MOICR',
        'MOCO','MOCOM','MOCOC',
        'MOOM','MOOMS','MOOMR','MOOJ','MOOC','MOOCR',
        'MONQ','MORQ','MOQR','MOQC','MOQM',
        'MOOE'))
);

CREATE TABLE gold.fact_option_executions (
    option_execution_sk         BIGINT IDENTITY(1,1) NOT NULL,
    event_dts                   DATETIME2(7) NOT NULL,
    event_date                  DATE NOT NULL,
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT NOT NULL,
    party_sk                    BIGINT NOT NULL,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT NOT NULL,
    option_id                   VARCHAR(22) NOT NULL,
    cat_event_code              VARCHAR(8)  NOT NULL,
    trade_id                    VARCHAR(64),
    trade_key_date              DATETIME2(7),           -- CAT IM v4.1.0r15 section 5.1.11 row 6 (MOOT)
    fulfillment_id              VARCHAR(64),
    fill_key_date               DATETIME2(7),           -- CAT IM v4.1.0r15 section 5.1.12.1 row 6 (MOOF/MOOFS)
    prior_fulfillment_id        VARCHAR(64),
    prior_fill_key_date         DATETIME2(7),           -- CAT IM v4.1.0r15 section 5.1.12.3 row 9 (MOFA)
    quantity                    DECIMAL(38, 18) NOT NULL,
    price                       DECIMAL(38, 18) NOT NULL,
    capacity                    VARCHAR(12),
    tape_trade_id               VARCHAR(40),
    market_center_id            VARCHAR(8),
    side_details_ind            VARCHAR(4),
    clearing_firm               VARCHAR(16),
    fulfillment_link_type       VARCHAR(8),
    cancel_flag                 BIT,
    cancel_timestamp            DATETIME2(7),
    multi_leg_ind               BIT,
    source_file                 VARCHAR(256) NOT NULL,
    source_batch_id             VARCHAR(64)  NOT NULL,
    dv2_source_hk               VARCHAR(64)  NOT NULL,
    quality_outcome             VARCHAR(16),
    CONSTRAINT pk_fact_option_execution PRIMARY KEY NONCLUSTERED (option_execution_sk) NOT ENFORCED,
    CONSTRAINT chk_fact_oex_code CHECK (cat_event_code IN ('MOOT','MOOF','MOOFS','MOFA'))
);

CREATE TABLE gold.fact_option_allocations (
    option_allocation_sk        BIGINT IDENTITY(1,1) NOT NULL,
    event_dts                   DATETIME2(7) NOT NULL,
    event_date                  DATE NOT NULL,
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT NOT NULL,
    party_sk                    BIGINT NOT NULL,
    account_sk                  BIGINT,
    event_type_sk               BIGINT NOT NULL,
    option_id                   VARCHAR(22) NOT NULL,
    cat_event_code              VARCHAR(8) NOT NULL,
    allocation_id               VARCHAR(64) NOT NULL,
    allocation_key_date         DATETIME2(7) NOT NULL,  -- CAT IM v4.1.0r15 section 5.1.13.1 row 6 (MOPA/MOAA)
    prior_allocation_id         VARCHAR(64),
    prior_allocation_key_date   DATETIME2(7),           -- CAT IM v4.1.0r15 section 5.1.13.2 row 8 (MOAA)
    side                        VARCHAR(8),
    quantity                    DECIMAL(38, 18) NOT NULL,
    price                       DECIMAL(38, 18),
    firm_designated_id          VARCHAR(40) NOT NULL,
    new_order_fdid              VARCHAR(40),
    correspondent_crd           BIGINT,
    occ_clearing_member_id      VARCHAR(40),
    trade_date                  DATE,
    settlement_date             DATE,
    allocation_type             VARCHAR(8),
    institution_flag            BIT,
    account_holder_type         VARCHAR(2),
    allocation_instruction_time DATETIME2(7),           -- CAT IM v4.1.0r15 section 5.1.13.1 row 24 (MOPA/MOAA)
    cancel_flag                 BIT,
    cancel_timestamp            DATETIME2(7),
    source_file                 VARCHAR(256) NOT NULL,
    source_batch_id             VARCHAR(64)  NOT NULL,
    dv2_source_hk               VARCHAR(64)  NOT NULL,
    quality_outcome             VARCHAR(16),
    CONSTRAINT pk_fact_option_allocation PRIMARY KEY NONCLUSTERED (option_allocation_sk) NOT ENFORCED,
    CONSTRAINT chk_fact_oal_code CHECK (cat_event_code IN ('MOPA','MOAA'))
);
