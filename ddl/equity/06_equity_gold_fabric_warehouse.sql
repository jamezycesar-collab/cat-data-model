-- ============================================================================
-- Equity Gold Layer (Microsoft Fabric Warehouse - T-SQL)
-- ============================================================================
--
-- Tier 13 (WS2 burndown sub-tier 1): builds fact_execution_events.
--
-- Fabric Warehouse uses T-SQL semantics: BIGINT IDENTITY for surrogate keys,
-- DATETIME2(7) for timestamps, VARCHAR for strings, BIT for booleans, NOT
-- ENFORCED constraints (Fabric does not enforce FK/PK at write time but the
-- declarations help the query optimizer).
-- ============================================================================

CREATE TABLE gold.fact_execution_events (
    execution_event_sk            BIGINT IDENTITY(1,1) NOT NULL,
    event_dts                     DATETIME2(7) NOT NULL,
    event_date                    DATE NOT NULL,
    -- conformed dimension FKs
    date_sk                       BIGINT NOT NULL,
    instrument_sk                 BIGINT NOT NULL,
    party_sk                      BIGINT NOT NULL,
    venue_sk                      BIGINT,
    event_type_sk                 BIGINT NOT NULL,
    -- equity execution event specifics (CAT IM v4.1.0r15 section 4.11.1)
    cat_order_id                  VARCHAR(64) NOT NULL,
    cat_event_code                VARCHAR(8)  NOT NULL,
    trade_key_date                DATETIME2(7),
    trade_id                      VARCHAR(64) NOT NULL,
    quantity                      DECIMAL(38, 18) NOT NULL,
    price                         DECIMAL(38, 18) NOT NULL,
    capacity                      VARCHAR(12) NOT NULL,
    tape_trade_id                 VARCHAR(40),
    market_center_id              VARCHAR(8)  NOT NULL,
    side_details_ind              VARCHAR(4),
    reporting_exception_code      VARCHAR(8),
    clearing_firm                 VARCHAR(16),
    counterparty                  VARCHAR(8),
    cancel_flag                   BIT,
    cancel_timestamp              DATETIME2(7),
    -- lineage
    source_file                   VARCHAR(256) NOT NULL,
    source_batch_id               VARCHAR(64)  NOT NULL,
    dv2_source_hk                 VARCHAR(64)  NOT NULL,
    quality_outcome               VARCHAR(16),
    CONSTRAINT pk_fact_execution_event PRIMARY KEY NONCLUSTERED (execution_event_sk) NOT ENFORCED,
    CONSTRAINT chk_fact_exec_code CHECK (cat_event_code IN (
        'MEOT','MEOTS'
    ))
);


-- ----------------------------------------------------------------------------
-- 2. fact_allocations  -- equity allocation events (MEPA/MEAA)
--    Tier 14 (WS2 sub-tier 2)
-- ----------------------------------------------------------------------------

CREATE TABLE gold.fact_allocations (
    allocation_event_sk           BIGINT IDENTITY(1,1) NOT NULL,
    event_dts                     DATETIME2(7) NOT NULL,
    event_date                    DATE NOT NULL,
    -- conformed dimension FKs
    date_sk                       BIGINT NOT NULL,
    instrument_sk                 BIGINT NOT NULL,
    party_sk                      BIGINT NOT NULL,
    account_sk                    BIGINT,
    event_type_sk                 BIGINT NOT NULL,
    -- allocation specifics
    cat_event_code                VARCHAR(8) NOT NULL,
    allocation_id                 VARCHAR(64) NOT NULL,
    allocation_key_date           DATETIME2(7) NOT NULL,
    prior_allocation_id           VARCHAR(64),
    prior_allocation_key_date     DATETIME2(7),
    side                          VARCHAR(8) NOT NULL,
    quantity                      DECIMAL(38, 18) NOT NULL,
    price                         DECIMAL(38, 18),
    firm_designated_id            VARCHAR(40) NOT NULL,
    new_order_fdid                VARCHAR(40),
    correspondent_crd             VARCHAR(16),
    trade_date                    DATE,
    settlement_date               DATE,
    allocation_type               VARCHAR(8),
    dvp_custodian_id              VARCHAR(40),
    institution_flag              BIT,
    account_holder_type           VARCHAR(2),
    allocation_instruction_time   DATETIME2(7),
    cancel_flag                   BIT,
    cancel_timestamp              DATETIME2(7),
    -- lineage
    source_file                   VARCHAR(256) NOT NULL,
    source_batch_id               VARCHAR(64)  NOT NULL,
    dv2_source_hk                 VARCHAR(64)  NOT NULL,
    quality_outcome               VARCHAR(16),
    CONSTRAINT pk_fact_allocation PRIMARY KEY NONCLUSTERED (allocation_event_sk) NOT ENFORCED,
    CONSTRAINT chk_fact_alloc_code CHECK (cat_event_code IN (
        'MEPA','MEAA'
    ))
);


-- ----------------------------------------------------------------------------
-- 3. fact_quotes  -- equity quote events (MENQ/MENQS/MERQ/MERQS/MEQR/MEQC/MEQM/MEQS)
--    Tier 15 (WS2 sub-tier 3)
-- ----------------------------------------------------------------------------

CREATE TABLE gold.fact_quotes (
    quote_event_sk                BIGINT IDENTITY(1,1) NOT NULL,
    event_dts                     DATETIME2(7) NOT NULL,
    event_date                    DATE NOT NULL,
    -- conformed dimension FKs
    date_sk                       BIGINT NOT NULL,
    instrument_sk                 BIGINT NOT NULL,
    party_sk                      BIGINT NOT NULL,
    venue_sk                      BIGINT,
    event_type_sk                 BIGINT NOT NULL,
    -- equity quote event specifics (CAT IM v4.1.0r15 section 4.10.x)
    cat_event_code                VARCHAR(8)  NOT NULL,
    quote_key_date                DATETIME2(7) NOT NULL,
    quote_id                      VARCHAR(64) NOT NULL,
    prior_quote_key_date          DATETIME2(7),
    prior_quote_id                VARCHAR(64),
    received_quote_id             VARCHAR(64),
    routed_quote_id               VARCHAR(64),
    bid_price                     DECIMAL(38, 18),
    bid_qty                       DECIMAL(38, 18),
    ask_price                     DECIMAL(38, 18),
    ask_qty                       DECIMAL(38, 18),
    bid_relative_price            VARCHAR(40),
    ask_relative_price            VARCHAR(40),
    valid_until_duration          DECIMAL(38, 18),
    only_one_quote_flag           BIT,
    unsolicited_ind               VARCHAR(4),
    unpriced_ind                  BIT,
    quote_wanted_ind              VARCHAR(4),
    representative_quote_ind      VARCHAR(4),
    ask_aggregated_orders         VARCHAR(4000),
    bid_aggregated_orders         VARCHAR(4000),
    quote_rejected_flag           BIT,
    sender_imid                   VARCHAR(8),
    destination                   VARCHAR(8),
    destination_type              VARCHAR(4),
    session                       VARCHAR(40),
    receiver_imid                 VARCHAR(8),
    initiator                     VARCHAR(4),
    mp_status_code                VARCHAR(4),
    dup_roid_cond                 BIT,
    rfq_id                        VARCHAR(64),
    originating_imid              VARCHAR(8),
    -- lineage
    source_file                   VARCHAR(256) NOT NULL,
    source_batch_id               VARCHAR(64)  NOT NULL,
    dv2_source_hk                 VARCHAR(64)  NOT NULL,
    quality_outcome               VARCHAR(16),
    CONSTRAINT pk_fact_quote PRIMARY KEY NONCLUSTERED (quote_event_sk) NOT ENFORCED,
    CONSTRAINT chk_fact_quote_code CHECK (cat_event_code IN (
        'MENQ','MENQS','MERQ','MERQS','MEQR','MEQC','MEQM','MEQS'
    ))
);


-- ----------------------------------------------------------------------------
-- 4. fact_order_events  -- equity order lifecycle events (24 codes)
--    Tier 16 (WS2 sub-tier 4 - final phantom-table burndown)
-- ----------------------------------------------------------------------------

CREATE TABLE gold.fact_order_events (
    order_event_sk                BIGINT IDENTITY(1,1) NOT NULL,
    event_dts                     DATETIME2(7) NOT NULL,
    event_date                    DATE NOT NULL,
    -- conformed dimension FKs
    date_sk                       BIGINT NOT NULL,
    instrument_sk                 BIGINT NOT NULL,
    party_sk                      BIGINT NOT NULL,
    venue_sk                      BIGINT,
    event_type_sk                 BIGINT NOT NULL,
    -- equity order event specifics (CAT IM v4.1.0r15 sections 4.1 - 4.9 + 4.14)
    action_type                   VARCHAR(8)   NOT NULL,
    error_roe_id                  VARCHAR(40),
    firm_roe_id                   VARCHAR(64)  NOT NULL,
    event_type_code               VARCHAR(8)   NOT NULL,
    cat_reporter_imid             VARCHAR(8),
    order_key_date                DATETIME2(7) NOT NULL,
    cat_order_id                  VARCHAR(64)  NOT NULL,
    symbol                        VARCHAR(14)  NOT NULL,
    event_timestamp               DATETIME2(7) NOT NULL,
    manual_flag                   BIT NOT NULL,
    electronic_dup_flag           BIT NOT NULL,
    electronic_timestamp          DATETIME2(7),
    manual_order_key_date         DATETIME2(7),
    manual_order_id               VARCHAR(64),
    dept_type                     VARCHAR(16),
    solicitation_flag             BIT,
    rfq_id                        VARCHAR(64),
    side                          VARCHAR(8),
    price                         DECIMAL(38, 18),
    quantity                      DECIMAL(38, 18),
    leaves_quantity               DECIMAL(38, 18),
    parent_order_id               VARCHAR(64),
    min_qty                       DECIMAL(38, 18),
    order_type                    VARCHAR(12),
    time_in_force                 VARCHAR(3),
    trading_session               VARCHAR(12),
    handling_instructions         VARCHAR(40),
    cust_dsp_intr_flag            BIT,
    firm_designated_id            VARCHAR(40),
    account_holder_type           VARCHAR(2),
    affiliate_flag                BIT,
    info_barrier_id               VARCHAR(20),
    negotiated_trade_flag         BIT,
    representative_ind            VARCHAR(4),
    seq_num                       VARCHAR(40),
    ats_display_ind               VARCHAR(4),
    display_price                 DECIMAL(38, 18),
    working_price                 DECIMAL(38, 18),
    display_qty                   DECIMAL(38, 18),
    nbb_price                     DECIMAL(38, 18),
    nbb_qty                       DECIMAL(38, 18),
    nbo_price                     DECIMAL(38, 18),
    nbo_qty                       DECIMAL(38, 18),
    nbbo_source                   VARCHAR(8),
    nbbo_timestamp                DATETIME2(7),
    net_price                     DECIMAL(38, 18),
    bfmm_flag                     BIT,
    originating_imid              VARCHAR(8),
    sender_imid                   VARCHAR(8),
    destination                   VARCHAR(8),
    destination_type              VARCHAR(4),
    routed_order_id               VARCHAR(64),
    session                       VARCHAR(40),
    iso_ind                       VARCHAR(4),
    route_rejected_flag           BIT,
    multi_leg_ind                 BIT,
    paired_order_id               VARCHAR(64),
    quote_key_date                DATETIME2(7),
    quote_id                      VARCHAR(64),
    -- lineage
    source_file                   VARCHAR(256) NOT NULL,
    source_batch_id               VARCHAR(64)  NOT NULL,
    dv2_source_hk                 VARCHAR(64)  NOT NULL,
    quality_outcome               VARCHAR(16),
    CONSTRAINT pk_fact_order_event PRIMARY KEY NONCLUSTERED (order_event_sk) NOT ENFORCED,
    CONSTRAINT chk_fact_order_code CHECK (event_type_code IN (
        'MENO','MENOS','MEOR','MEORS','MEMR','MEMRS','MECR','MECRS',
        'MEOA','MEIR','MEIM','MEIC','MEIMR','MEICR',
        'MECO','MECOM','MECOC','MEOM','MEOMS','MEOMR',
        'MEOJ','MEOC','MEOCR','MEOE'
    )),
    CONSTRAINT chk_fact_order_action CHECK (action_type IN ('NEW','FRC','RPR'))
);
