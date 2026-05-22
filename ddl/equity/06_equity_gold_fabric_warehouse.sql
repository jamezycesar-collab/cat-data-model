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
