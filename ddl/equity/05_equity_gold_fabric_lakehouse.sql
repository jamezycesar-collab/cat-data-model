-- ============================================================================
-- Equity Gold Layer (Microsoft Fabric Lakehouse)
-- ============================================================================
--
-- Tier 13 (WS2 burndown sub-tier 1): builds fact_execution_events.
--
-- Fabric Lakehouse uses Delta under the hood with Spark SQL DDL syntax, so
-- this is structurally identical to the Delta variant. Distinguished by
-- TBLPROPERTIES being slightly more conservative (no autoOptimize - Fabric
-- manages that via workspace settings).
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.fact_execution_events (
    execution_event_sk          BIGINT GENERATED ALWAYS AS IDENTITY,
    event_dts                   TIMESTAMP NOT NULL,
    event_date                  DATE      NOT NULL,
    -- conformed dimension FKs
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT NOT NULL,
    party_sk                    BIGINT NOT NULL,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT NOT NULL,
    -- equity execution event specifics (CAT IM v4.1.0r15 section 4.11.1)
    cat_order_id                STRING NOT NULL,
    cat_event_code              STRING NOT NULL,
    trade_key_date              TIMESTAMP,
    trade_id                    STRING NOT NULL,
    quantity                    DECIMAL(38, 18) NOT NULL,
    price                       DECIMAL(38, 18) NOT NULL,
    capacity                    STRING NOT NULL,
    tape_trade_id               STRING,
    market_center_id            STRING NOT NULL,
    side_details_ind            STRING,
    reporting_exception_code    STRING,
    clearing_firm               STRING,
    counterparty                STRING,
    cancel_flag                 BOOLEAN,
    cancel_timestamp            TIMESTAMP,
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT chk_fact_exec_code CHECK (cat_event_code IN (
        'MEOT','MEOTS'
    ))
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'CAT equity execution event fact - one row per MEOT or MEOTS submission';


-- ----------------------------------------------------------------------------
-- 2. fact_allocations  -- equity allocation events (MEPA/MEAA)
--    Tier 14 (WS2 sub-tier 2)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.fact_allocations (
    allocation_event_sk         BIGINT GENERATED ALWAYS AS IDENTITY,
    event_dts                   TIMESTAMP NOT NULL,
    event_date                  DATE      NOT NULL,
    -- conformed dimension FKs
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT NOT NULL,
    party_sk                    BIGINT NOT NULL,
    account_sk                  BIGINT,
    event_type_sk               BIGINT NOT NULL,
    -- allocation specifics
    cat_event_code              STRING NOT NULL,
    allocation_id               STRING NOT NULL,
    allocation_key_date         TIMESTAMP NOT NULL,
    prior_allocation_id         STRING,
    prior_allocation_key_date   TIMESTAMP,
    side                        STRING NOT NULL,
    quantity                    DECIMAL(38, 18) NOT NULL,
    price                       DECIMAL(38, 18),
    firm_designated_id          STRING NOT NULL,
    new_order_fdid              STRING,
    correspondent_crd           STRING,
    trade_date                  DATE,
    settlement_date             DATE,
    allocation_type             STRING,
    dvp_custodian_id            STRING,
    institution_flag            BOOLEAN,
    account_holder_type         STRING,
    allocation_instruction_time TIMESTAMP,
    cancel_flag                 BOOLEAN,
    cancel_timestamp            TIMESTAMP,
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT chk_fact_alloc_code CHECK (cat_event_code IN (
        'MEPA','MEAA'
    ))
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'CAT equity allocation event fact - one row per MEPA or MEAA submission';


-- ----------------------------------------------------------------------------
-- 3. fact_quotes  -- equity quote events (MENQ/MENQS/MERQ/MERQS/MEQR/MEQC/MEQM/MEQS)
--    Tier 15 (WS2 sub-tier 3)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.fact_quotes (
    quote_event_sk              BIGINT GENERATED ALWAYS AS IDENTITY,
    event_dts                   TIMESTAMP NOT NULL,
    event_date                  DATE      NOT NULL,
    -- conformed dimension FKs
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT NOT NULL,
    party_sk                    BIGINT NOT NULL,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT NOT NULL,
    -- equity quote event specifics (CAT IM v4.1.0r15 section 4.10.x)
    cat_event_code              STRING NOT NULL,
    quote_key_date              TIMESTAMP NOT NULL,
    quote_id                    STRING NOT NULL,
    prior_quote_key_date        TIMESTAMP,
    prior_quote_id              STRING,
    received_quote_id           STRING,
    routed_quote_id             STRING,
    bid_price                   DECIMAL(38, 18),
    bid_qty                     DECIMAL(38, 18),
    ask_price                   DECIMAL(38, 18),
    ask_qty                     DECIMAL(38, 18),
    bid_relative_price          STRING,
    ask_relative_price          STRING,
    valid_until_duration        DECIMAL(38, 18),
    only_one_quote_flag         BOOLEAN,
    unsolicited_ind             STRING,
    unpriced_ind                BOOLEAN,
    quote_wanted_ind            STRING,
    representative_quote_ind    STRING,
    ask_aggregated_orders       STRING,
    bid_aggregated_orders       STRING,
    quote_rejected_flag         BOOLEAN,
    sender_imid                 STRING,
    destination                 STRING,
    destination_type            STRING,
    session                     STRING,
    receiver_imid               STRING,
    initiator                   STRING,
    mp_status_code              STRING,
    dup_roid_cond               BOOLEAN,
    rfq_id                      STRING,
    originating_imid            STRING,
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT chk_fact_quote_code CHECK (cat_event_code IN (
        'MENQ','MENQS','MERQ','MERQS','MEQR','MEQC','MEQM','MEQS'
    ))
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'CAT equity quote event fact - one row per MENQ/MENQS/MERQ/MERQS/MEQR/MEQC/MEQM/MEQS submission';


-- ----------------------------------------------------------------------------
-- 4. fact_order_events  -- equity order lifecycle events (24 codes)
--    Tier 16 (WS2 sub-tier 4 - final phantom-table burndown)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.fact_order_events (
    order_event_sk              BIGINT GENERATED ALWAYS AS IDENTITY,
    event_dts                   TIMESTAMP NOT NULL,
    event_date                  DATE      NOT NULL,
    -- conformed dimension FKs
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT NOT NULL,
    party_sk                    BIGINT NOT NULL,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT NOT NULL,
    -- equity order event specifics (CAT IM v4.1.0r15 sections 4.1 - 4.9 + 4.14)
    action_type                 STRING NOT NULL,
    error_roe_id                STRING,
    firm_roe_id                 STRING NOT NULL,
    event_type_code             STRING NOT NULL,
    cat_reporter_imid           STRING,
    order_key_date              TIMESTAMP NOT NULL,
    cat_order_id                STRING NOT NULL,
    symbol                      STRING NOT NULL,
    event_timestamp             TIMESTAMP NOT NULL,
    manual_flag                 BOOLEAN NOT NULL,
    electronic_dup_flag         BOOLEAN NOT NULL,
    electronic_timestamp        TIMESTAMP,
    manual_order_key_date       TIMESTAMP,
    manual_order_id             STRING,
    dept_type                   STRING,
    solicitation_flag           BOOLEAN,
    rfq_id                      STRING,
    side                        STRING,
    price                       DECIMAL(38, 18),
    quantity                    DECIMAL(38, 18),
    leaves_quantity             DECIMAL(38, 18),
    parent_order_id             STRING,
    min_qty                     DECIMAL(38, 18),
    order_type                  STRING,
    time_in_force               STRING,
    trading_session             STRING,
    handling_instructions       STRING,
    cust_dsp_intr_flag          BOOLEAN,
    firm_designated_id          STRING,
    account_holder_type         STRING,
    affiliate_flag              BOOLEAN,
    info_barrier_id             STRING,
    negotiated_trade_flag       BOOLEAN,
    representative_ind          STRING,
    seq_num                     STRING,
    ats_display_ind             STRING,
    display_price               DECIMAL(38, 18),
    working_price               DECIMAL(38, 18),
    display_qty                 DECIMAL(38, 18),
    nbb_price                   DECIMAL(38, 18),
    nbb_qty                     DECIMAL(38, 18),
    nbo_price                   DECIMAL(38, 18),
    nbo_qty                     DECIMAL(38, 18),
    nbbo_source                 STRING,
    nbbo_timestamp              TIMESTAMP,
    net_price                   DECIMAL(38, 18),
    bfmm_flag                   BOOLEAN,
    originating_imid            STRING,
    sender_imid                 STRING,
    destination                 STRING,
    destination_type            STRING,
    routed_order_id             STRING,
    session                     STRING,
    iso_ind                     STRING,
    route_rejected_flag         BOOLEAN,
    multi_leg_ind               BOOLEAN,
    paired_order_id             STRING,
    quote_key_date              TIMESTAMP,
    quote_id                    STRING,
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT chk_fact_order_code CHECK (event_type_code IN (
        'MENO','MENOS','MEOR','MEORS','MEMR','MEMRS','MECR','MECRS',
        'MEOA','MEIR','MEIM','MEIC','MEIMR','MEICR',
        'MECO','MECOM','MECOC','MEOM','MEOMS','MEOMR',
        'MEOJ','MEOC','MEOCR','MEOE'
    )),
    CONSTRAINT chk_fact_order_action CHECK (action_type IN ('NEW','FRC','RPR'))
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'CAT equity order event fact - one row per equity order lifecycle event (24 distinct codes from sections 4.1-4.9 + 4.14, excluding trades MEOT/MEOTS)';
