-- ============================================================================
-- Equity Gold Layer (Delta Lake)
-- ============================================================================
--
-- Tier 13 (WS2 burndown sub-tier 1): builds fact_execution_events.
--
-- This is the equity equivalent of fact_option_executions. Mirrors the
-- structure of ddl/option/02_option_gold_delta.sql so cross-dialect parity
-- and ETL patterns match across event families.
--
-- Spec coverage: section 4.11.1 (Trade Event MEOT) + 4.11.2 (Trade Event
-- Supplement MEOTS). Section 4.12 events (Order Fulfillment MEOF/MEOFS/MEFA)
-- land on this same table - tracked in known_uncovered_events.csv (F4.1)
-- pending dedicated field-table reconciliation.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. fact_execution_events  -- equity execution events (MEOT/MEOTS)
--    Section 4.11.1 (Trade) + 4.11.2 (Trade Supplement)
-- ----------------------------------------------------------------------------

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
    cat_order_id                STRING NOT NULL,        -- order this execution belongs to
    cat_event_code              STRING NOT NULL,
    trade_key_date              TIMESTAMP,              -- Section 4.11.1 row 6: tradeID assignment time
    trade_id                    STRING NOT NULL,        -- Section 4.11.1 row 7: unique fill ID per executing IM
    quantity                    DECIMAL(38, 18) NOT NULL,
    price                       DECIMAL(38, 18) NOT NULL,
    capacity                    STRING NOT NULL,        -- Section 4.11.1 row 18
    tape_trade_id               STRING,                 -- Section 4.11.1 row 19
    market_center_id            STRING NOT NULL,        -- Section 4.11.1 row 20
    side_details_ind            STRING,                 -- Section 4.11.1 row 21
    reporting_exception_code    STRING,                 -- Section 4.11.1 row 24
    clearing_firm               STRING,                 -- Section 4.11.1 row 33
    counterparty                STRING,                 -- Section 4.11.1 row 34
    cancel_flag                 BOOLEAN,                -- Section 4.11.1 row 12
    cancel_timestamp            TIMESTAMP,              -- Section 4.11.1 row 13
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT pk_fact_execution_event PRIMARY KEY (execution_event_sk),
    CONSTRAINT chk_fact_exec_code CHECK (cat_event_code IN (
        'MEOT','MEOTS'
    )),
    CONSTRAINT fk_fact_ee_date     FOREIGN KEY (date_sk)
        REFERENCES gold.dim_date (date_sk),
    CONSTRAINT fk_fact_ee_instr    FOREIGN KEY (instrument_sk)
        REFERENCES gold.dim_instrument (instrument_sk),
    CONSTRAINT fk_fact_ee_party    FOREIGN KEY (party_sk)
        REFERENCES gold.dim_party (party_sk),
    CONSTRAINT fk_fact_ee_evtype   FOREIGN KEY (event_type_sk)
        REFERENCES gold.dim_event_type (event_type_sk)
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'CAT equity execution event fact - one row per MEOT or MEOTS submission'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'compression.codec' = 'zstd',
    'subject_area' = 'gold_fact',
    'cat_submission_file_type' = 'OrderEvents',
    'grain' = 'one row per equity execution event'
);


-- ----------------------------------------------------------------------------
-- 2. fact_allocations  -- equity allocation events (MEPA/MEAA)
--    Section 4.13.1 (New Allocation) + 4.13.2 (Allocation Amendment)
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
    -- allocation specifics (CAT IM v4.1.0r15 section 4.13.1 / 4.13.2)
    cat_event_code              STRING NOT NULL,
    allocation_id               STRING NOT NULL,        -- Section 4.13.1 row 7
    allocation_key_date         TIMESTAMP NOT NULL,     -- Section 4.13.1 row 6
    prior_allocation_id         STRING,                 -- Section 4.13.2 row 9
    prior_allocation_key_date   TIMESTAMP,              -- Section 4.13.2 row 8
    side                        STRING NOT NULL,        -- Section 4.13.1 row 14
    quantity                    DECIMAL(38, 18) NOT NULL, -- Section 4.13.1 row 12
    price                       DECIMAL(38, 18),        -- Section 4.13.1 row 13
    firm_designated_id          STRING NOT NULL,        -- Section 4.13.1 row 15: receiving FDID
    new_order_fdid              STRING,                 -- Section 4.13.1 row 23
    correspondent_crd           STRING,                 -- Section 4.13.1 row 22
    trade_date                  DATE,                   -- Section 4.13.1 row 18
    settlement_date             DATE,                   -- Section 4.13.1 row 19
    allocation_type             STRING,                 -- Section 4.13.1 row 20
    dvp_custodian_id            STRING,                 -- Section 4.13.1 row 21
    institution_flag            BOOLEAN,                -- Section 4.13.1 row 17
    account_holder_type         STRING,                 -- Section 4.13.1 row 26
    allocation_instruction_time TIMESTAMP,              -- Section 4.13.1 row 24
    cancel_flag                 BOOLEAN,                -- Section 4.13.1 row 10
    cancel_timestamp            TIMESTAMP,              -- Section 4.13.1 row 11
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT pk_fact_allocation PRIMARY KEY (allocation_event_sk),
    CONSTRAINT chk_fact_alloc_code CHECK (cat_event_code IN (
        'MEPA','MEAA'
    )),
    CONSTRAINT fk_fact_al_date     FOREIGN KEY (date_sk)
        REFERENCES gold.dim_date (date_sk),
    CONSTRAINT fk_fact_al_instr    FOREIGN KEY (instrument_sk)
        REFERENCES gold.dim_instrument (instrument_sk),
    CONSTRAINT fk_fact_al_party    FOREIGN KEY (party_sk)
        REFERENCES gold.dim_party (party_sk),
    CONSTRAINT fk_fact_al_evtype   FOREIGN KEY (event_type_sk)
        REFERENCES gold.dim_event_type (event_type_sk)
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'CAT equity allocation event fact - one row per MEPA or MEAA submission'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'compression.codec' = 'zstd',
    'subject_area' = 'gold_fact',
    'cat_submission_file_type' = 'Allocations',
    'grain' = 'one row per equity allocation event'
);


-- ----------------------------------------------------------------------------
-- 3. fact_quotes  -- equity quote events (MENQ/MENQS/MERQ/MERQS/MEQR/MEQC/MEQM/MEQS)
--    Section 4.10.1 - 4.10.8 (New Quote / Routed Quote / Quote Received /
--    Quote Cancelled / Quote Modified / Quote Status, plus their supplements)
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
    quote_key_date              TIMESTAMP NOT NULL,       -- Section 4.10.1 row 6
    quote_id                    STRING NOT NULL,          -- Section 4.10.1 row 7
    prior_quote_key_date        TIMESTAMP,                -- Section 4.10.7 row 9 (MEQM)
    prior_quote_id              STRING,                   -- Section 4.10.7 row 10 (MEQM)
    received_quote_id           STRING,                   -- Section 4.10.5 row 9  (MEQR)
    routed_quote_id             STRING,                   -- Section 4.10.3 row 13 (MERQ/S)
    bid_price                   DECIMAL(38, 18),          -- Section 4.10.1 row 17
    bid_qty                     DECIMAL(38, 18),          -- Section 4.10.1 row 18
    ask_price                   DECIMAL(38, 18),          -- Section 4.10.1 row 19
    ask_qty                     DECIMAL(38, 18),          -- Section 4.10.1 row 20
    bid_relative_price          STRING,                   -- Section 4.10.3 (RFQ-only)
    ask_relative_price          STRING,                   -- Section 4.10.3 (RFQ-only)
    valid_until_duration        DECIMAL(38, 18),          -- Section 4.10.3 (RFQ-only seconds)
    only_one_quote_flag         BOOLEAN,                  -- Section 4.10.1 row 16
    unsolicited_ind             STRING,                   -- Section 4.10.1 row 23
    unpriced_ind                BOOLEAN,                  -- Section 4.10.1 row 26
    quote_wanted_ind            STRING,                   -- Section 4.10.5 row 23 (IDQS)
    representative_quote_ind    STRING,                   -- Section 4.10.1 row 29 (ADF)
    ask_aggregated_orders       STRING,                   -- Section 4.10.1 row 30 (ADF)
    bid_aggregated_orders       STRING,                   -- Section 4.10.1 row 31 (ADF)
    quote_rejected_flag         BOOLEAN,                  -- Section 4.10.3 row 18
    sender_imid                 STRING,                   -- Section 4.10.3 row 11
    destination                 STRING,                   -- Section 4.10.3 row 12
    destination_type            STRING,                   -- Section 4.10.3 row 23
    session                     STRING,                   -- Section 4.10.3 row 24
    receiver_imid               STRING,                   -- Section 4.10.5 row 12
    initiator                   STRING,                   -- Section 4.10.6 row 13 (MEQC)
    mp_status_code              STRING,                   -- Section 4.10.8 row 12 (MEQS)
    dup_roid_cond               BOOLEAN,                  -- Section 4.10.3 row 20 / 4.10.5 row 25
    rfq_id                      STRING,                   -- Section 4.10.1 row 32
    originating_imid            STRING,                   -- Section 4.10.6 row 9
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT pk_fact_quote PRIMARY KEY (quote_event_sk),
    CONSTRAINT chk_fact_quote_code CHECK (cat_event_code IN (
        'MENQ','MENQS','MERQ','MERQS','MEQR','MEQC','MEQM','MEQS'
    )),
    CONSTRAINT fk_fact_q_date      FOREIGN KEY (date_sk)
        REFERENCES gold.dim_date (date_sk),
    CONSTRAINT fk_fact_q_instr     FOREIGN KEY (instrument_sk)
        REFERENCES gold.dim_instrument (instrument_sk),
    CONSTRAINT fk_fact_q_party     FOREIGN KEY (party_sk)
        REFERENCES gold.dim_party (party_sk),
    CONSTRAINT fk_fact_q_evtype    FOREIGN KEY (event_type_sk)
        REFERENCES gold.dim_event_type (event_type_sk)
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'CAT equity quote event fact - one row per MENQ/MENQS/MERQ/MERQS/MEQR/MEQC/MEQM/MEQS submission'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'compression.codec' = 'zstd',
    'subject_area' = 'gold_fact',
    'cat_submission_file_type' = 'OrderEvents',
    'grain' = 'one row per equity quote event'
);


-- ----------------------------------------------------------------------------
-- 4. fact_order_events  -- equity order lifecycle events
--    CAT IM v4.1.0r15 sections 4.1 - 4.9 + 4.14 (24 distinct event codes)
--    Tier 16 (WS2 sub-tier 4 - final phantom-table burndown)
--
--    Covers New Order, Order Route, Route Modified, Route Cancelled,
--    Order Accepted, Internal Route family, Child Order family,
--    Order Modified, Order Adjusted, Order Cancelled, Order Effective.
--    Trade events (MEOT/MEOTS) intentionally excluded - they live on
--    fact_execution_events.
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
    action_type                 STRING NOT NULL,          -- Section 4.1 row 1 (NEW/FRC/RPR)
    error_roe_id                STRING,                   -- Section 4.1 row 2 (required when RPR)
    firm_roe_id                 STRING NOT NULL,          -- Section 2.3.3
    event_type_code             STRING NOT NULL,          -- Section 6.1.5: message type code (CHECK)
    cat_reporter_imid           STRING,                   -- Section 2.4.1
    order_key_date              TIMESTAMP NOT NULL,       -- Section 4.1 row 6
    cat_order_id                STRING NOT NULL,          -- Section 4.1 row 7
    symbol                      STRING NOT NULL,          -- Section 2.4.3
    event_timestamp             TIMESTAMP NOT NULL,       -- Section 4.1 row 9
    manual_flag                 BOOLEAN NOT NULL,         -- Section 4.1 row 10
    electronic_dup_flag         BOOLEAN NOT NULL,         -- Section 4.1 row 11
    electronic_timestamp        TIMESTAMP,                -- Section 4.1 row 12
    manual_order_key_date       TIMESTAMP,                -- Section 4.1 row 13
    manual_order_id             STRING,                   -- Section 4.1 row 14
    dept_type                   STRING,                   -- Section 4.1 row 15 (MENO/MEOM)
    solicitation_flag           BOOLEAN,                  -- Section 4.1 row 16 (MENO)
    rfq_id                      STRING,                   -- Section 4.1 row 17 (MENO)
    side                        STRING,                   -- Section 4.1 row 18
    price                       DECIMAL(38, 18),          -- Section 4.1 row 19
    quantity                    DECIMAL(38, 18),          -- Section 4.1 row 20
    leaves_quantity             DECIMAL(38, 18),          -- Section 4.1 / 4.7 / 4.11.1
    parent_order_id             STRING,                   -- Section 4.1 parent linkage
    min_qty                     DECIMAL(38, 18),          -- Section 4.1 row 21
    order_type                  STRING,                   -- Section 4.1 row 22
    time_in_force               STRING,                   -- Section 4.1 row 23
    trading_session             STRING,                   -- Section 4.1 row 24
    handling_instructions       STRING,                   -- Section 4.1 row 25 / 4.3 row 28
    cust_dsp_intr_flag          BOOLEAN,                  -- Section 4.1 row 26
    firm_designated_id          STRING,                   -- Section 2.4.2 / Appendix G
    account_holder_type         STRING,                   -- Section 4.1 row 28
    affiliate_flag              BOOLEAN,                  -- Section 4.1 row 29 / 4.3 row 26
    info_barrier_id             STRING,                   -- Section 4.1 row 30
    negotiated_trade_flag       BOOLEAN,                  -- Section 4.1 row 32
    representative_ind          STRING,                   -- Section 4.1 row 33
    seq_num                     STRING,                   -- Section 4.1 row 34
    ats_display_ind             STRING,                   -- Section 4.1 row 35
    display_price               DECIMAL(38, 18),          -- Section 4.1 row 36
    working_price               DECIMAL(38, 18),          -- Section 4.1 row 37
    display_qty                 DECIMAL(38, 18),          -- Section 4.1 row 38
    nbb_price                   DECIMAL(38, 18),          -- Section 4.1 row 40
    nbb_qty                     DECIMAL(38, 18),          -- Section 4.1 row 41
    nbo_price                   DECIMAL(38, 18),          -- Section 4.1 row 42
    nbo_qty                     DECIMAL(38, 18),          -- Section 4.1 row 43
    nbbo_source                 STRING,                   -- Section 4.1 row 44
    nbbo_timestamp              TIMESTAMP,                -- Section 4.1 row 45
    net_price                   DECIMAL(38, 18),          -- Section 4.1 row 46
    bfmm_flag                   BOOLEAN,                  -- Section 4.1 row 47 (short-sale BFMM)
    originating_imid            STRING,                   -- Section 4.3 row 9
    sender_imid                 STRING,                   -- Section 4.3 row 14
    destination                 STRING,                   -- Section 4.3 row 15
    destination_type            STRING,                   -- Section 4.3 row 16
    routed_order_id             STRING,                   -- Section 4.3 row 17
    session                     STRING,                   -- Section 4.3 row 18
    iso_ind                     STRING,                   -- Section 4.3 row 27 (ISO)
    route_rejected_flag         BOOLEAN,                  -- Section 4.3 row 29
    multi_leg_ind               BOOLEAN,                  -- Section 4.3 row 32 / 4.11.1 row 35
    paired_order_id             STRING,                   -- Section 4.3 row 33
    quote_key_date              TIMESTAMP,                -- Section 4.3 row 36 (RFQ response)
    quote_id                    STRING,                   -- Section 4.3 row 37 (RFQ response)
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT pk_fact_order_event PRIMARY KEY (order_event_sk),
    CONSTRAINT chk_fact_order_code CHECK (event_type_code IN (
        'MENO','MENOS','MEOR','MEORS','MEMR','MEMRS','MECR','MECRS',
        'MEOA','MEIR','MEIM','MEIC','MEIMR','MEICR',
        'MECO','MECOM','MECOC','MEOM','MEOMS','MEOMR',
        'MEOJ','MEOC','MEOCR','MEOE'
    )),
    CONSTRAINT chk_fact_order_action CHECK (action_type IN ('NEW','FRC','RPR')),
    CONSTRAINT fk_fact_oe_date     FOREIGN KEY (date_sk)
        REFERENCES gold.dim_date (date_sk),
    CONSTRAINT fk_fact_oe_instr    FOREIGN KEY (instrument_sk)
        REFERENCES gold.dim_instrument (instrument_sk),
    CONSTRAINT fk_fact_oe_party    FOREIGN KEY (party_sk)
        REFERENCES gold.dim_party (party_sk),
    CONSTRAINT fk_fact_oe_evtype   FOREIGN KEY (event_type_sk)
        REFERENCES gold.dim_event_type (event_type_sk)
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'CAT equity order event fact - one row per equity order lifecycle event (24 distinct codes from sections 4.1-4.9 + 4.14, excluding trades MEOT/MEOTS)'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'compression.codec' = 'zstd',
    'subject_area' = 'gold_fact',
    'cat_submission_file_type' = 'OrderEvents',
    'grain' = 'one row per equity order lifecycle event'
);
