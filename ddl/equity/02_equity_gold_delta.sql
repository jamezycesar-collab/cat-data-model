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
