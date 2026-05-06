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
