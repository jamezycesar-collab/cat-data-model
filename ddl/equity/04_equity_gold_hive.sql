-- ============================================================================
-- Equity Gold Layer (Apache Hive)
-- ============================================================================
--
-- Tier 13 (WS2 burndown sub-tier 1): builds fact_execution_events.
--
-- Hive does not enforce PRIMARY KEY / FOREIGN KEY / CHECK constraints; they
-- are documented as comments only (matching the pattern in
-- ddl/option/04_option_gold_hive.sql and ddl/cais/06_cais_gold_hive.sql).
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.fact_execution_events (
    execution_event_sk          BIGINT,
    event_dts                   TIMESTAMP,
    -- conformed dimension FKs
    date_sk                     BIGINT,
    instrument_sk               BIGINT,
    party_sk                    BIGINT,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT,
    -- equity execution event specifics (CAT IM v4.1.0r15 section 4.11.1)
    cat_order_id                STRING,
    cat_event_code              STRING COMMENT 'CHECK in (MEOT, MEOTS)',
    trade_key_date              TIMESTAMP,
    trade_id                    STRING,
    quantity                    DECIMAL(38, 18),
    price                       DECIMAL(38, 18),
    capacity                    STRING,
    tape_trade_id               STRING,
    market_center_id            STRING,
    side_details_ind            STRING,
    reporting_exception_code    STRING,
    clearing_firm               STRING,
    counterparty                STRING,
    cancel_flag                 BOOLEAN,
    cancel_timestamp            TIMESTAMP,
    -- lineage
    source_file                 STRING,
    source_batch_id             STRING,
    dv2_source_hk               STRING,
    quality_outcome             STRING
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('comment' = 'Gold fact: one row per equity execution event (MEOT/MEOTS).');
