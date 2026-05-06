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
