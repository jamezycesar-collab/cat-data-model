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
