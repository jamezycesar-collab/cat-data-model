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
