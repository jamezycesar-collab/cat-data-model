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


-- ----------------------------------------------------------------------------
-- 2. fact_allocations  -- equity allocation events (MEPA/MEAA)
--    Tier 14 (WS2 sub-tier 2)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.fact_allocations (
    allocation_event_sk         BIGINT,
    event_dts                   TIMESTAMP,
    -- conformed dimension FKs
    date_sk                     BIGINT,
    instrument_sk               BIGINT,
    party_sk                    BIGINT,
    account_sk                  BIGINT,
    event_type_sk               BIGINT,
    -- allocation specifics (CAT IM v4.1.0r15 section 4.13.1 / 4.13.2)
    cat_event_code              STRING COMMENT 'CHECK in (MEPA, MEAA)',
    allocation_id               STRING,
    allocation_key_date         TIMESTAMP,
    prior_allocation_id         STRING,
    prior_allocation_key_date   TIMESTAMP,
    side                        STRING,
    quantity                    DECIMAL(38, 18),
    price                       DECIMAL(38, 18),
    firm_designated_id          STRING,
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
    source_file                 STRING,
    source_batch_id             STRING,
    dv2_source_hk               STRING,
    quality_outcome             STRING
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('comment' = 'Gold fact: one row per equity allocation event (MEPA/MEAA).');


-- ----------------------------------------------------------------------------
-- 3. fact_quotes  -- equity quote events (MENQ/MENQS/MERQ/MERQS/MEQR/MEQC/MEQM/MEQS)
--    Tier 15 (WS2 sub-tier 3)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.fact_quotes (
    quote_event_sk              BIGINT,
    event_dts                   TIMESTAMP,
    -- conformed dimension FKs
    date_sk                     BIGINT,
    instrument_sk               BIGINT,
    party_sk                    BIGINT,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT,
    -- equity quote event specifics (CAT IM v4.1.0r15 section 4.10.x)
    cat_event_code              STRING COMMENT 'CHECK in (MENQ, MENQS, MERQ, MERQS, MEQR, MEQC, MEQM, MEQS)',
    quote_key_date              TIMESTAMP,
    quote_id                    STRING,
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
    source_file                 STRING,
    source_batch_id             STRING,
    dv2_source_hk               STRING,
    quality_outcome             STRING
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('comment' = 'Gold fact: one row per equity quote event (MENQ/MENQS/MERQ/MERQS/MEQR/MEQC/MEQM/MEQS).');
