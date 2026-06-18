-- Simple-option Gold-layer DDL (Apache Hive).

CREATE TABLE IF NOT EXISTS gold.fact_option_order_events (
    option_event_sk             BIGINT,
    event_dts                   TIMESTAMP,
    date_sk                     BIGINT,
    instrument_sk               BIGINT,
    party_sk                    BIGINT,
    trader_sk                   BIGINT,
    desk_sk                     BIGINT,
    account_sk                  BIGINT,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT,
    option_id                   STRING,
    cat_order_id                STRING,
    cat_event_code              STRING COMMENT 'CHECK in Section 5.1 order/quote/lifecycle codes',
    side                        STRING,
    price                       DECIMAL(38, 18),
    quantity                    DECIMAL(38, 18),
    leaves_qty                  DECIMAL(38, 18),
    cancel_qty                  DECIMAL(38, 18),
    order_type                  STRING,
    time_in_force               STRING,
    trading_session             STRING,
    open_close_indicator        STRING,
    handling_instructions       STRING,
    net_price                   DECIMAL(38, 18),
    parent_order_id             STRING,
    prior_order_id              STRING,
    initiator                   STRING,
    trigger_price               DECIMAL(38, 18),
    -- CAT IM v4.1.0r15 spec-mapping columns added in Tier 17.4
    order_key_date              TIMESTAMP,        -- section 5.1.1 row 6 orderKeyDate
    event_timestamp             TIMESTAMP,        -- section 5.1.1 row 9 eventTimestamp
    manual_flag                 BOOLEAN,          -- section 5.1.1 row 10 manualFlag
    manual_order_key_date       TIMESTAMP,        -- section 5.1.1 row 11 manualOrderKeyDate
    manual_order_id             STRING,           -- section 5.1.1 row 12 manualOrderID
    electronic_dup_flag         BOOLEAN,          -- section 5.1.1 row 13 electronicDupFlag
    electronic_timestamp        TIMESTAMP,        -- section 5.1.1 row 14 electronicTimestamp
    dept_type                   STRING,           -- section 5.1.1 row 15 deptType
    min_qty                     DECIMAL(38, 18),  -- section 5.1.1 row 19 minQty
    solicitation_flag           BOOLEAN,          -- section 5.1.1 row 28 solicitationFlag
    rfq_id                      STRING,           -- section 5.1.1 row 32 RFQID
    representative_ind          STRING,           -- section 5.1.1 row 30 representativeInd
    exch_origin_code            STRING,           -- section 5.1.3 row 28 exchOriginCode
    firm_designated_id          STRING,           -- section 5.1.1 row 24 firmDesignatedID
    account_holder_type         STRING,           -- section 5.1.1 row 25 accountHolderType
    affiliate_flag              BOOLEAN,          -- section 5.1.1 row 26 affiliateFlag
    sender_imid                 STRING,           -- section 5.1.3 row 14 senderIMID
    receiver_imid               STRING,           -- section 5.1.4 row 15 receiverIMID
    sender_type                 STRING,           -- section 5.1.4 row 17 senderType
    originating_imid            STRING,           -- section 5.1.3 row 9 originatingIMID
    destination                 STRING,           -- section 5.1.3 row 15 destination
    destination_type            STRING,           -- section 5.1.3 row 16 destinationType
    routed_order_id             STRING,           -- section 5.1.3 row 17 routedOrderID
    session                     STRING,           -- section 5.1.3 row 18 session
    route_rejected_flag         BOOLEAN,          -- section 5.1.3 row 27 routeRejectedFlag
    multi_leg_ind               BOOLEAN,          -- section 5.1.3 row 30 multiLegInd
    paired_order_id             STRING,           -- section 5.1.3 row 34 pairedOrderID
    prior_order_key_date        TIMESTAMP,        -- section 5.1.7 row 9 priorOrderKeyDate
    parent_order_key_date       TIMESTAMP,        -- section 5.1.6.1 row 9 parentOrderKeyDate
    request_timestamp           TIMESTAMP,        -- section 5.1.7 row 33 requestTimestamp
    source_file                 STRING,
    source_batch_id             STRING,
    dv2_source_hk               STRING,
    quality_outcome             STRING
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS gold.fact_option_executions (
    option_execution_sk         BIGINT,
    event_dts                   TIMESTAMP,
    date_sk                     BIGINT,
    instrument_sk               BIGINT,
    party_sk                    BIGINT,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT,
    option_id                   STRING,
    cat_event_code              STRING COMMENT 'CHECK in MOOT,MOOF,MOOFS,MOFA',
    trade_id                    STRING,
    trade_key_date              TIMESTAMP COMMENT 'CAT IM v4.1.0r15 section 5.1.11 row 6 (MOOT)',
    fulfillment_id              STRING,
    fill_key_date               TIMESTAMP COMMENT 'CAT IM v4.1.0r15 section 5.1.12.1 row 6 (MOOF/MOOFS)',
    prior_fulfillment_id        STRING,
    prior_fill_key_date         TIMESTAMP COMMENT 'CAT IM v4.1.0r15 section 5.1.12.3 row 9 (MOFA)',
    quantity                    DECIMAL(38, 18),
    price                       DECIMAL(38, 18),
    capacity                    STRING,
    tape_trade_id               STRING,
    market_center_id            STRING,
    side_details_ind            STRING,
    clearing_firm               STRING,
    fulfillment_link_type       STRING,
    cancel_flag                 BOOLEAN,
    cancel_timestamp            TIMESTAMP,
    multi_leg_ind               BOOLEAN,
    source_file                 STRING,
    source_batch_id             STRING,
    dv2_source_hk               STRING,
    quality_outcome             STRING
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS gold.fact_option_allocations (
    option_allocation_sk        BIGINT,
    event_dts                   TIMESTAMP,
    date_sk                     BIGINT,
    instrument_sk               BIGINT,
    party_sk                    BIGINT,
    account_sk                  BIGINT,
    event_type_sk               BIGINT,
    option_id                   STRING,
    cat_event_code              STRING COMMENT 'CHECK in MOPA,MOAA',
    allocation_id               STRING,
    allocation_key_date         TIMESTAMP COMMENT 'CAT IM v4.1.0r15 section 5.1.13.1 row 6 (MOPA/MOAA)',
    prior_allocation_id         STRING,
    prior_allocation_key_date   TIMESTAMP COMMENT 'CAT IM v4.1.0r15 section 5.1.13.2 row 8 (MOAA)',
    side                        STRING,
    quantity                    DECIMAL(38, 18),
    price                       DECIMAL(38, 18),
    firm_designated_id          STRING,
    new_order_fdid              STRING,
    correspondent_crd           BIGINT,
    occ_clearing_member_id      STRING,
    trade_date                  DATE,
    settlement_date             DATE,
    allocation_type             STRING,
    institution_flag            BOOLEAN,
    account_holder_type         STRING,
    allocation_instruction_time TIMESTAMP COMMENT 'CAT IM v4.1.0r15 section 5.1.13.1 row 24 (MOPA/MOAA)',
    cancel_flag                 BOOLEAN,
    cancel_timestamp            TIMESTAMP,
    source_file                 STRING,
    source_batch_id             STRING,
    dv2_source_hk               STRING,
    quality_outcome             STRING
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET;
