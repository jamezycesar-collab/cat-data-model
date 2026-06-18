-- Simple-option Silver-layer DDL (Microsoft Fabric Lakehouse).
-- Same Spark-SQL-on-Delta as the Databricks variant.

CREATE TABLE IF NOT EXISTS silver.hub_option_order (
    option_order_hk             STRING NOT NULL,
    option_order_bk_orderID     STRING NOT NULL,
    option_order_bk_keyDate     DATE   NOT NULL,
    option_order_bk_imid        STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL
)
USING DELTA
PARTITIONED BY (option_order_bk_keyDate)
COMMENT 'DV2 Hub: Simple option orders (Fabric Lakehouse).';

CREATE TABLE IF NOT EXISTS silver.hub_option_instrument (
    option_instrument_hk        STRING NOT NULL,
    option_id_bk                STRING NOT NULL,
    is_flex                     BOOLEAN NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver.link_option_event (
    option_event_hk             STRING NOT NULL,
    option_order_hk             STRING NOT NULL,
    option_instrument_hk        STRING NOT NULL,
    event_code                  STRING NOT NULL,
    event_timestamp             TIMESTAMP NOT NULL,
    event_date                  DATE NOT NULL,
    event_type_hk               STRING NOT NULL,
    venue_hk                    STRING,
    sender_party_hk             STRING,
    receiver_party_hk           STRING,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT chk_link_oe_code CHECK (event_code IN (
        'MONO','MONOS','MOOR','MOMR','MOCR','MOORS','MOMRS','MOCRS','MOOA',
        'MOIR','MOIM','MOIC','MOIMR','MOICR',
        'MOCO','MOCOM','MOCOC',
        'MOOM','MOOMS','MOOMR','MOOJ','MOOC','MOOCR',
        'MONQ','MORQ','MOQR','MOQC','MOQM',
        'MOOT','MOOF','MOOFS','MOFA','MOPA','MOAA','MOOE'
    ))
)
USING DELTA
PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS silver.link_option_child_order (
    option_child_order_hk       STRING NOT NULL,
    parent_option_order_hk      STRING NOT NULL,
    child_option_order_hk       STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver.sat_option_order_state (
    option_order_hk             STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING NOT NULL,
    side                        STRING,
    order_status                STRING,
    order_quantity              DECIMAL(38, 18),
    leaves_quantity             DECIMAL(38, 18),
    limit_price                 DECIMAL(38, 18),
    net_price                   DECIMAL(38, 18),
    time_in_force               STRING,
    trading_session             STRING,
    handling_instructions       STRING,
    open_close_indicator        STRING,
    representative_ind          STRING,
    solicitation_flag           BOOLEAN,
    affiliate_flag              BOOLEAN,
    rfq_id                      STRING,
    record_source               STRING NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver.sat_option_event_state (
    option_event_hk             STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING NOT NULL,
    event_qty                   DECIMAL(38, 18),
    event_price                 DECIMAL(38, 18),
    cancel_qty                  DECIMAL(38, 18),
    cancel_flag                 BOOLEAN,
    cancel_timestamp            TIMESTAMP,
    rejected_flag               BOOLEAN,
    initiator                   STRING,
    capacity                    STRING,
    market_center_id            STRING,
    side_details_ind            STRING,
    clearing_firm               STRING,
    fulfillment_id              STRING,
    fulfillment_link_type       STRING,
    occ_clearing_member_id      STRING,
    raw_payload_json            STRING,
    record_source               STRING NOT NULL
)
USING DELTA;

-- Gold facts (same Spark-SQL-on-Delta as Databricks)

CREATE TABLE IF NOT EXISTS gold.fact_option_order_events (
    option_event_sk             BIGINT GENERATED ALWAYS AS IDENTITY,
    event_dts                   TIMESTAMP NOT NULL,
    event_date                  DATE NOT NULL,
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT NOT NULL,
    party_sk                    BIGINT NOT NULL,
    trader_sk                   BIGINT,
    desk_sk                     BIGINT,
    account_sk                  BIGINT,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT NOT NULL,
    option_id                   STRING NOT NULL,
    cat_order_id                STRING NOT NULL,
    cat_event_code              STRING NOT NULL,
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
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING
)
USING DELTA
PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS gold.fact_option_executions (
    option_execution_sk         BIGINT GENERATED ALWAYS AS IDENTITY,
    event_dts                   TIMESTAMP NOT NULL,
    event_date                  DATE NOT NULL,
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT NOT NULL,
    party_sk                    BIGINT NOT NULL,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT NOT NULL,
    option_id                   STRING NOT NULL,
    cat_event_code              STRING NOT NULL,
    trade_id                    STRING,
    trade_key_date              TIMESTAMP,              -- CAT IM v4.1.0r15 section 5.1.11 row 6 (MOOT)
    fulfillment_id              STRING,
    fill_key_date               TIMESTAMP,              -- CAT IM v4.1.0r15 section 5.1.12.1 row 6 (MOOF/MOOFS)
    prior_fulfillment_id        STRING,
    prior_fill_key_date         TIMESTAMP,              -- CAT IM v4.1.0r15 section 5.1.12.3 row 9 (MOFA)
    quantity                    DECIMAL(38, 18) NOT NULL,
    price                       DECIMAL(38, 18) NOT NULL,
    capacity                    STRING,
    tape_trade_id               STRING,
    market_center_id            STRING,
    side_details_ind            STRING,
    clearing_firm               STRING,
    fulfillment_link_type       STRING,
    cancel_flag                 BOOLEAN,
    cancel_timestamp            TIMESTAMP,
    multi_leg_ind               BOOLEAN,
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING
)
USING DELTA
PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS gold.fact_option_allocations (
    option_allocation_sk        BIGINT GENERATED ALWAYS AS IDENTITY,
    event_dts                   TIMESTAMP NOT NULL,
    event_date                  DATE NOT NULL,
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT NOT NULL,
    party_sk                    BIGINT NOT NULL,
    account_sk                  BIGINT,
    event_type_sk               BIGINT NOT NULL,
    option_id                   STRING NOT NULL,
    cat_event_code              STRING NOT NULL,
    allocation_id               STRING NOT NULL,
    prior_allocation_id         STRING,
    side                        STRING,
    quantity                    DECIMAL(38, 18) NOT NULL,
    price                       DECIMAL(38, 18),
    firm_designated_id          STRING NOT NULL,
    new_order_fdid              STRING,
    correspondent_crd           BIGINT,
    occ_clearing_member_id      STRING,
    trade_date                  DATE,
    settlement_date             DATE,
    allocation_type             STRING,
    institution_flag            BOOLEAN,
    account_holder_type         STRING,
    cancel_flag                 BOOLEAN,
    cancel_timestamp            TIMESTAMP,
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING
)
USING DELTA
PARTITIONED BY (event_date);
