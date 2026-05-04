-- Simple-option Silver-layer DDL (Apache Hive).
-- Hive does not enforce PRIMARY KEY / FOREIGN KEY / CHECK; documented as comments.

CREATE TABLE IF NOT EXISTS silver.hub_option_order (
    option_order_hk             STRING COMMENT 'PK',
    option_order_bk_orderID     STRING,
    option_order_bk_imid        STRING,
    load_dts                    TIMESTAMP,
    record_source               STRING,
    dv2_source_hk               STRING
)
PARTITIONED BY (option_order_bk_keyDate DATE)
STORED AS PARQUET
TBLPROPERTIES ('comment' = 'DV2 Hub: Simple option orders.');

CREATE TABLE IF NOT EXISTS silver.hub_option_instrument (
    option_instrument_hk        STRING,
    option_id_bk                STRING,
    is_flex                     BOOLEAN,
    load_dts                    TIMESTAMP,
    record_source               STRING,
    dv2_source_hk               STRING
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS silver.link_option_event (
    option_event_hk             STRING,
    option_order_hk             STRING,
    option_instrument_hk        STRING,
    event_code                  STRING COMMENT 'CHECK in Section 5.1 codes',
    event_timestamp             TIMESTAMP,
    event_type_hk               STRING,
    venue_hk                    STRING,
    sender_party_hk             STRING,
    receiver_party_hk           STRING,
    load_dts                    TIMESTAMP,
    record_source               STRING,
    dv2_source_hk               STRING
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS silver.link_option_child_order (
    option_child_order_hk       STRING,
    parent_option_order_hk      STRING,
    child_option_order_hk       STRING,
    load_dts                    TIMESTAMP,
    record_source               STRING,
    dv2_source_hk               STRING
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS silver.sat_option_order_state (
    option_order_hk             STRING,
    load_dts                    TIMESTAMP,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING,
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
    record_source               STRING
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS silver.sat_option_event_state (
    option_event_hk             STRING,
    load_dts                    TIMESTAMP,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING,
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
    record_source               STRING
)
STORED AS PARQUET;
