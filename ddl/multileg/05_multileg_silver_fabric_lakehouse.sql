-- Multi-leg option Silver-layer DDL (Microsoft Fabric Lakehouse).
-- Mirror of 01_multileg_silver_delta.sql for Fabric Lakehouse (Spark SQL on
-- OneLake / Delta Lake). Differences from Databricks Delta:
--   * USING DELTA stays
--   * Identity / surrogate-key generation uses Fabric Lakehouse default
--   * Constraints and CHECK clauses are kept (Fabric Lakehouse supports them)


CREATE TABLE IF NOT EXISTS silver.hub_multileg_order (
    multileg_order_hk           STRING NOT NULL,
    multileg_order_bk_orderID   STRING NOT NULL,
    multileg_order_bk_keyDate   DATE   NOT NULL,
    multileg_order_bk_imid      STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL
)
USING DELTA
PARTITIONED BY (multileg_order_bk_keyDate)
COMMENT 'DV2 Hub: Multi-leg / complex option orders (Fabric Lakehouse).';


CREATE TABLE IF NOT EXISTS silver.hub_multileg_leg (
    multileg_leg_hk             STRING NOT NULL,
    multileg_order_hk           STRING NOT NULL,
    leg_seq                     INT    NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL
)
USING DELTA
COMMENT 'DV2 Hub: Multi-leg legs (Fabric Lakehouse).';


CREATE TABLE IF NOT EXISTS silver.link_multileg_order_leg (
    multileg_order_leg_hk       STRING NOT NULL,
    multileg_order_hk           STRING NOT NULL,
    multileg_leg_hk             STRING NOT NULL,
    instrument_hk               STRING NOT NULL,
    leg_side                    STRING NOT NULL,
    leg_open_close_indicator    STRING,
    leg_ratio                   DECIMAL(10, 4),
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.link_multileg_event (
    multileg_event_hk           STRING NOT NULL,
    multileg_order_hk           STRING NOT NULL,
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
    CONSTRAINT chk_link_mle_code CHECK (event_code IN (
        'MLNO','MLOR','MLMR','MLCR','MLOA','MLIR','MLIM','MLIC','MLIMR','MLICR',
        'MLCO','MLCOM','MLCOC','MLOM','MLOMR','MLOC','MLOCR',
        'MLNQ','MLRQ','MLQS','MLQR','MLQC','MLQM',
        'MLOS','MLOE'
    ))
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'DV2 Link: 25 multi-leg CAT events (Section 5.2 of IM Tech Specs v4.1.0r15).';


CREATE TABLE IF NOT EXISTS silver.link_multileg_quote (
    multileg_quote_hk           STRING NOT NULL,
    multileg_order_hk           STRING NOT NULL,
    quote_id                    STRING NOT NULL,
    quote_event_code            STRING NOT NULL,
    venue_hk                    STRING,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT chk_link_mlq_code CHECK (quote_event_code IN (
        'MLNQ','MLRQ','MLQS','MLQR','MLQC','MLQM'
    ))
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.link_multileg_child_order (
    multileg_child_order_hk     STRING NOT NULL,
    parent_multileg_order_hk    STRING NOT NULL,
    child_multileg_order_hk     STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.sat_multileg_order_state (
    multileg_order_hk           STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING NOT NULL,
    order_status                STRING,
    order_quantity              DECIMAL(38, 18),
    limit_price                 DECIMAL(38, 18),
    net_price                   DECIMAL(38, 18),
    time_in_force               STRING,
    handling_instructions       STRING,
    representative_ind          BOOLEAN,
    aggregated_orders_json      STRING,
    record_source               STRING NOT NULL
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.sat_multileg_leg_state (
    multileg_leg_hk             STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING NOT NULL,
    leg_quantity                DECIMAL(38, 18),
    leg_price                   DECIMAL(38, 18),
    leg_open_close              STRING,
    leg_ratio                   DECIMAL(10, 4),
    leg_status                  STRING,
    record_source               STRING NOT NULL
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.sat_multileg_event_state (
    multileg_event_hk           STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    load_end_dts                TIMESTAMP,
    hash_diff                   STRING NOT NULL,
    event_qty                   DECIMAL(38, 18),
    event_price                 DECIMAL(38, 18),
    route_dest_imid             STRING,
    rejected_flag               BOOLEAN,
    cancel_reason               STRING,
    raw_payload_json            STRING,
    record_source               STRING NOT NULL
)
USING DELTA;
