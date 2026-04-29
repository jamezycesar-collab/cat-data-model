-- Multi-leg option Silver-layer DDL (Apache Hive).
-- Hive mirror of 01_multileg_silver_delta.sql.
-- Notes: Hive does not support PRIMARY KEY / FOREIGN KEY enforcement; constraints
-- are documented as comments only. No CHECK constraints either; enum values are
-- enforced upstream (DLT pipeline expectations).

CREATE TABLE IF NOT EXISTS silver.hub_multileg_order (
 multileg_order_hk STRING COMMENT 'PK SHA-256(orderKeyDate||orderID||CATReporterIMID)',
 multileg_order_bk_orderID STRING,
 multileg_order_bk_imid STRING,
 load_dts TIMESTAMP,
 record_source STRING,
 dv2_source_hk STRING
)
PARTITIONED BY (multileg_order_bk_keyDate DATE)
STORED AS PARQUET
TBLPROPERTIES ('comment' = 'DV2 Hub: Multi-leg / complex option orders.');


CREATE TABLE IF NOT EXISTS silver.hub_multileg_leg (
 multileg_leg_hk STRING,
 multileg_order_hk STRING,
 leg_seq INT,
 load_dts TIMESTAMP,
 record_source STRING,
 dv2_source_hk STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.link_multileg_order_leg (
 multileg_order_leg_hk STRING,
 multileg_order_hk STRING,
 multileg_leg_hk STRING,
 instrument_hk STRING,
 leg_side STRING,
 leg_open_close_indicator STRING,
 leg_ratio DECIMAL(10, 4),
 load_dts TIMESTAMP,
 record_source STRING,
 dv2_source_hk STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.link_multileg_event (
 multileg_event_hk STRING,
 multileg_order_hk STRING,
 event_code STRING COMMENT 'CHECK in MLNO,MLOR,MLMR,MLCR,MLOA,MLIR,MLIM,MLIC,MLIMR,MLICR,MLCO,MLCOM,MLCOC,MLOM,MLOMR,MLOC,MLOCR,MLNQ,MLRQ,MLQS,MLQR,MLQC,MLQM,MLOS,MLOE',
 event_timestamp TIMESTAMP,
 event_type_hk STRING,
 venue_hk STRING,
 sender_party_hk STRING,
 receiver_party_hk STRING,
 load_dts TIMESTAMP,
 record_source STRING,
 dv2_source_hk STRING
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.link_multileg_quote (
 multileg_quote_hk STRING,
 multileg_order_hk STRING,
 quote_id STRING,
 quote_event_code STRING COMMENT 'CHECK in MLNQ,MLRQ,MLQS,MLQR,MLQC,MLQM',
 venue_hk STRING,
 load_dts TIMESTAMP,
 record_source STRING,
 dv2_source_hk STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.link_multileg_child_order (
 multileg_child_order_hk STRING,
 parent_multileg_order_hk STRING,
 child_multileg_order_hk STRING,
 load_dts TIMESTAMP,
 record_source STRING,
 dv2_source_hk STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.sat_multileg_order_state (
 multileg_order_hk STRING,
 load_dts TIMESTAMP,
 load_end_dts TIMESTAMP,
 hash_diff STRING,
 order_status STRING,
 order_quantity DECIMAL(38, 18),
 limit_price DECIMAL(38, 18),
 net_price DECIMAL(38, 18),
 time_in_force STRING,
 handling_instructions STRING,
 representative_ind BOOLEAN,
 aggregated_orders_json STRING,
 record_source STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.sat_multileg_leg_state (
 multileg_leg_hk STRING,
 load_dts TIMESTAMP,
 load_end_dts TIMESTAMP,
 hash_diff STRING,
 leg_quantity DECIMAL(38, 18),
 leg_price DECIMAL(38, 18),
 leg_open_close STRING,
 leg_ratio DECIMAL(10, 4),
 leg_status STRING,
 record_source STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS silver.sat_multileg_event_state (
 multileg_event_hk STRING,
 load_dts TIMESTAMP,
 load_end_dts TIMESTAMP,
 hash_diff STRING,
 event_qty DECIMAL(38, 18),
 event_price DECIMAL(38, 18),
 route_dest_imid STRING,
 rejected_flag BOOLEAN,
 cancel_reason STRING,
 raw_payload_json STRING,
 record_source STRING
)
STORED AS PARQUET;
