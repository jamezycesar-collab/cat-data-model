-- Simple-option Silver-layer DDL (Delta Lake / Databricks).
--
-- Source: FINRA CAT IM Tech Specs v4.1.0r15 Section 5.1 (Simple Option Events).
-- Covers the 35 simple-option event codes:
--   MONO MONOS MOOR MOMR MOCR MOORS MOMRS MOCRS MOOA
--   MOIR MOIM MOIC MOIMR MOICR
--   MOCO MOCOM MOCOC
--   MOOM MOOMS MOOMR MOOJ MOOC MOOCR
--   MONQ MORQ MOQR MOQC MOQM
--   MOOT MOOF MOOFS MOFA MOPA MOAA MOOE
--
-- Structurally parallels equity Silver (hub_order / link_order_event / etc)
-- with optionID as the instrument identifier rather than symbol. Multi-leg
-- option events live in ddl/multileg/ and are not covered here.


CREATE TABLE IF NOT EXISTS silver.hub_option_order (
    option_order_hk             STRING NOT NULL,
    option_order_bk_orderID     STRING NOT NULL,
    option_order_bk_keyDate     DATE   NOT NULL,
    option_order_bk_imid        STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_hub_option_order PRIMARY KEY (option_order_hk)
)
USING DELTA
PARTITIONED BY (option_order_bk_keyDate)
COMMENT 'DV2 Hub: Simple option orders. BK = (orderID, orderKeyDate, CATReporterIMID).';


CREATE TABLE IF NOT EXISTS silver.hub_option_instrument (
    option_instrument_hk        STRING NOT NULL,
    option_id_bk                STRING NOT NULL,        -- 21-char OSI Symbol; 22-char with FLEX prefix
    is_flex                     BOOLEAN NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_hub_option_instrument PRIMARY KEY (option_instrument_hk)
)
USING DELTA
COMMENT 'DV2 Hub: Option instruments keyed on the OSI Symbol (Section 2.4.4).';


CREATE TABLE IF NOT EXISTS silver.link_option_event (
    option_event_hk             STRING NOT NULL,
    option_order_hk             STRING NOT NULL,
    option_instrument_hk        STRING NOT NULL,
    event_code                  STRING NOT NULL,
    event_timestamp             TIMESTAMP NOT NULL,
    event_type_hk               STRING NOT NULL,
    venue_hk                    STRING,
    sender_party_hk             STRING,
    receiver_party_hk           STRING,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_link_option_event PRIMARY KEY (option_event_hk),
    CONSTRAINT fk_link_oe_order  FOREIGN KEY (option_order_hk)
        REFERENCES silver.hub_option_order (option_order_hk),
    CONSTRAINT fk_link_oe_instr  FOREIGN KEY (option_instrument_hk)
        REFERENCES silver.hub_option_instrument (option_instrument_hk),
    CONSTRAINT fk_link_oe_evtype FOREIGN KEY (event_type_hk)
        REFERENCES silver.hub_event_type (event_type_hk),
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
PARTITIONED BY (DATE(event_timestamp))
COMMENT 'DV2 Link: First-class event for every Section 5.1 simple-option CAT event.';


CREATE TABLE IF NOT EXISTS silver.link_option_child_order (
    option_child_order_hk       STRING NOT NULL,
    parent_option_order_hk      STRING NOT NULL,
    child_option_order_hk       STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_link_option_child PRIMARY KEY (option_child_order_hk),
    CONSTRAINT fk_link_ochild_parent FOREIGN KEY (parent_option_order_hk)
        REFERENCES silver.hub_option_order (option_order_hk),
    CONSTRAINT fk_link_ochild_child  FOREIGN KEY (child_option_order_hk)
        REFERENCES silver.hub_option_order (option_order_hk)
)
USING DELTA
COMMENT 'DV2 Link: Parent/child for option Child Order family (MOCO, MOCOM, MOCOC).';


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
    record_source               STRING NOT NULL,
    CONSTRAINT pk_sat_option_order PRIMARY KEY (option_order_hk, load_dts),
    CONSTRAINT fk_sat_option_order FOREIGN KEY (option_order_hk)
        REFERENCES silver.hub_option_order (option_order_hk)
)
USING DELTA
COMMENT 'DV2 Satellite: SCD2 attributes of the option order header.';


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
    record_source               STRING NOT NULL,
    CONSTRAINT pk_sat_option_event PRIMARY KEY (option_event_hk, load_dts),
    CONSTRAINT fk_sat_option_event FOREIGN KEY (option_event_hk)
        REFERENCES silver.link_option_event (option_event_hk)
)
USING DELTA
COMMENT 'DV2 Satellite: Per-event attributes for simple-option CAT events.';
