-- Multi-leg option order Silver-layer DDL (Delta Lake / Databricks).
--
-- Adds Data Vault 2.0 hubs, links, and satellites to cover Section 5.2 of
-- the FINRA CAT IM Tech Specs (Multi-Leg / Complex Option Order Events).
--
-- The simple-option model already covers Section 5.1. Multi-leg orders
-- differ in three structural ways:
--   (1) A multi-leg order has 2..N legs, each carrying its own optionID,
--       side, and ratio. The leg list is part of the order's identity.
--   (2) Some legs may be equity (covered stock) - so a leg references
--       hub_instrument generically rather than an option-only hub.
--   (3) Quote, route, modify, cancel, and effective events are reported
--       at the multi-leg order level, not per leg.
--
-- The DV2 pattern below adds:
--   hub_multileg_order        - one hub per multi-leg order
--   hub_multileg_leg          - one hub per leg (composite of order + leg #)
--   link_multileg_order_leg   - relates multi-leg order to its legs and
--                               their underlying instruments
--   link_multileg_event       - first-class CAT event link for all 25 MLxxx codes
--   link_multileg_quote       - dedicated link for MLNQ / MLRQ / MLQS / etc.
--   link_multileg_child_order - parent multi-leg order to MLCO child
--   sat_multileg_order_state  - SCD2 attributes of the multi-leg order
--   sat_multileg_leg_state    - SCD2 attributes per leg
--   sat_multileg_event_state  - per-event attributes


CREATE TABLE IF NOT EXISTS silver.hub_multileg_order (
    multileg_order_hk           STRING NOT NULL,        -- SHA-256(orderKeyDate || orderID || CATReporterIMID)
    multileg_order_bk_orderID   STRING NOT NULL,
    multileg_order_bk_keyDate   DATE   NOT NULL,
    multileg_order_bk_imid      STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_hub_multileg_order PRIMARY KEY (multileg_order_hk)
)
USING DELTA
PARTITIONED BY (multileg_order_bk_keyDate)
COMMENT 'DV2 Hub: Multi-leg / complex option orders. Business key is (orderID, orderKeyDate, CATReporterIMID).';


CREATE TABLE IF NOT EXISTS silver.hub_multileg_leg (
    multileg_leg_hk             STRING NOT NULL,        -- SHA-256(multileg_order_hk || leg_seq)
    multileg_order_hk           STRING NOT NULL,
    leg_seq                     INT    NOT NULL,        -- 1..N within the parent order
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_hub_multileg_leg PRIMARY KEY (multileg_leg_hk),
    CONSTRAINT fk_hub_multileg_leg_order FOREIGN KEY (multileg_order_hk)
        REFERENCES silver.hub_multileg_order (multileg_order_hk)
)
USING DELTA
COMMENT 'DV2 Hub: Individual legs of a multi-leg order.';


CREATE TABLE IF NOT EXISTS silver.link_multileg_order_leg (
    multileg_order_leg_hk       STRING NOT NULL,
    multileg_order_hk           STRING NOT NULL,
    multileg_leg_hk             STRING NOT NULL,
    instrument_hk               STRING NOT NULL,        -- the underlying / option for this leg
    leg_side                    STRING NOT NULL,        -- BUY/SELL
    leg_open_close_indicator    STRING,                 -- O / C / null for equity
    leg_ratio                   DECIMAL(10, 4),
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_link_multileg_order_leg PRIMARY KEY (multileg_order_leg_hk),
    CONSTRAINT fk_link_mlo_leg_order FOREIGN KEY (multileg_order_hk)
        REFERENCES silver.hub_multileg_order (multileg_order_hk),
    CONSTRAINT fk_link_mlo_leg_leg   FOREIGN KEY (multileg_leg_hk)
        REFERENCES silver.hub_multileg_leg   (multileg_leg_hk),
    CONSTRAINT fk_link_mlo_leg_instr FOREIGN KEY (instrument_hk)
        REFERENCES silver.hub_instrument     (instrument_hk)
)
USING DELTA
COMMENT 'DV2 Link: Connects a multi-leg order to its legs and their underlying instruments.';


CREATE TABLE IF NOT EXISTS silver.link_multileg_event (
    multileg_event_hk           STRING NOT NULL,
    multileg_order_hk           STRING NOT NULL,
    event_code                  STRING NOT NULL,        -- one of MLNO MLOR MLMR MLCR ... MLOE
    event_timestamp             TIMESTAMP NOT NULL,
    event_type_hk               STRING NOT NULL,
    venue_hk                    STRING,
    sender_party_hk             STRING,
    receiver_party_hk           STRING,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_link_multileg_event PRIMARY KEY (multileg_event_hk),
    CONSTRAINT fk_link_mle_order   FOREIGN KEY (multileg_order_hk)
        REFERENCES silver.hub_multileg_order (multileg_order_hk),
    CONSTRAINT fk_link_mle_evtype  FOREIGN KEY (event_type_hk)
        REFERENCES silver.hub_event_type (event_type_hk),
    CONSTRAINT chk_link_mle_code CHECK (event_code IN (
        'MLNO','MLOR','MLMR','MLCR','MLOA','MLIR','MLIM','MLIC','MLIMR','MLICR',
        'MLCO','MLCOM','MLCOC','MLOM','MLOMR','MLOC','MLOCR',
        'MLNQ','MLRQ','MLQS','MLQR','MLQC','MLQM',
        'MLOS','MLOE'
    ))
)
USING DELTA
PARTITIONED BY (DATE(event_timestamp))
COMMENT 'DV2 Link: First-class event for every Section 5.2 (Multi-Leg) CAT event.';


CREATE TABLE IF NOT EXISTS silver.link_multileg_quote (
    multileg_quote_hk           STRING NOT NULL,
    multileg_order_hk           STRING NOT NULL,
    quote_id                    STRING NOT NULL,
    quote_event_code            STRING NOT NULL,        -- MLNQ MLRQ MLQS MLQR MLQC MLQM
    venue_hk                    STRING,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_link_multileg_quote PRIMARY KEY (multileg_quote_hk),
    CONSTRAINT fk_link_mlq_order FOREIGN KEY (multileg_order_hk)
        REFERENCES silver.hub_multileg_order (multileg_order_hk),
    CONSTRAINT chk_link_mlq_code CHECK (quote_event_code IN (
        'MLNQ','MLRQ','MLQS','MLQR','MLQC','MLQM'
    ))
)
USING DELTA
COMMENT 'DV2 Link: Multi-leg quote events.';


CREATE TABLE IF NOT EXISTS silver.link_multileg_child_order (
    multileg_child_order_hk     STRING NOT NULL,
    parent_multileg_order_hk    STRING NOT NULL,
    child_multileg_order_hk     STRING NOT NULL,
    load_dts                    TIMESTAMP NOT NULL,
    record_source               STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    CONSTRAINT pk_link_multileg_child PRIMARY KEY (multileg_child_order_hk),
    CONSTRAINT fk_link_mlc_parent FOREIGN KEY (parent_multileg_order_hk)
        REFERENCES silver.hub_multileg_order (multileg_order_hk),
    CONSTRAINT fk_link_mlc_child  FOREIGN KEY (child_multileg_order_hk)
        REFERENCES silver.hub_multileg_order (multileg_order_hk)
)
USING DELTA
COMMENT 'DV2 Link: Parent / child relationship for multi-leg child orders (MLCO family).';


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
    record_source               STRING NOT NULL,
    CONSTRAINT pk_sat_multileg_order PRIMARY KEY (multileg_order_hk, load_dts),
    CONSTRAINT fk_sat_mlo_order FOREIGN KEY (multileg_order_hk)
        REFERENCES silver.hub_multileg_order (multileg_order_hk)
)
USING DELTA
COMMENT 'DV2 Satellite: SCD2 attributes for the multi-leg order header (price/qty/status).';


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
    record_source               STRING NOT NULL,
    CONSTRAINT pk_sat_multileg_leg PRIMARY KEY (multileg_leg_hk, load_dts),
    CONSTRAINT fk_sat_mll_leg FOREIGN KEY (multileg_leg_hk)
        REFERENCES silver.hub_multileg_leg (multileg_leg_hk)
)
USING DELTA
COMMENT 'DV2 Satellite: SCD2 attributes per leg.';


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
    record_source               STRING NOT NULL,
    CONSTRAINT pk_sat_multileg_event PRIMARY KEY (multileg_event_hk, load_dts),
    CONSTRAINT fk_sat_mle_event FOREIGN KEY (multileg_event_hk)
        REFERENCES silver.link_multileg_event (multileg_event_hk)
)
USING DELTA
COMMENT 'DV2 Satellite: Per-event attributes for multi-leg CAT events.';
