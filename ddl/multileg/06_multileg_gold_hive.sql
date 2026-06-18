-- Multi-leg option Gold-layer DDL (Apache Hive).
-- Hive mirror of 02_multileg_gold_delta.sql.
-- Notes: Hive does not enforce PRIMARY KEY / FOREIGN KEY / CHECK constraints;
-- they are documented as comments only. Enforcement is upstream in the DLT
-- quality gates (dlt_fact_multileg_option_events.py).


CREATE TABLE IF NOT EXISTS gold.dim_multileg_strategy (
    multileg_strategy_sk        BIGINT,
    multileg_strategy_bk        STRING,
    strategy_name               STRING,
    leg_count_min               INT,
    leg_count_max               INT,
    description                 STRING,
    effective_dts               TIMESTAMP,
    end_dts                     TIMESTAMP,
    is_current                  BOOLEAN
)
STORED AS PARQUET
TBLPROPERTIES ('comment' = 'Gold dim: known multi-leg option strategies (SCD2).');


CREATE TABLE IF NOT EXISTS gold.fact_multileg_option_events (
    multileg_event_sk           BIGINT,
    event_dts                   TIMESTAMP,
    -- conformed dimension FKs
    date_sk                     BIGINT,
    instrument_sk               BIGINT,
    party_sk                    BIGINT,
    trader_sk                   BIGINT,
    desk_sk                     BIGINT,
    account_sk                  BIGINT,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT,
    multileg_strategy_sk        BIGINT,
    -- multi-leg specifics
    multileg_order_id           STRING,
    parent_multileg_order_id    STRING,
    leg_count                   INT,
    cat_event_code              STRING COMMENT 'CHECK in MLNO,MLOR,MLMR,MLCR,MLOA,MLIR,MLIM,MLIC,MLIMR,MLICR,MLCO,MLCOM,MLCOC,MLOM,MLOMR,MLOC,MLOCR,MLNQ,MLRQ,MLQS,MLQR,MLQC,MLQM,MLOS,MLOE',
    side_summary                STRING COMMENT 'BUY | SELL | MIXED',
    net_price                   DECIMAL(38, 18),
    order_quantity              DECIMAL(38, 18),
    time_in_force               STRING,
    handling_instructions       STRING,
    -- lineage
    source_file                 STRING,
    source_batch_id             STRING,
    dv2_source_hk               STRING,
    quality_outcome             STRING COMMENT 'PASS | QUARANTINE'
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('comment' = 'Gold fact: header-grain row per multi-leg CAT event (Section 5.2 of IM Tech Specs v4.1.0r15).');


CREATE TABLE IF NOT EXISTS gold.fact_multileg_option_legs (
    multileg_leg_event_sk       BIGINT,
    multileg_event_sk           BIGINT COMMENT 'FK to fact_multileg_option_events.multileg_event_sk',
    leg_seq                     INT COMMENT 'the leg index n in CAT IM legs array',
    instrument_sk               BIGINT COMMENT 'FK to dim_instrument; option contract or covered stock',
    -- CAT IM v4.1.0r15 section 5.2.1 row 32 legs sub-table: rows 32.n.1 - 32.n.6
    leg_ref_id                  STRING COMMENT 'section 5.2.1 row 32.n.1 legRefID unique identifier of leg',
    leg_symbol                  STRING COMMENT 'section 5.2.1 row 32.n.2 symbol of stock leg',
    leg_option_id               STRING COMMENT 'section 5.2.1 row 32.n.3 optionID OSI symbol of option leg',
    leg_open_close_indicator    STRING COMMENT 'section 5.2.1 row 32.n.4 openCloseIndicator O C NULL',
    leg_side                    STRING COMMENT 'section 5.2.1 row 32.n.5 side BUY SELL',
    leg_ratio_quantity          DECIMAL(10, 4) COMMENT 'section 5.2.1 row 32.n.6 legRatioQuantity ratio',
    leg_quantity                DECIMAL(38, 18),
    leg_price                   DECIMAL(38, 18),
    leg_status                  STRING,
    -- lineage
    source_file                 STRING,
    source_batch_id             STRING
)
STORED AS PARQUET
TBLPROPERTIES ('comment' = 'Gold fact: per-leg detail. One header row in fact_multileg_option_events expands to N rows here.');


-- Hive view consolidating multi-leg events with leg-level detail for BI consumption.
-- Mirrors the Delta variant in 02_multileg_gold_delta.sql.
CREATE VIEW IF NOT EXISTS gold.vw_multileg_option_lifecycle AS
SELECT
    h.multileg_event_sk,
    h.event_dts,
    h.cat_event_code,
    h.multileg_order_id,
    h.parent_multileg_order_id,
    h.leg_count,
    h.net_price,
    h.order_quantity,
    p.party_bk,
    p.legal_name AS party_name,
    et.event_code,
    et.event_name,
    l.leg_seq,
    l.leg_ref_id,
    l.leg_symbol,
    l.leg_option_id,
    l.leg_side,
    l.leg_open_close_indicator,
    l.leg_quantity,
    l.leg_price,
    l.leg_ratio_quantity,
    i.instrument_bk AS leg_instrument
FROM gold.fact_multileg_option_events h
JOIN gold.fact_multileg_option_legs   l ON h.multileg_event_sk = l.multileg_event_sk
JOIN gold.dim_party                   p ON h.party_sk          = p.party_sk
JOIN gold.dim_event_type              et ON h.event_type_sk    = et.event_type_sk
JOIN gold.dim_instrument              i  ON l.instrument_sk    = i.instrument_sk;
