-- Multi-leg option Gold-layer DDL (Delta Lake / Databricks).
--
-- Adds:
--   fact_multileg_option_events  - one row per multi-leg CAT event
--   fact_multileg_option_legs    - one row per leg per event (for execution / fill grain)
--   dim_multileg_strategy        - reference dim for known strategy types (vertical, calendar, butterfly, condor, custom)
--
-- The four existing facts (fact_order_events, fact_execution_events,
-- fact_allocations, fact_cais_snapshots) remain in place for equity and
-- simple option events. Multi-leg events are NOT folded into them because
-- of grain mismatch: a multi-leg execution produces N leg fills against
-- one parent event, and the existing facts are single-leg.


CREATE TABLE IF NOT EXISTS gold.dim_multileg_strategy (
    multileg_strategy_sk        BIGINT GENERATED ALWAYS AS IDENTITY,
    multileg_strategy_bk        STRING NOT NULL,
    strategy_name               STRING NOT NULL,
    leg_count_min               INT    NOT NULL,
    leg_count_max               INT    NOT NULL,
    description                 STRING,
    effective_dts               TIMESTAMP NOT NULL,
    end_dts                     TIMESTAMP NOT NULL,
    is_current                  BOOLEAN NOT NULL,
    CONSTRAINT pk_dim_multileg_strategy PRIMARY KEY (multileg_strategy_sk)
)
USING DELTA
COMMENT 'Gold dim: known multi-leg option strategies. SCD2.';


CREATE TABLE IF NOT EXISTS gold.fact_multileg_option_events (
    multileg_event_sk           BIGINT GENERATED ALWAYS AS IDENTITY,
    event_dts                   TIMESTAMP NOT NULL,
    event_date                  DATE      NOT NULL,
    -- conformed dimension FKs
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT,                -- NULL for header events that have no single instrument
    party_sk                    BIGINT NOT NULL,
    trader_sk                   BIGINT,
    desk_sk                     BIGINT,
    account_sk                  BIGINT,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT NOT NULL,
    multileg_strategy_sk        BIGINT,
    -- multi-leg specifics
    multileg_order_id           STRING NOT NULL,
    parent_multileg_order_id    STRING,
    leg_count                   INT    NOT NULL,
    cat_event_code              STRING NOT NULL,        -- MLNO MLOR ... MLOE
    side_summary                STRING,                 -- BUY/SELL/MIXED
    net_price                   DECIMAL(38, 18),
    order_quantity              DECIMAL(38, 18),
    time_in_force               STRING,
    handling_instructions       STRING,
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,                 -- PASS / QUARANTINE
    CONSTRAINT pk_fact_multileg_event PRIMARY KEY (multileg_event_sk),
    CONSTRAINT chk_fact_mle_code CHECK (cat_event_code IN (
        'MLNO','MLOR','MLMR','MLCR','MLOA','MLIR','MLIM','MLIC','MLIMR','MLICR',
        'MLCO','MLCOM','MLCOC','MLOM','MLOMR','MLOC','MLOCR',
        'MLNQ','MLRQ','MLQS','MLQR','MLQC','MLQM',
        'MLOS','MLOE'
    )),
    CONSTRAINT chk_fact_mle_legs CHECK (leg_count >= 2),
    CONSTRAINT fk_fact_mle_date     FOREIGN KEY (date_sk)
        REFERENCES gold.dim_date (date_sk),
    CONSTRAINT fk_fact_mle_party    FOREIGN KEY (party_sk)
        REFERENCES gold.dim_party (party_sk),
    CONSTRAINT fk_fact_mle_evtype   FOREIGN KEY (event_type_sk)
        REFERENCES gold.dim_event_type (event_type_sk),
    CONSTRAINT fk_fact_mle_strategy FOREIGN KEY (multileg_strategy_sk)
        REFERENCES gold.dim_multileg_strategy (multileg_strategy_sk)
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'Gold fact: header-grain row per multi-leg CAT event (Section 5.2 of IM Tech Specs v4.1.0r15).';


CREATE TABLE IF NOT EXISTS gold.fact_multileg_option_legs (
    multileg_leg_event_sk       BIGINT GENERATED ALWAYS AS IDENTITY,
    multileg_event_sk           BIGINT NOT NULL,        -- FK to header
    leg_seq                     INT    NOT NULL,
    instrument_sk               BIGINT NOT NULL,        -- the option or covered stock for this leg
    leg_side                    STRING NOT NULL,        -- BUY / SELL
    leg_open_close              STRING,                 -- O / C / NULL for stock
    leg_quantity                DECIMAL(38, 18) NOT NULL,
    leg_price                   DECIMAL(38, 18),
    leg_ratio                   DECIMAL(10, 4) NOT NULL,
    leg_status                  STRING,
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    CONSTRAINT pk_fact_mle_legs PRIMARY KEY (multileg_leg_event_sk),
    CONSTRAINT fk_fact_mlel_header FOREIGN KEY (multileg_event_sk)
        REFERENCES gold.fact_multileg_option_events (multileg_event_sk),
    CONSTRAINT fk_fact_mlel_instr FOREIGN KEY (instrument_sk)
        REFERENCES gold.dim_instrument (instrument_sk),
    CONSTRAINT chk_fact_mlel_side CHECK (leg_side IN ('BUY','SELL'))
)
USING DELTA
COMMENT 'Gold fact: per-leg detail for multi-leg events. One header in fact_multileg_option_events expands to N rows here.';


-- View consolidating multi-leg events with leg-level detail for BI consumption.
CREATE OR REPLACE VIEW gold.vw_multileg_option_lifecycle AS
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
    l.leg_side,
    l.leg_open_close,
    l.leg_quantity,
    l.leg_price,
    l.leg_ratio,
    i.instrument_bk AS leg_instrument
FROM gold.fact_multileg_option_events h
JOIN gold.fact_multileg_option_legs   l ON h.multileg_event_sk = l.multileg_event_sk
JOIN gold.dim_party                   p ON h.party_sk          = p.party_sk
JOIN gold.dim_event_type              et ON h.event_type_sk     = et.event_type_sk
JOIN gold.dim_instrument              i ON l.instrument_sk     = i.instrument_sk;
