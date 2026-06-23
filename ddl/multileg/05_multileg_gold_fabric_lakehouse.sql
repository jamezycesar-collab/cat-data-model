-- ============================================================================
-- Multi-leg option Gold layer (Microsoft Fabric Lakehouse)
-- ============================================================================
--
-- Tier 18.3: fourth-dialect variant of the multi-leg Gold layer, mirroring
-- the existing Delta (02_*), Fabric Warehouse (04_*), and Hive (06_*) files.
-- Brings multi-leg Gold to 4-dialect parity (matching the option Gold layout).
--
-- Fabric Lakehouse uses Delta under the hood with Spark SQL DDL syntax, so
-- this is structurally identical to the Delta variant in 02_*. Distinguished
-- by TBLPROPERTIES being slightly more conservative (no autoOptimize - Fabric
-- manages that via workspace settings).
-- ============================================================================


CREATE TABLE IF NOT EXISTS gold.dim_multileg_strategy (
    multileg_strategy_sk        BIGINT GENERATED ALWAYS AS IDENTITY,
    multileg_strategy_bk        STRING NOT NULL,
    strategy_name               STRING NOT NULL,
    leg_count_min               INT,
    leg_count_max               INT,
    description                 STRING,
    effective_dts               TIMESTAMP NOT NULL,
    end_dts                     TIMESTAMP,
    is_current                  BOOLEAN NOT NULL
)
USING DELTA
COMMENT 'Gold dim: known multi-leg option strategies (SCD2).';


CREATE TABLE IF NOT EXISTS gold.fact_multileg_option_events (
    multileg_event_sk           BIGINT GENERATED ALWAYS AS IDENTITY,
    event_dts                   TIMESTAMP NOT NULL,
    event_date                  DATE NOT NULL,
    -- conformed dimension FKs
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT,
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
    leg_count                   INT NOT NULL,
    cat_event_code              STRING NOT NULL,
    side_summary                STRING,
    net_price                   DECIMAL(38, 18),
    order_quantity              DECIMAL(38, 18),
    time_in_force               STRING,
    handling_instructions       STRING,
    -- CAT IM v4.1.0r15 spec-mapping columns (added in Tier 17.5)
    underlying                  STRING NOT NULL,        -- section 5.2.1 row 8 underlying
    order_key_date              TIMESTAMP NOT NULL,     -- section 5.2.1 row 6 orderKeyDate
    cat_order_id                STRING NOT NULL,        -- section 5.2.1 row 7 orderID
    event_timestamp             TIMESTAMP NOT NULL,     -- section 5.2.1 row 9 eventTimestamp
    manual_flag                 BOOLEAN NOT NULL,       -- section 5.2.1 row 10 manualFlag
    manual_order_key_date       TIMESTAMP,              -- section 5.2.1 row 11 manualOrderKeyDate
    manual_order_id             STRING,                 -- section 5.2.1 row 12 manualOrderID
    electronic_dup_flag         BOOLEAN NOT NULL,       -- section 5.2.1 row 13 electronicDupFlag
    electronic_timestamp        TIMESTAMP,              -- section 5.2.1 row 14 electronicTimestamp
    dept_type                   STRING NOT NULL,        -- section 5.2.1 row 15 deptType
    price                       DECIMAL(38, 18),        -- section 5.2.1 row 16 net price
    quantity                    DECIMAL(38, 18) NOT NULL, -- section 5.2.1 row 17 quantity
    min_qty                     DECIMAL(38, 18),        -- section 5.2.1 row 18 minQty
    order_type                  STRING NOT NULL,        -- section 5.2.1 row 19 orderType
    trading_session             STRING NOT NULL,        -- section 5.2.1 row 21 tradingSession
    firm_designated_id          STRING,                 -- section 5.2.1 row 23 firmDesignatedID
    account_holder_type         STRING,                 -- section 5.2.1 row 24 accountHolderType
    affiliate_flag              BOOLEAN,                -- section 5.2.1 row 25 affiliateFlag
    representative_ind          STRING,                 -- section 5.2.1 row 27 representativeInd
    solicitation_flag           BOOLEAN NOT NULL,       -- section 5.2.1 row 28 solicitationFlag
    rfq_id                      STRING,                 -- section 5.2.1 row 29 RFQID
    number_of_legs              INT NOT NULL,           -- section 5.2.1 row 30 numberOfLegs
    sender_imid                 STRING,                 -- section 5.2.2 row 14 senderIMID
    destination                 STRING,                 -- section 5.2.2 row 15 destination
    destination_type            STRING,                 -- section 5.2.2 row 16 destinationType
    routed_order_id             STRING,                 -- section 5.2.2 row 17 routedOrderID
    session                     STRING,                 -- section 5.2.2 row 18 session
    route_rejected_flag         BOOLEAN,                -- section 5.2.2 row 27 routeRejectedFlag
    exch_origin_code            STRING,                 -- section 5.2.2 row 28 exchOriginCode
    paired_order_id             STRING,                 -- section 5.2.2 row 34 pairedOrderID
    receiver_imid               STRING,                 -- section 5.2.3 row 15 receiverIMID
    sender_type                 STRING,                 -- section 5.2.3 row 17 senderType
    originating_imid            STRING,                 -- section 5.2.2 row 9 originatingIMID
    prior_order_key_date        TIMESTAMP,              -- section 5.2.6 row 9 priorOrderKeyDate
    prior_order_id              STRING,                 -- section 5.2.6 row 10 priorOrderID
    parent_order_key_date       TIMESTAMP,              -- section 5.2.5.1 row 9 parentOrderKeyDate
    parent_order_id             STRING,                 -- section 5.2.5.1 row 10 parentOrderID
    initiator                   STRING,                 -- section 5.2.6 row 22 initiator
    leaves_qty                  DECIMAL(38, 18),        -- section 5.2.6 row 27 leavesQty
    cancel_qty                  DECIMAL(38, 18),        -- section 5.2.7 row 13 cancelQty
    request_timestamp           TIMESTAMP,              -- section 5.2.6 row 33 requestTimestamp
    quote_key_date              TIMESTAMP,              -- section 5.2.8.1 row 6 quoteKeyDate
    quote_id                    STRING,                 -- section 5.2.8.1 row 7 quoteID
    bid_price                   DECIMAL(38, 18),        -- section 5.2.8.1 row 11 bidPrice
    ask_price                   DECIMAL(38, 18),        -- section 5.2.8.1 row 13 askPrice
    bid_qty                     DECIMAL(38, 18),        -- section 5.2.8.1 row 12 bidQty
    ask_qty                     DECIMAL(38, 18),        -- section 5.2.8.1 row 14 askQty
    routed_quote_id             STRING,                 -- section 5.2.8.2 row 12 routedQuoteID
    quote_rejected_flag         BOOLEAN,                -- section 5.2.8.2 row 18 quoteRejectedFlag
    prior_quote_key_date        TIMESTAMP,              -- section 5.2.8.6 row 9 priorQuoteKeyDate
    prior_quote_id              STRING,                 -- section 5.2.8.6 row 10 priorQuoteID
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT chk_fact_mle_code CHECK (cat_event_code IN (
        'MLNO','MLOR','MLMR','MLCR','MLOA','MLIR','MLIM','MLIC','MLIMR','MLICR',
        'MLCO','MLCOM','MLCOC','MLOM','MLOMR','MLOC','MLOCR',
        'MLNQ','MLRQ','MLQS','MLQR','MLQC','MLQM',
        'MLOS','MLOE'
    )),
    CONSTRAINT chk_fact_mle_legs CHECK (leg_count >= 2)
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'Gold fact: header-grain row per multi-leg CAT event (Section 5.2 of IM Tech Specs v4.1.0r15).';


CREATE TABLE IF NOT EXISTS gold.fact_multileg_option_legs (
    multileg_leg_event_sk       BIGINT GENERATED ALWAYS AS IDENTITY,
    multileg_event_sk           BIGINT NOT NULL,    -- FK to fact_multileg_option_events
    leg_seq                     INT NOT NULL,       -- the leg index n in CAT IM legs array
    instrument_sk               BIGINT NOT NULL,    -- FK to dim_instrument; option contract or covered stock
    -- CAT IM v4.1.0r15 section 5.2.1 row 32 legs sub-table: rows 32.n.1 - 32.n.6
    leg_ref_id                  STRING,             -- section 5.2.1 row 32.n.1 legRefID
    leg_symbol                  STRING,             -- section 5.2.1 row 32.n.2 symbol of stock leg
    leg_option_id               STRING,             -- section 5.2.1 row 32.n.3 optionID OSI symbol
    leg_open_close_indicator    STRING,             -- section 5.2.1 row 32.n.4 openCloseIndicator
    leg_side                    STRING NOT NULL,    -- section 5.2.1 row 32.n.5 side BUY SELL
    leg_ratio_quantity          DECIMAL(10, 4) NOT NULL, -- section 5.2.1 row 32.n.6 legRatioQuantity
    leg_quantity                DECIMAL(38, 18) NOT NULL,
    leg_price                   DECIMAL(38, 18),
    leg_status                  STRING,
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    CONSTRAINT chk_fact_mlel_side CHECK (leg_side IN ('BUY','SELL'))
)
USING DELTA
COMMENT 'Gold fact: per-leg detail. One header row in fact_multileg_option_events expands to N rows here.';


-- View consolidating multi-leg events with leg-level detail for BI consumption.
-- Mirrors the view in 02_multileg_gold_delta.sql and 06_multileg_gold_hive.sql.
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
