-- Simple-option Gold-layer DDL (Delta Lake / Databricks).
-- Adds the three Gold facts referenced by 06_cat_field_mapping.csv:
--   fact_option_order_events       header-grain row per simple-option order event
--   fact_option_executions         per-execution row for MOOT / MOOF / MOOFS / MOFA
--   fact_option_allocations        per-allocation row for MOPA / MOAA


CREATE TABLE IF NOT EXISTS gold.fact_option_order_events (
    option_event_sk             BIGINT GENERATED ALWAYS AS IDENTITY,
    event_dts                   TIMESTAMP NOT NULL,
    event_date                  DATE      NOT NULL,
    -- conformed dimension FKs
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT NOT NULL,        -- option instrument (dim_instrument)
    party_sk                    BIGINT NOT NULL,
    trader_sk                   BIGINT,
    desk_sk                     BIGINT,
    account_sk                  BIGINT,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT NOT NULL,
    -- option order specifics
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
    -- CAT IM v4.1.0r15 spec-mapping columns added in Tier 17.4 -- see 06_cat_field_mapping.csv
    -- order key + timestamps
    order_key_date              TIMESTAMP NOT NULL,     -- section 5.1.1 row 6 orderKeyDate
    event_timestamp             TIMESTAMP NOT NULL,     -- section 5.1.1 row 9 eventTimestamp
    -- manual event flags
    manual_flag                 BOOLEAN NOT NULL,       -- section 5.1.1 row 10 manualFlag
    manual_order_key_date       TIMESTAMP,              -- section 5.1.1 row 11 manualOrderKeyDate
    manual_order_id             STRING,                 -- section 5.1.1 row 12 manualOrderID
    electronic_dup_flag         BOOLEAN NOT NULL,       -- section 5.1.1 row 13 electronicDupFlag
    electronic_timestamp        TIMESTAMP,              -- section 5.1.1 row 14 electronicTimestamp
    -- order metadata
    dept_type                   STRING NOT NULL,        -- section 5.1.1 row 15 deptType
    min_qty                     DECIMAL(38, 18),        -- section 5.1.1 row 19 minQty
    solicitation_flag           BOOLEAN NOT NULL,       -- section 5.1.1 row 28 solicitationFlag
    rfq_id                      STRING,                 -- section 5.1.1 row 32 RFQID
    representative_ind          STRING,                 -- section 5.1.1 row 30 representativeInd
    exch_origin_code            STRING,                 -- section 5.1.3 row 28 exchOriginCode
    -- party / account
    firm_designated_id          STRING,                 -- section 5.1.1 row 24 firmDesignatedID
    account_holder_type         STRING,                 -- section 5.1.1 row 25 accountHolderType
    affiliate_flag              BOOLEAN,                -- section 5.1.1 row 26 affiliateFlag
    -- routing
    sender_imid                 STRING,                 -- section 5.1.3 row 14 senderIMID
    receiver_imid               STRING,                 -- section 5.1.4 row 15 receiverIMID
    sender_type                 STRING NOT NULL,        -- section 5.1.4 row 17 senderType
    originating_imid            STRING,                 -- section 5.1.3 row 9 originatingIMID
    destination                 STRING NOT NULL,        -- section 5.1.3 row 15 destination
    destination_type            STRING NOT NULL,        -- section 5.1.3 row 16 destinationType
    routed_order_id             STRING,                 -- section 5.1.3 row 17 routedOrderID
    session                     STRING,                 -- section 5.1.3 row 18 session
    route_rejected_flag         BOOLEAN,                -- section 5.1.3 row 27 routeRejectedFlag
    multi_leg_ind               BOOLEAN,                -- section 5.1.3 row 30 multiLegInd
    paired_order_id             STRING,                 -- section 5.1.3 row 34 pairedOrderID
    -- modify / parent linkage
    prior_order_key_date        TIMESTAMP,              -- section 5.1.7 row 9 priorOrderKeyDate
    parent_order_key_date       TIMESTAMP,              -- section 5.1.6.1 row 9 parentOrderKeyDate
    request_timestamp           TIMESTAMP,              -- section 5.1.7 row 33 requestTimestamp
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT pk_fact_option_event PRIMARY KEY (option_event_sk),
    CONSTRAINT chk_fact_option_event_code CHECK (cat_event_code IN (
        'MONO','MONOS','MOOR','MOMR','MOCR','MOORS','MOMRS','MOCRS','MOOA',
        'MOIR','MOIM','MOIC','MOIMR','MOICR',
        'MOCO','MOCOM','MOCOC',
        'MOOM','MOOMS','MOOMR','MOOJ','MOOC','MOOCR',
        'MONQ','MORQ','MOQR','MOQC','MOQM',
        'MOOE'
    )),
    CONSTRAINT fk_fact_oe_date     FOREIGN KEY (date_sk)
        REFERENCES gold.dim_date (date_sk),
    CONSTRAINT fk_fact_oe_instr    FOREIGN KEY (instrument_sk)
        REFERENCES gold.dim_instrument (instrument_sk),
    CONSTRAINT fk_fact_oe_party    FOREIGN KEY (party_sk)
        REFERENCES gold.dim_party (party_sk),
    CONSTRAINT fk_fact_oe_evtype   FOREIGN KEY (event_type_sk)
        REFERENCES gold.dim_event_type (event_type_sk)
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'Gold fact: header-grain row per simple-option order/quote event (Section 5.1, excluding executions/allocations).';


CREATE TABLE IF NOT EXISTS gold.fact_option_executions (
    option_execution_sk         BIGINT GENERATED ALWAYS AS IDENTITY,
    event_dts                   TIMESTAMP NOT NULL,
    event_date                  DATE      NOT NULL,
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT NOT NULL,
    party_sk                    BIGINT NOT NULL,
    venue_sk                    BIGINT,
    event_type_sk               BIGINT NOT NULL,
    -- execution specifics
    option_id                   STRING NOT NULL,
    cat_event_code              STRING NOT NULL,        -- MOOT / MOOF / MOOFS / MOFA
    trade_id                    STRING,
    trade_key_date              TIMESTAMP,              -- CAT IM v4.1.0r15 §5.1.11 row 6 (MOOT)
    fulfillment_id              STRING,
    fill_key_date               TIMESTAMP,              -- CAT IM v4.1.0r15 §5.1.12.1 row 6 (MOOF/MOOFS)
    prior_fulfillment_id        STRING,
    prior_fill_key_date         TIMESTAMP,              -- CAT IM v4.1.0r15 §5.1.12.3 row 9 (MOFA)
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
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT pk_fact_option_execution PRIMARY KEY (option_execution_sk),
    CONSTRAINT chk_fact_option_exec_code CHECK (cat_event_code IN (
        'MOOT','MOOF','MOOFS','MOFA'
    )),
    CONSTRAINT fk_fact_oex_date  FOREIGN KEY (date_sk)
        REFERENCES gold.dim_date (date_sk),
    CONSTRAINT fk_fact_oex_instr FOREIGN KEY (instrument_sk)
        REFERENCES gold.dim_instrument (instrument_sk),
    CONSTRAINT fk_fact_oex_party FOREIGN KEY (party_sk)
        REFERENCES gold.dim_party (party_sk),
    CONSTRAINT fk_fact_oex_evtype FOREIGN KEY (event_type_sk)
        REFERENCES gold.dim_event_type (event_type_sk)
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'Gold fact: simple-option trades and fulfillments (MOOT / MOOF / MOOFS / MOFA).';


CREATE TABLE IF NOT EXISTS gold.fact_option_allocations (
    option_allocation_sk        BIGINT GENERATED ALWAYS AS IDENTITY,
    event_dts                   TIMESTAMP NOT NULL,
    event_date                  DATE      NOT NULL,
    date_sk                     BIGINT NOT NULL,
    instrument_sk               BIGINT NOT NULL,
    party_sk                    BIGINT NOT NULL,
    account_sk                  BIGINT,
    event_type_sk               BIGINT NOT NULL,
    -- allocation specifics
    option_id                   STRING NOT NULL,
    cat_event_code              STRING NOT NULL,        -- MOPA / MOAA
    allocation_id               STRING NOT NULL,
    allocation_key_date         TIMESTAMP NOT NULL,     -- CAT IM v4.1.0r15 §5.1.13.1 row 6 (MOPA/MOAA)
    prior_allocation_id         STRING,
    prior_allocation_key_date   TIMESTAMP,              -- CAT IM v4.1.0r15 §5.1.13.2 row 8 (MOAA)
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
    allocation_instruction_time TIMESTAMP,              -- CAT IM v4.1.0r15 §5.1.13.1 row 24 (MOPA/MOAA)
    cancel_flag                 BOOLEAN,
    cancel_timestamp            TIMESTAMP,
    -- lineage
    source_file                 STRING NOT NULL,
    source_batch_id             STRING NOT NULL,
    dv2_source_hk               STRING NOT NULL,
    quality_outcome             STRING,
    CONSTRAINT pk_fact_option_allocation PRIMARY KEY (option_allocation_sk),
    CONSTRAINT chk_fact_option_alloc_code CHECK (cat_event_code IN (
        'MOPA','MOAA'
    )),
    CONSTRAINT fk_fact_oal_date  FOREIGN KEY (date_sk)
        REFERENCES gold.dim_date (date_sk),
    CONSTRAINT fk_fact_oal_instr FOREIGN KEY (instrument_sk)
        REFERENCES gold.dim_instrument (instrument_sk),
    CONSTRAINT fk_fact_oal_party FOREIGN KEY (party_sk)
        REFERENCES gold.dim_party (party_sk),
    CONSTRAINT fk_fact_oal_evtype FOREIGN KEY (event_type_sk)
        REFERENCES gold.dim_event_type (event_type_sk)
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'Gold fact: simple-option post-trade allocations (MOPA / MOAA).';


-- Consolidated lifecycle view spanning option order + execution + allocation
CREATE OR REPLACE VIEW gold.vw_option_lifecycle AS
SELECT
    'order' AS event_family,
    e.option_event_sk AS sk,
    e.event_dts,
    e.cat_event_code,
    e.option_id,
    e.cat_order_id AS related_id,
    e.side,
    e.quantity,
    e.price
FROM gold.fact_option_order_events e
UNION ALL
SELECT
    'execution',
    x.option_execution_sk,
    x.event_dts,
    x.cat_event_code,
    x.option_id,
    COALESCE(x.trade_id, x.fulfillment_id),
    NULL,
    x.quantity,
    x.price
FROM gold.fact_option_executions x
UNION ALL
SELECT
    'allocation',
    a.option_allocation_sk,
    a.event_dts,
    a.cat_event_code,
    a.option_id,
    a.allocation_id,
    a.side,
    a.quantity,
    a.price
FROM gold.fact_option_allocations a;
