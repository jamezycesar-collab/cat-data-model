-- Multi-leg option Silver + Gold DDL (Microsoft Fabric Warehouse / T-SQL).
-- Adjustments from the Delta variant:
-- * STRING -> VARCHAR(N) sized to spec maxima
-- * TIMESTAMP -> DATETIME2(7)
-- * BIGINT GENERATED ALWAYS -> IDENTITY(1,1)
-- * No PARTITIONED BY (Fabric Warehouse handles partitioning automatically)
-- * Clustered columnstore is the default; no explicit clustered key is set
-- * BOOLEAN -> BIT

CREATE TABLE silver.hub_multileg_order (
 multileg_order_hk VARCHAR(64) NOT NULL,
 multileg_order_bk_orderID VARCHAR(64) NOT NULL,
 multileg_order_bk_keyDate DATE NOT NULL,
 multileg_order_bk_imid VARCHAR(8) NOT NULL,
 load_dts DATETIME2(7) NOT NULL,
 record_source VARCHAR(64) NOT NULL,
 dv2_source_hk VARCHAR(64) NOT NULL,
 CONSTRAINT pk_hub_multileg_order PRIMARY KEY NONCLUSTERED (multileg_order_hk) NOT ENFORCED
);

CREATE TABLE silver.hub_multileg_leg (
 multileg_leg_hk VARCHAR(64) NOT NULL,
 multileg_order_hk VARCHAR(64) NOT NULL,
 leg_seq INT NOT NULL,
 load_dts DATETIME2(7) NOT NULL,
 record_source VARCHAR(64) NOT NULL,
 dv2_source_hk VARCHAR(64) NOT NULL,
 CONSTRAINT pk_hub_multileg_leg PRIMARY KEY NONCLUSTERED (multileg_leg_hk) NOT ENFORCED
);

CREATE TABLE silver.link_multileg_order_leg (
 multileg_order_leg_hk VARCHAR(64) NOT NULL,
 multileg_order_hk VARCHAR(64) NOT NULL,
 multileg_leg_hk VARCHAR(64) NOT NULL,
 instrument_hk VARCHAR(64) NOT NULL,
 leg_side VARCHAR(8) NOT NULL,
 leg_open_close_indicator VARCHAR(2),
 leg_ratio DECIMAL(10, 4),
 load_dts DATETIME2(7) NOT NULL,
 record_source VARCHAR(64) NOT NULL,
 dv2_source_hk VARCHAR(64) NOT NULL,
 CONSTRAINT pk_link_mlol PRIMARY KEY NONCLUSTERED (multileg_order_leg_hk) NOT ENFORCED
);

CREATE TABLE silver.link_multileg_event (
 multileg_event_hk VARCHAR(64) NOT NULL,
 multileg_order_hk VARCHAR(64) NOT NULL,
 event_code VARCHAR(8) NOT NULL,
 event_timestamp DATETIME2(7) NOT NULL,
 event_date DATE NOT NULL,
 event_type_hk VARCHAR(64) NOT NULL,
 venue_hk VARCHAR(64),
 sender_party_hk VARCHAR(64),
 receiver_party_hk VARCHAR(64),
 load_dts DATETIME2(7) NOT NULL,
 record_source VARCHAR(64) NOT NULL,
 dv2_source_hk VARCHAR(64) NOT NULL,
 CONSTRAINT pk_link_mle PRIMARY KEY NONCLUSTERED (multileg_event_hk) NOT ENFORCED,
 CONSTRAINT chk_link_mle_code CHECK (event_code IN (
 'MLNO','MLOR','MLMR','MLCR','MLOA','MLIR','MLIM','MLIC','MLIMR','MLICR',
 'MLCO','MLCOM','MLCOC','MLOM','MLOMR','MLOC','MLOCR',
 'MLNQ','MLRQ','MLQS','MLQR','MLQC','MLQM',
 'MLOS','MLOE'))
);

CREATE TABLE silver.link_multileg_quote (
 multileg_quote_hk VARCHAR(64) NOT NULL,
 multileg_order_hk VARCHAR(64) NOT NULL,
 quote_id VARCHAR(64) NOT NULL,
 quote_event_code VARCHAR(8) NOT NULL,
 venue_hk VARCHAR(64),
 load_dts DATETIME2(7) NOT NULL,
 record_source VARCHAR(64) NOT NULL,
 dv2_source_hk VARCHAR(64) NOT NULL,
 CONSTRAINT pk_link_mlq PRIMARY KEY NONCLUSTERED (multileg_quote_hk) NOT ENFORCED,
 CONSTRAINT chk_link_mlq_code CHECK (quote_event_code IN (
 'MLNQ','MLRQ','MLQS','MLQR','MLQC','MLQM'))
);

CREATE TABLE silver.link_multileg_child_order (
 multileg_child_order_hk VARCHAR(64) NOT NULL,
 parent_multileg_order_hk VARCHAR(64) NOT NULL,
 child_multileg_order_hk VARCHAR(64) NOT NULL,
 load_dts DATETIME2(7) NOT NULL,
 record_source VARCHAR(64) NOT NULL,
 dv2_source_hk VARCHAR(64) NOT NULL,
 CONSTRAINT pk_link_mlc PRIMARY KEY NONCLUSTERED (multileg_child_order_hk) NOT ENFORCED
);

CREATE TABLE silver.sat_multileg_order_state (
 multileg_order_hk VARCHAR(64) NOT NULL,
 load_dts DATETIME2(7) NOT NULL,
 load_end_dts DATETIME2(7),
 hash_diff VARCHAR(64) NOT NULL,
 order_status VARCHAR(32),
 order_quantity DECIMAL(38, 18),
 limit_price DECIMAL(38, 18),
 net_price DECIMAL(38, 18),
 time_in_force VARCHAR(8),
 handling_instructions VARCHAR(32),
 representative_ind BIT,
 aggregated_orders_json VARCHAR(MAX),
 record_source VARCHAR(64) NOT NULL,
 CONSTRAINT pk_sat_mlo PRIMARY KEY NONCLUSTERED (multileg_order_hk, load_dts) NOT ENFORCED
);

CREATE TABLE silver.sat_multileg_leg_state (
 multileg_leg_hk VARCHAR(64) NOT NULL,
 load_dts DATETIME2(7) NOT NULL,
 load_end_dts DATETIME2(7),
 hash_diff VARCHAR(64) NOT NULL,
 leg_quantity DECIMAL(38, 18),
 leg_price DECIMAL(38, 18),
 leg_open_close VARCHAR(2),
 leg_ratio DECIMAL(10, 4),
 leg_status VARCHAR(32),
 record_source VARCHAR(64) NOT NULL,
 CONSTRAINT pk_sat_mll PRIMARY KEY NONCLUSTERED (multileg_leg_hk, load_dts) NOT ENFORCED
);

CREATE TABLE silver.sat_multileg_event_state (
 multileg_event_hk VARCHAR(64) NOT NULL,
 load_dts DATETIME2(7) NOT NULL,
 load_end_dts DATETIME2(7),
 hash_diff VARCHAR(64) NOT NULL,
 event_qty DECIMAL(38, 18),
 event_price DECIMAL(38, 18),
 route_dest_imid VARCHAR(8),
 rejected_flag BIT,
 cancel_reason VARCHAR(64),
 raw_payload_json VARCHAR(MAX),
 record_source VARCHAR(64) NOT NULL,
 CONSTRAINT pk_sat_mle PRIMARY KEY NONCLUSTERED (multileg_event_hk, load_dts) NOT ENFORCED
);

-- ===== Gold layer =====

CREATE TABLE gold.dim_multileg_strategy (
 multileg_strategy_sk BIGINT IDENTITY(1,1) NOT NULL,
 multileg_strategy_bk VARCHAR(64) NOT NULL,
 strategy_name VARCHAR(128) NOT NULL,
 leg_count_min INT NOT NULL,
 leg_count_max INT NOT NULL,
 description VARCHAR(MAX),
 effective_dts DATETIME2(7) NOT NULL,
 end_dts DATETIME2(7) NOT NULL,
 is_current BIT NOT NULL,
 CONSTRAINT pk_dim_mls PRIMARY KEY NONCLUSTERED (multileg_strategy_sk) NOT ENFORCED
);

CREATE TABLE gold.fact_multileg_option_events (
 multileg_event_sk BIGINT IDENTITY(1,1) NOT NULL,
 event_dts DATETIME2(7) NOT NULL,
 event_date DATE NOT NULL,
 date_sk BIGINT NOT NULL,
 instrument_sk BIGINT,
 party_sk BIGINT NOT NULL,
 trader_sk BIGINT,
 desk_sk BIGINT,
 account_sk BIGINT,
 venue_sk BIGINT,
 event_type_sk BIGINT NOT NULL,
 multileg_strategy_sk BIGINT,
 multileg_order_id VARCHAR(64) NOT NULL,
 parent_multileg_order_id VARCHAR(64),
 leg_count INT NOT NULL,
 cat_event_code VARCHAR(8) NOT NULL,
 side_summary VARCHAR(8),
 net_price DECIMAL(38, 18),
 order_quantity DECIMAL(38, 18),
 time_in_force VARCHAR(8),
 handling_instructions VARCHAR(32),
 -- CAT IM v4.1.0r15 spec-mapping columns added in Tier 17.5
 underlying VARCHAR(14) NOT NULL,        -- section 5.2.1 row 8 underlying
 order_key_date DATETIME2(7) NOT NULL,   -- section 5.2.1 row 6 orderKeyDate
 cat_order_id VARCHAR(64) NOT NULL,      -- section 5.2.1 row 7 orderID
 event_timestamp DATETIME2(7) NOT NULL,  -- section 5.2.1 row 9 eventTimestamp
 manual_flag BIT NOT NULL,               -- section 5.2.1 row 10 manualFlag
 manual_order_key_date DATETIME2(7),     -- section 5.2.1 row 11 manualOrderKeyDate
 manual_order_id VARCHAR(64),            -- section 5.2.1 row 12 manualOrderID
 electronic_dup_flag BIT NOT NULL,       -- section 5.2.1 row 13 electronicDupFlag
 electronic_timestamp DATETIME2(7),      -- section 5.2.1 row 14 electronicTimestamp
 dept_type VARCHAR(16) NOT NULL,         -- section 5.2.1 row 15 deptType
 price DECIMAL(38, 18),                  -- section 5.2.1 row 16 net price
 quantity DECIMAL(38, 18) NOT NULL,      -- section 5.2.1 row 17 quantity
 min_qty DECIMAL(38, 18),                -- section 5.2.1 row 18 minQty
 order_type VARCHAR(12) NOT NULL,        -- section 5.2.1 row 19 orderType
 trading_session VARCHAR(16) NOT NULL,   -- section 5.2.1 row 21 tradingSession
 firm_designated_id VARCHAR(40),         -- section 5.2.1 row 23 firmDesignatedID
 account_holder_type VARCHAR(2),         -- section 5.2.1 row 24 accountHolderType
 affiliate_flag BIT,                     -- section 5.2.1 row 25 affiliateFlag
 representative_ind VARCHAR(4),          -- section 5.2.1 row 27 representativeInd
 solicitation_flag BIT NOT NULL,         -- section 5.2.1 row 28 solicitationFlag
 rfq_id VARCHAR(64),                     -- section 5.2.1 row 29 RFQID
 number_of_legs INT NOT NULL,            -- section 5.2.1 row 30 numberOfLegs
 sender_imid VARCHAR(8),                 -- section 5.2.2 row 14 senderIMID
 destination VARCHAR(8),                 -- section 5.2.2 row 15 destination
 destination_type VARCHAR(4),            -- section 5.2.2 row 16 destinationType
 routed_order_id VARCHAR(64),            -- section 5.2.2 row 17 routedOrderID
 session VARCHAR(40),                    -- section 5.2.2 row 18 session
 route_rejected_flag BIT,                -- section 5.2.2 row 27 routeRejectedFlag
 exch_origin_code VARCHAR(4),            -- section 5.2.2 row 28 exchOriginCode
 paired_order_id VARCHAR(64),            -- section 5.2.2 row 34 pairedOrderID
 receiver_imid VARCHAR(8),               -- section 5.2.3 row 15 receiverIMID
 sender_type VARCHAR(4),                 -- section 5.2.3 row 17 senderType
 originating_imid VARCHAR(8),            -- section 5.2.2 row 9 originatingIMID
 prior_order_key_date DATETIME2(7),      -- section 5.2.6 row 9 priorOrderKeyDate
 prior_order_id VARCHAR(64),             -- section 5.2.6 row 10 priorOrderID
 parent_order_key_date DATETIME2(7),     -- section 5.2.5.1 row 9 parentOrderKeyDate
 parent_order_id VARCHAR(64),            -- section 5.2.5.1 row 10 parentOrderID
 initiator VARCHAR(4),                   -- section 5.2.6 row 22 initiator
 leaves_qty DECIMAL(38, 18),             -- section 5.2.6 row 27 leavesQty
 cancel_qty DECIMAL(38, 18),             -- section 5.2.7 row 13 cancelQty
 request_timestamp DATETIME2(7),         -- section 5.2.6 row 33 requestTimestamp
 quote_key_date DATETIME2(7),            -- section 5.2.8.1 row 6 quoteKeyDate
 quote_id VARCHAR(64),                   -- section 5.2.8.1 row 7 quoteID
 bid_price DECIMAL(38, 18),              -- section 5.2.8.1 row 11 bidPrice
 ask_price DECIMAL(38, 18),              -- section 5.2.8.1 row 13 askPrice
 bid_qty DECIMAL(38, 18),                -- section 5.2.8.1 row 12 bidQty
 ask_qty DECIMAL(38, 18),                -- section 5.2.8.1 row 14 askQty
 routed_quote_id VARCHAR(64),            -- section 5.2.8.2 row 12 routedQuoteID
 quote_rejected_flag BIT,                -- section 5.2.8.2 row 18 quoteRejectedFlag
 prior_quote_key_date DATETIME2(7),      -- section 5.2.8.6 row 9 priorQuoteKeyDate
 prior_quote_id VARCHAR(64),             -- section 5.2.8.6 row 10 priorQuoteID
 source_file VARCHAR(256) NOT NULL,
 source_batch_id VARCHAR(64) NOT NULL,
 dv2_source_hk VARCHAR(64) NOT NULL,
 quality_outcome VARCHAR(16),
 CONSTRAINT pk_fact_mle PRIMARY KEY NONCLUSTERED (multileg_event_sk) NOT ENFORCED,
 CONSTRAINT chk_fact_mle_code CHECK (cat_event_code IN (
 'MLNO','MLOR','MLMR','MLCR','MLOA','MLIR','MLIM','MLIC','MLIMR','MLICR',
 'MLCO','MLCOM','MLCOC','MLOM','MLOMR','MLOC','MLOCR',
 'MLNQ','MLRQ','MLQS','MLQR','MLQC','MLQM',
 'MLOS','MLOE')),
 CONSTRAINT chk_fact_mle_legs CHECK (leg_count >= 2)
);

CREATE TABLE gold.fact_multileg_option_legs (
 multileg_leg_event_sk BIGINT IDENTITY(1,1) NOT NULL,
 multileg_event_sk BIGINT NOT NULL,
 leg_seq INT NOT NULL, -- the leg index n in CAT IM legs array
 instrument_sk BIGINT NOT NULL, -- FK to option contract or covered stock
 -- CAT IM v4.1.0r15 section 5.2.1 row 32 legs sub-table: rows 32.n.1 - 32.n.6
 leg_ref_id VARCHAR(64), -- section 5.2.1 row 32.n.1 legRefID unique identifier of leg
 leg_symbol VARCHAR(14), -- section 5.2.1 row 32.n.2 symbol of stock leg
 leg_option_id VARCHAR(22), -- section 5.2.1 row 32.n.3 optionID OSI symbol of option leg
 leg_open_close_indicator VARCHAR(2), -- section 5.2.1 row 32.n.4 openCloseIndicator O C NULL
 leg_side VARCHAR(8) NOT NULL, -- section 5.2.1 row 32.n.5 side BUY SELL
 leg_ratio_quantity DECIMAL(10, 4) NOT NULL, -- section 5.2.1 row 32.n.6 legRatioQuantity ratio
 leg_quantity DECIMAL(38, 18) NOT NULL,
 leg_price DECIMAL(38, 18),
 leg_status VARCHAR(32),
 source_file VARCHAR(256) NOT NULL,
 source_batch_id VARCHAR(64) NOT NULL,
 CONSTRAINT pk_fact_mlel PRIMARY KEY NONCLUSTERED (multileg_leg_event_sk) NOT ENFORCED,
 CONSTRAINT chk_fact_mlel_side CHECK (leg_side IN ('BUY','SELL'))
);

GO

CREATE OR ALTER VIEW gold.vw_multileg_option_lifecycle AS
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
JOIN gold.fact_multileg_option_legs l ON h.multileg_event_sk = l.multileg_event_sk
JOIN gold.dim_party p ON h.party_sk = p.party_sk
JOIN gold.dim_event_type et ON h.event_type_sk = et.event_type_sk
JOIN gold.dim_instrument i ON l.instrument_sk = i.instrument_sk;
GO
