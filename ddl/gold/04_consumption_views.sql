-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 cat-pretrade-data-model contributors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- ============================================================================
-- File: 04_consumption_views.sql
-- Purpose: Gold layer - curated views for compliance, BI, and audit
-- Consumers: Power BI Direct Lake semantic model, Compliance analysts, Auditors
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. v_cat_order_reconstruction
-- Full lifecycle timeline of a single order (MEIR -> MENO -> MECO/MLCO -> -- MEOT (Trade) and MEOTS (Trade Supplement) -> MEPA/MEAA/MOFA -> any MEOJ corrections).
-- Regulatory use: SEC Rule 613 "complete lifecycle reconstruction".
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_cat_order_reconstruction AS
SELECT
 f.cat_order_id,
 f.firm_roe_id,
 f.event_date,
 f.event_timestamp,
 f.event_nanos,
 f.cat_event_code,
 et.event_name,
 et.event_phase,
 et.submission_file_type,
 p.party_id_bk AS reporting_firm_id,
 p.cat_imid AS reporting_firm_imid,
 p.legal_name AS reporting_firm_name,
 i.isin,
 i.cusip,
 i.symbol,
 i.asset_class,
 v.mic AS venue_mic,
 v.venue_name,
 a.account_id_bk,
 a.account_type,
 f.side,
 f.order_type,
 f.time_in_force,
 f.quantity,
 f.leaves_quantity,
 f.cumulative_filled_quantity,
 f.price,
 f.execution_price,
 f.execution_quantity,
 f.parent_order_id,
 f.routed_to_venue_mic,
 f.routed_to_firm_imid,
 f.original_event_ref,
 f.correction_reason,
 f.dv2_source_hk,
 -- Sequence the events inside the order
 ROW_NUMBER OVER (
 PARTITION BY f.cat_order_id
 ORDER BY f.event_timestamp, COALESCE(f.event_nanos, 0)
) AS lifecycle_sequence,
 LAG(f.cat_event_code) OVER (
 PARTITION BY f.cat_order_id
 ORDER BY f.event_timestamp, COALESCE(f.event_nanos, 0)
) AS prior_event_code,
 LEAD(f.cat_event_code) OVER (
 PARTITION BY f.cat_order_id
 ORDER BY f.event_timestamp, COALESCE(f.event_nanos, 0)
) AS next_event_code
FROM fact_cat_order_events f
JOIN dim_event_type et
 ON f.cat_event_code = et.cat_event_code
JOIN dim_party p
 ON f.dim_party_sk = p.dim_party_sk
JOIN dim_instrument i
 ON f.dim_instrument_sk = i.dim_instrument_sk
LEFT JOIN dim_venue v
 ON f.dim_venue_sk = v.dim_venue_sk
LEFT JOIN dim_account a
 ON f.dim_account_sk = a.dim_account_sk;

-- ----------------------------------------------------------------------------
-- 2. v_cat_daily_submission_status
-- Daily dashboard: per firm, per file type - submitted, accepted, rejected,
-- open errors, deadline status. Drives regulatory ops dashboard.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_cat_daily_submission_status AS
WITH batches AS (
 SELECT
 event_date,
 cat_reporter_imid,
 submission_file_type,
 COUNT(*) AS batch_count,
 SUM(record_count) AS total_records,
 SUM(records_accepted) AS total_accepted,
 SUM(records_rejected) AS total_rejected,
 SUM(records_repaired) AS total_repaired,
 MIN(submission_timestamp) AS first_submission_ts,
 MAX(submission_timestamp) AS last_submission_ts,
 MIN(submission_deadline) AS submission_deadline,
 MAX(CASE WHEN is_late_submission THEN 1 ELSE 0 END) AS any_late_flag,
 COUNT(DISTINCT CASE WHEN ack_status='ACCEPTED' THEN batch_id END) AS accepted_batches,
 COUNT(DISTINCT CASE WHEN ack_status='REJECTED' THEN batch_id END) AS rejected_batches,
 COUNT(DISTINCT CASE WHEN ack_status IN ('PENDING','PROCESSING') THEN batch_id END) AS pending_batches
 FROM cat_submission_batch
 GROUP BY event_date, cat_reporter_imid, submission_file_type
),
errors AS (
 SELECT
 event_date,
 firm_roe_id,
 cat_event_code,
 COUNT(*) AS open_error_count,
 SUM(CASE WHEN resolution_status='OPEN' THEN 1 ELSE 0 END) AS unresolved_count,
 SUM(CASE WHEN days_to_deadline < 0 THEN 1 ELSE 0 END) AS overdue_count,
 MIN(t_plus_3_deadline) AS earliest_correction_deadline
 FROM cat_feedback_error
 WHERE resolution_status IN ('OPEN','IN_PROGRESS','ESCALATED')
 GROUP BY event_date, firm_roe_id, cat_event_code
),
errors_by_file AS (
 SELECT
 e.event_date,
 et.submission_file_type,
 SUM(e.open_error_count) AS open_error_count,
 SUM(e.unresolved_count) AS unresolved_count,
 SUM(e.overdue_count) AS overdue_count,
 MIN(e.earliest_correction_deadline) AS earliest_correction_deadline
 FROM errors e
 JOIN dim_event_type et
 ON e.cat_event_code = et.cat_event_code
 GROUP BY e.event_date, et.submission_file_type
)
SELECT
 b.event_date,
 b.cat_reporter_imid,
 b.submission_file_type,
 b.batch_count,
 b.total_records,
 b.total_accepted,
 b.total_rejected,
 b.total_repaired,
 b.first_submission_ts,
 b.last_submission_ts,
 b.submission_deadline,
 b.any_late_flag,
 b.accepted_batches,
 b.rejected_batches,
 b.pending_batches,
 COALESCE(e.open_error_count, 0) AS open_error_count,
 COALESCE(e.unresolved_count, 0) AS unresolved_count,
 COALESCE(e.overdue_count, 0) AS overdue_count,
 e.earliest_correction_deadline,
 CASE
 WHEN b.any_late_flag = 1 THEN 'LATE'
 WHEN COALESCE(e.overdue_count,0) > 0 THEN 'OVERDUE_CORRECTIONS'
 WHEN b.pending_batches > 0 THEN 'PENDING'
 WHEN b.rejected_batches > 0 THEN 'REJECTED'
 WHEN b.accepted_batches = b.batch_count AND b.batch_count > 0 THEN 'GREEN'
 ELSE 'UNKNOWN'
 END AS overall_status
FROM batches b
LEFT JOIN errors_by_file e
 ON b.event_date = e.event_date
 AND b.submission_file_type = e.submission_file_type;

-- ----------------------------------------------------------------------------
-- 3. v_cat_best_execution_audit
-- Best-execution audit - executions on fact_cat_order_events with
-- NBBO-equivalent context. Drives MSRB/FINRA Rule 5310 audit.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_cat_best_execution_audit AS
SELECT
 f.cat_order_id,
 f.firm_roe_id,
 f.event_date,
 f.event_timestamp,
 f.event_nanos,
 f.cat_event_code,
 et.event_name,
 i.symbol,
 i.isin,
 i.cusip,
 i.asset_class,
 v.mic AS execution_venue_mic,
 v.venue_name AS execution_venue_name,
 v.venue_category AS venue_category,
 f.side,
 f.execution_quantity,
 f.execution_price,
 f.price AS limit_price,
 f.currency_code,
 -- Spread to limit
 CASE f.side
 WHEN 'BUY' THEN (f.execution_price - f.price)
 WHEN 'SELL' THEN (f.price - f.execution_price)
 WHEN 'SELL_SHORT' THEN (f.price - f.execution_price)
 END AS price_improvement,
 -- ISO flag per Rule 611
 f.iso_flag,
 f.short_sale_exempt_flag,
 -- Order-routing provenance
 f.parent_order_id,
 f.routed_to_venue_mic,
 f.routed_to_firm_imid,
 -- Dimension context
 p.cat_imid AS executing_firm_imid,
 p.legal_name AS executing_firm_name,
 t.cat_trader_id,
 d.desk_name,
 f.dv2_source_hk,
 f.load_date
FROM fact_cat_order_events f
JOIN dim_event_type et
 ON f.cat_event_code = et.cat_event_code
 AND et.event_phase = 'Execution'
JOIN dim_party p
 ON f.dim_party_sk = p.dim_party_sk
JOIN dim_instrument i
 ON f.dim_instrument_sk = i.dim_instrument_sk
LEFT JOIN dim_venue v
 ON f.dim_venue_sk = v.dim_venue_sk
LEFT JOIN dim_trader t
 ON f.dim_trader_sk = t.dim_trader_sk
LEFT JOIN dim_desk d
 ON f.dim_desk_sk = d.dim_desk_sk;
