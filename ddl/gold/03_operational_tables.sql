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
-- File: 03_operational_tables.sql
-- Purpose: Gold layer - operational tables for CAT submission dispatch + feedback
-- Consumers: 07_submission_generator.py + regulatory ops dashboards
-- Deadlines: - Submission: 08:00 ET on T+1 (FINRA CAT Reporting Technical Specs)
-- - Error correction: T+3
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. cat_submission_batch - One row per CAT file submitted to the Plan Processor
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cat_submission_batch (
 batch_id STRING NOT NULL COMMENT 'UUID assigned at file generation',
 event_date DATE NOT NULL COMMENT 'CAT activity date - partition key',
 cat_reporter_imid STRING NOT NULL COMMENT 'Firm IMID per FINRA CAT registration',
 submission_file_type STRING NOT NULL COMMENT 'OrderEvents | QuoteEvents | Allocations | CAIS',
 submission_cycle STRING NOT NULL COMMENT 'INITIAL | CORRECTION | REPAIR | LATE',
 correction_cycle_number INT COMMENT 'Sequence for correction files',
 file_name STRING NOT NULL COMMENT 'Format: <IMID>_<Type>_<YYYYMMDD>_<Seq>.json.bz2',
 file_size_bytes BIGINT,
 compressed_size_bytes BIGINT COMMENT 'Post-BZip2',
 compression_algorithm STRING COMMENT 'BZIP2 (required by CAT)',
 record_count BIGINT NOT NULL COMMENT 'Number of JSON event objects',
 min_event_timestamp TIMESTAMP,
 max_event_timestamp TIMESTAMP,
 sha256_checksum STRING NOT NULL COMMENT 'SHA-256 digest of uncompressed JSON payload',
 generation_started_at TIMESTAMP,
 generation_completed_at TIMESTAMP,
 generation_duration_seconds INT,
 submission_timestamp TIMESTAMP COMMENT 'When file PUT to CAT SFTP',
 submission_method STRING COMMENT 'SFTP | HTTPS | MFT',
 target_endpoint STRING COMMENT 'CAT SFTP hostname',
 submission_attempt_number INT COMMENT '1-based retry counter',
 ack_timestamp TIMESTAMP COMMENT 'When CAT receipt acknowledged',
 ack_status STRING NOT NULL COMMENT 'PENDING | ACCEPTED | PARTIAL | REJECTED | PROCESSING',
 ack_reference_id STRING COMMENT 'CAT-assigned submission reference',
 rejection_reason STRING COMMENT 'If ack_status=REJECTED',
 submission_deadline TIMESTAMP NOT NULL COMMENT '08:00 ET on T+1 - hard regulatory deadline',
 is_late_submission BOOLEAN COMMENT 'TRUE if submitted after deadline',
 records_accepted BIGINT,
 records_rejected BIGINT,
 records_repaired BIGINT,
 cat_submission_version STRING COMMENT 'CAT Tech Specs version (e.g. "4.3")',
 submitted_by STRING COMMENT 'Service account or user',
 dv2_source_hk STRING COMMENT 'Aggregated Silver lineage reference',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'CAT submission batch tracking - one row per file sent to the CAT Plan Processor'
TBLPROPERTIES (
 'delta.enableChangeDataFeed'='true',
 'compression.codec'='zstd',
 'subject_area'='gold_ops',
 'grain'='one row per submission attempt'
);

-- ----------------------------------------------------------------------------
-- 2. cat_feedback_error - One row per rejected/flagged record from CAT feedback
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cat_feedback_error (
 error_id STRING NOT NULL COMMENT 'UUID assigned at feedback ingestion',
 feedback_date DATE NOT NULL COMMENT 'Date the feedback file was received - partition key',
 batch_id STRING NOT NULL COMMENT 'FK -> cat_submission_batch.batch_id',
 event_date DATE NOT NULL COMMENT 'Original activity date of errored event',
 firm_roe_id STRING NOT NULL COMMENT 'Original firmROEID from the rejected event',
 cat_order_id STRING COMMENT 'Original orderID from rejected event',
 cat_event_code STRING COMMENT 'Event type of rejected record',
 error_code STRING NOT NULL COMMENT 'CAT error code (e.g. "E001", "E150")',
 error_severity STRING NOT NULL COMMENT 'REJECT | WARNING | REPAIR_REQUIRED',
 error_category STRING COMMENT 'SYNTAX | LINKAGE | REFERENCE | BUSINESS | TIMING',
 error_description STRING NOT NULL COMMENT 'Human-readable error message from CAT',
 field_name STRING COMMENT 'JSON field that failed validation',
 field_value STRING COMMENT 'Offending value (PII redacted for CAIS)',
 expected_value STRING COMMENT 'What CAT expected',
 detected_timestamp TIMESTAMP NOT NULL COMMENT 'When the feedback was received',
 t_plus_3_deadline DATE NOT NULL COMMENT 'T+3 error correction hard deadline',
 days_to_deadline INT COMMENT 'Business days remaining (negative = overdue)',
 resolution_status STRING NOT NULL COMMENT 'OPEN | IN_PROGRESS | RESOLVED | SUPPRESSED | ESCALATED',
 resolution_action STRING COMMENT 'REPAIR_FILE | NEW_EVENT | MANUAL | NO_ACTION',
 resolution_batch_id STRING COMMENT 'FK to the corrective cat_submission_batch',
 resolution_timestamp TIMESTAMP,
 resolved_by STRING,
 resolution_notes STRING,
 escalation_level INT COMMENT '0=none, 1=ops, 2=compliance, 3=exec',
 escalation_timestamp TIMESTAMP,
 regulatory_impact STRING COMMENT 'NONE | MATERIAL | REPORTABLE_BREACH',
 dv2_source_hk STRING COMMENT 'Silver lineage of original event',
 load_date TIMESTAMP NOT NULL,
 record_source STRING NOT NULL
)
USING DELTA
PARTITIONED BY (feedback_date)
COMMENT 'CAT feedback/error records - one row per rejected or flagged event, tracked to T+3 resolution'
TBLPROPERTIES (
 'delta.enableChangeDataFeed'='true',
 'compression.codec'='zstd',
 'subject_area'='gold_ops',
 'grain'='one row per error per feedback cycle'
);

-- ----------------------------------------------------------------------------
-- Z-ORDER and indexing hints
-- ----------------------------------------------------------------------------
-- OPTIMIZE cat_submission_batch ZORDER BY (cat_reporter_imid, submission_file_type, ack_status);
-- OPTIMIZE cat_feedback_error ZORDER BY (resolution_status, error_severity, t_plus_3_deadline);
