# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 cat-pretrade-data-model contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Serialize Gold facts into FINRA CAT-conformant JSON, BZip2-compress, and SFTP
to the CAT Plan Processor. Runs daily at 06:00 ET on T+1 against Databricks,
with the 08:00 ET deadline as a hard cutoff. Reads the four fact tables and
the four dims for denormalisation; writes one compressed file per
(reporter_imid, file_type, cycle) and one row per file in cat_submission_batch.
Produces camelCase JSON per CAT Tech Specs 4.3, nanosecond timestamps on
MEOTQ, SHA-256 checksum of the uncompressed payload, ISO 8601 timestamps
with explicit offsets.
"""
from __future__ import annotations

import argparse
import bz2
import hashlib
import io
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Iterable, Iterator

import paramiko
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
LOG = logging.getLogger("cat_submission_generator")
LOG.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SubmissionConfig:
 reporter_imid: str
 event_date: date
 submission_file_types: tuple[str,...] = (
 "OrderEvents", "Allocations", "QuoteEvents", "CAIS",
)
 submission_cycle: str = "INITIAL" # INITIAL | CORRECTION | REPAIR | LATE
 correction_cycle_number: int | None = None
 target_hostname: str = "sftp.catnmsplan.com"
 target_port: int = 22
 target_username: str = "" # loaded from secret scope
 target_private_key_path: str = "" # loaded from secret scope
 remote_incoming_path: str = "/incoming"
 deadline_iso: str = "" # 08:00 ET T+1 computed upstream
 cat_spec_version: str = "4.3"
 max_records_per_file: int = 1_000_000 # CAT recommended max
 dry_run: bool = False

@dataclass
class BatchResult:
 batch_id: str
 submission_file_type: str
 file_name: str
 record_count: int
 file_size_bytes: int
 compressed_size_bytes: int
 sha256_checksum: str
 submission_timestamp: datetime | None
 ack_status: str
 min_event_timestamp: datetime | None = None
 max_event_timestamp: datetime | None = None
 rejection_reason: str | None = None
 generation_duration_seconds: int = 0
 dv2_source_hks: list[str] = field(default_factory=list)

# ---------------------------------------------------------------------------
# Spark selection - build CAT-shaped rows from Gold
# ---------------------------------------------------------------------------
def select_order_events(spark: SparkSession, cfg: SubmissionConfig) -> DataFrame:
 """Join fact_cat_order_events with dim_party / dim_instrument / dim_venue /
 dim_event_type / dim_account / dim_trader to produce CAT-shaped rows."""
 return (
 spark.table("gold.fact_cat_order_events").alias("f").filter((F.col("f.event_date") == F.lit(cfg.event_date))).join(
 spark.table("gold.dim_party").alias("p"),
 (F.col("f.dim_party_sk") == F.col("p.dim_party_sk")) & F.col("p.is_current"),
 "inner",
).filter(F.col("p.cat_imid") == F.lit(cfg.reporter_imid)).join(
 spark.table("gold.dim_instrument").alias("i"),
 (F.col("f.dim_instrument_sk") == F.col("i.dim_instrument_sk")) & F.col("i.is_current"),
 "inner",
).join(
 spark.table("gold.dim_event_type").alias("et"),
 F.col("f.cat_event_code") == F.col("et.cat_event_code"),
 "inner",
).join(
 spark.table("gold.dim_venue").alias("v"),
 (F.col("f.dim_venue_sk") == F.col("v.dim_venue_sk")) & F.col("v.is_current"),
 "left",
).join(
 spark.table("gold.dim_account").alias("a"),
 (F.col("f.dim_account_sk") == F.col("a.dim_account_sk")) & F.col("a.is_current"),
 "left",
).join(
 spark.table("gold.dim_trader").alias("t"),
 (F.col("f.dim_trader_sk") == F.col("t.dim_trader_sk")) & F.col("t.is_current"),
 "left",
).select(
 F.col("f.firm_roe_id").alias("firmROEID"),
 F.col("f.cat_order_id").alias("orderID"),
 F.col("f.cat_event_code").alias("eventType"),
 F.col("p.cat_imid").alias("firmIMID"),
 F.col("i.symbol").alias("symbol"),
 F.col("i.security_id").alias("securityID"),
 F.col("i.security_id_source").alias("securityIDSource"),
 F.col("v.mic").alias("marketCenterID"),
 F.col("a.account_id_bk").alias("accountHolderType"),
 F.col("t.cat_trader_id").alias("representativeID"),
 F.col("f.event_timestamp").alias("eventTimestamp"),
 F.col("f.event_nanos").alias("nanosecondPrecision"),
 F.col("f.received_timestamp").alias("receiveDateTime"),
 F.col("f.side").alias("side"),
 F.col("f.order_type").alias("orderType"),
 F.col("f.order_capacity").alias("orderCapacity"),
 F.col("f.time_in_force").alias("timeInForce"),
 F.col("f.quantity").alias("quantity"),
 F.col("f.leaves_quantity").alias("leavesQty"),
 F.col("f.cumulative_filled_quantity").alias("cumQty"),
 F.col("f.price").alias("price"),
 F.col("f.execution_price").alias("executionPrice"),
 F.col("f.execution_quantity").alias("executionQuantity"),
 F.col("f.currency_code").alias("currencyCode"),
 F.col("f.handling_instructions").alias("handlingInstructions"),
 F.col("f.special_handling_codes").alias("specialHandlingCode"),
 F.col("f.display_instruction").alias("displayInstruction"),
 F.col("f.iso_flag").alias("isoFlag"),
 F.col("f.short_sale_exempt_flag").alias("shortSaleExempt"),
 F.col("f.solicited_flag").alias("solicitedFlag"),
 F.col("f.directed_flag").alias("directedOrderFlag"),
 F.col("f.parent_order_id").alias("parentOrderID"),
 F.col("f.routed_to_venue_mic").alias("destinationMIC"),
 F.col("f.routed_to_firm_imid").alias("destinationIMID"),
 F.col("f.original_event_ref").alias("originalFirmROEID"),
 F.col("f.correction_reason").alias("correctionReason"),
 F.col("f.cancel_reason").alias("cancelReason"),
 F.col("f.rejection_reason").alias("rejectionReason"),
 F.col("f.dv2_source_hk").alias("_dv2_source_hk"),
)
)

def select_allocations(spark: SparkSession, cfg: SubmissionConfig) -> DataFrame:
 return (
 spark.table("gold.fact_cat_allocations").alias("f").filter(F.col("f.event_date") == F.lit(cfg.event_date)).join(
 spark.table("gold.dim_party").alias("p"),
 (F.col("f.dim_party_sk") == F.col("p.dim_party_sk")) & F.col("p.is_current"),
).filter(F.col("p.cat_imid") == F.lit(cfg.reporter_imid)).join(
 spark.table("gold.dim_account").alias("a"),
 (F.col("f.dim_account_sk") == F.col("a.dim_account_sk")) & F.col("a.is_current"),
).select(
 F.col("f.firm_roe_id").alias("firmROEID"),
 F.col("f.cat_order_id").alias("orderID"),
 F.col("f.cat_allocation_id").alias("allocationID"),
 F.col("f.cat_event_code").alias("eventType"),
 F.col("p.cat_imid").alias("firmIMID"),
 F.col("a.account_id_bk").alias("accountID"),
 F.col("f.parent_execution_id").alias("executionID"),
 F.col("f.allocation_method").alias("allocationMethod"),
 F.col("f.allocation_status").alias("allocationStatus"),
 F.col("f.allocated_quantity").alias("allocatedQuantity"),
 F.col("f.allocated_price").alias("allocatedPrice"),
 F.col("f.allocation_pct").alias("allocationPercent"),
 F.col("f.gross_amount").alias("grossAmount"),
 F.col("f.commission").alias("commission"),
 F.col("f.net_amount").alias("netAmount"),
 F.col("f.settlement_date").alias("settlementDate"),
 F.col("f.affirmation_timestamp").alias("affirmationTimestamp"),
 F.col("f.allocation_timestamp").alias("eventTimestamp"),
 F.col("f.dv2_source_hk").alias("_dv2_source_hk"),
)
)

def select_quotes(spark: SparkSession, cfg: SubmissionConfig) -> DataFrame:
 return (
 spark.table("gold.fact_cat_quotes").alias("f").filter(F.col("f.event_date") == F.lit(cfg.event_date)).join(
 spark.table("gold.dim_party").alias("p"),
 (F.col("f.dim_party_sk") == F.col("p.dim_party_sk")) & F.col("p.is_current"),
).filter(F.col("p.cat_imid") == F.lit(cfg.reporter_imid)).join(
 spark.table("gold.dim_instrument").alias("i"),
 (F.col("f.dim_instrument_sk") == F.col("i.dim_instrument_sk")) & F.col("i.is_current"),
).join(
 spark.table("gold.dim_venue").alias("v"),
 (F.col("f.dim_venue_sk") == F.col("v.dim_venue_sk")) & F.col("v.is_current"),
 "left",
).select(
 F.col("f.firm_roe_id").alias("firmROEID"),
 F.col("f.cat_quote_id").alias("quoteID"),
 F.col("f.cat_rfq_id").alias("rfqID"),
 F.col("f.cat_event_code").alias("eventType"),
 F.col("p.cat_imid").alias("firmIMID"),
 F.col("i.symbol").alias("symbol"),
 F.col("i.security_id").alias("securityID"),
 F.col("v.mic").alias("marketCenterID"),
 F.col("f.quote_side").alias("quoteSide"),
 F.col("f.bid_price").alias("bidPrice"),
 F.col("f.ask_price").alias("askPrice"),
 F.col("f.bid_size").alias("bidSize"),
 F.col("f.ask_size").alias("askSize"),
 F.col("f.currency_code").alias("currencyCode"),
 F.col("f.quote_status").alias("quoteStatus"),
 F.col("f.quote_type").alias("quoteType"),
 F.col("f.min_quote_life_ms").alias("minQuoteLife"),
 F.col("f.quote_expiry_timestamp").alias("quoteExpiryTimestamp"),
 F.col("f.rfq_expiry_timestamp").alias("rfqExpiryTimestamp"),
 F.col("f.quote_timestamp").alias("eventTimestamp"),
 F.col("f.dv2_source_hk").alias("_dv2_source_hk"),
)
)

def select_cais(spark: SparkSession, cfg: SubmissionConfig) -> DataFrame:
 return (
 spark.table("gold.fact_cat_customer_records").alias("f").filter(F.col("f.snapshot_date") == F.lit(cfg.event_date)).join(
 spark.table("gold.dim_party").alias("p"),
 (F.col("f.dim_party_sk") == F.col("p.dim_party_sk")) & F.col("p.is_current"),
).filter(F.col("p.cat_imid") == F.lit(cfg.reporter_imid)).select(
 F.col("f.firm_roe_id").alias("submissionID"),
 F.col("f.fdid").alias("fdid"),
 F.col("f.cat_event_code").alias("recordType"),
 F.col("f.action_type").alias("action"),
 F.col("f.customer_type").alias("customerType"),
 F.col("f.natural_person_flag").alias("naturalPersonFlag"),
 F.col("f.primary_identifier_type").alias("primaryIDType"),
 F.col("f.primary_identifier_hash").alias("primaryIDHash"),
 F.col("f.birth_year").alias("birthYear"),
 F.col("f.residence_country").alias("residenceCountry"),
 F.col("f.postal_code").alias("postalCode"),
 F.col("f.account_type").alias("accountType"),
 F.col("f.account_status").alias("accountStatus"),
 F.col("f.account_open_date").alias("accountOpenDate"),
 F.col("f.account_close_date").alias("accountCloseDate"),
 F.col("f.trading_authorization").alias("tradingAuthorization"),
 F.col("f.authorized_trader_fdid").alias("authorizedTraderFDID"),
 F.col("f.relationship_start_date").alias("relationshipStartDate"),
 F.col("f.relationship_end_date").alias("relationshipEndDate"),
 F.col("f.large_trader_flag").alias("largeTraderFlag"),
 F.col("f.large_trader_id").alias("largeTraderID"),
 F.col("f.dv2_source_hk").alias("_dv2_source_hk"),
)
)

SELECTOR_BY_FILE_TYPE = {
 "OrderEvents": select_order_events,
 "Allocations": select_allocations,
 "QuoteEvents": select_quotes,
 "CAIS": select_cais,
}

# ---------------------------------------------------------------------------
# JSON serialization
# ---------------------------------------------------------------------------
def _json_default(obj: object) -> str:
 """Render dates/datetimes/decimals in CAT-compliant ISO 8601 with UTC offset."""
 if isinstance(obj, datetime):
 if obj.tzinfo is None:
 obj = obj.replace(tzinfo=timezone.utc)
 return obj.isoformat(timespec="microseconds")
 if isinstance(obj, date):
 return obj.isoformat
 try:
 return str(obj)
 except Exception: # pragma: no cover
 raise TypeError(f"Cannot serialize {type(obj).__name__}: {obj!r}")

def _drop_nulls(row_dict: dict) -> dict:
 """Remove keys whose value is None. CAT specs require absent fields,
 not null-valued ones, for non-required attributes."""
 return {k: v for k, v in row_dict.items if v is not None}

def rows_to_json_lines(rows: Iterable[dict], file_type: str) -> Iterator[str]:
 """Yield one JSON object per line. file_type is embedded as a wrapper only
 in the header record - body records are flat per CAT spec."""
 for row in rows:
 # Pop lineage column before serialization (not a CAT field)
 row.pop("_dv2_source_hk", None)
 yield json.dumps(_drop_nulls(row), default=_json_default, separators=(",", ":"))

# ---------------------------------------------------------------------------
# File assembly + BZip2 compression + checksum
# ---------------------------------------------------------------------------
def build_submission_payload(
 file_type: str,
 header: dict,
 body_iter: Iterator[str],
) -> tuple[bytes, str, int, int]:
 """Assemble uncompressed payload, compute SHA-256, BZip2 compress.
 Returns (compressed_bytes, sha256_hex, uncompressed_size, record_count).
 """
 buf = io.BytesIO
 record_count = 0
 # Header line
 buf.write(json.dumps(header, default=_json_default, separators=(",", ":")).encode("utf-8"))
 buf.write(b"\n")
 for line in body_iter:
 buf.write(line.encode("utf-8"))
 buf.write(b"\n")
 record_count += 1

 uncompressed = buf.getvalue
 uncompressed_size = len(uncompressed)
 sha256_hex = hashlib.sha256(uncompressed).hexdigest
 compressed = bz2.compress(uncompressed, compresslevel=9)
 return compressed, sha256_hex, uncompressed_size, record_count

def make_file_name(reporter_imid: str, file_type: str, event_date: date, seq: int) -> str:
 return f"{reporter_imid}_{file_type}_{event_date.strftime('%Y%m%d')}_{seq:04d}.json.bz2"

# ---------------------------------------------------------------------------
# SFTP upload
# ---------------------------------------------------------------------------
def sftp_put(cfg: SubmissionConfig, local_bytes: bytes, remote_filename: str) -> None:
 """Upload `local_bytes` to CAT SFTP. Raises on failure."""
 if cfg.dry_run:
 LOG.info("DRY RUN - would upload %s (%d bytes)", remote_filename, len(local_bytes))
 return

 pkey = paramiko.RSAKey.from_private_key_file(cfg.target_private_key_path)
 with paramiko.Transport((cfg.target_hostname, cfg.target_port)) as transport:
 transport.connect(username=cfg.target_username, pkey=pkey)
 sftp = paramiko.SFTPClient.from_transport(transport)
 remote_path = os.path.join(cfg.remote_incoming_path, remote_filename)
 with sftp.file(remote_path, "wb") as remote_f:
 remote_f.write(local_bytes)
 sftp.close

# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def generate_and_submit(
 spark: SparkSession,
 cfg: SubmissionConfig,
) -> list[BatchResult]:
 """Generate one file per CAT submission file type, upload, record batches."""
 results: list[BatchResult] = []
 for file_type in cfg.submission_file_types:
 selector = SELECTOR_BY_FILE_TYPE[file_type]
 df = selector(spark, cfg)
 records = df.toPandas.to_dict(orient="records")
 if not records:
 LOG.info("No records for %s on %s - skipping", file_type, cfg.event_date)
 continue

 dv2_hks = [r.get("_dv2_source_hk") for r in records if r.get("_dv2_source_hk")]
 event_timestamps = [
 r.get("eventTimestamp") for r in records if r.get("eventTimestamp") is not None
 ]

 generation_started_at = datetime.now(timezone.utc)
 header = {
 "reporterIMID": cfg.reporter_imid,
 "fileType": file_type,
 "activityDate": cfg.event_date.isoformat,
 "submissionCycle": cfg.submission_cycle,
 "correctionCycleNumber": cfg.correction_cycle_number,
 "catSpecVersion": cfg.cat_spec_version,
 "recordCount": len(records),
 "generatedAt": generation_started_at.isoformat,
 }
 header = _drop_nulls(header)

 compressed, sha256_hex, uncompressed_size, record_count = build_submission_payload(
 file_type=file_type,
 header=header,
 body_iter=rows_to_json_lines(iter(records), file_type),
)
 generation_completed_at = datetime.now(timezone.utc)

 batch_id = str(uuid.uuid4)
 file_name = make_file_name(cfg.reporter_imid, file_type, cfg.event_date, seq=1)

 try:
 sftp_put(cfg, compressed, file_name)
 submission_timestamp = datetime.now(timezone.utc)
 ack_status = "PENDING" if not cfg.dry_run else "DRY_RUN"
 rejection_reason = None
 except Exception as exc: # noqa: BLE001 - we want to record the reason
 LOG.exception("SFTP upload failed for %s", file_name)
 submission_timestamp = None
 ack_status = "REJECTED"
 rejection_reason = str(exc)

 results.append(
 BatchResult(
 batch_id=batch_id,
 submission_file_type=file_type,
 file_name=file_name,
 record_count=record_count,
 file_size_bytes=uncompressed_size,
 compressed_size_bytes=len(compressed),
 sha256_checksum=sha256_hex,
 submission_timestamp=submission_timestamp,
 ack_status=ack_status,
 min_event_timestamp=min(event_timestamps) if event_timestamps else None,
 max_event_timestamp=max(event_timestamps) if event_timestamps else None,
 rejection_reason=rejection_reason,
 generation_duration_seconds=int(
 (generation_completed_at - generation_started_at).total_seconds
),
 dv2_source_hks=dv2_hks,
)
)

 return results

def persist_batch_results(
 spark: SparkSession,
 cfg: SubmissionConfig,
 results: list[BatchResult],
) -> None:
 """Append one row per BatchResult into gold.cat_submission_batch."""
 if not results:
 LOG.info("No batches to persist.")
 return

 rows = [
 {
 "batch_id": r.batch_id,
 "event_date": cfg.event_date,
 "cat_reporter_imid": cfg.reporter_imid,
 "submission_file_type": r.submission_file_type,
 "submission_cycle": cfg.submission_cycle,
 "correction_cycle_number": cfg.correction_cycle_number,
 "file_name": r.file_name,
 "file_size_bytes": r.file_size_bytes,
 "compressed_size_bytes": r.compressed_size_bytes,
 "compression_algorithm": "BZIP2",
 "record_count": r.record_count,
 "min_event_timestamp": r.min_event_timestamp,
 "max_event_timestamp": r.max_event_timestamp,
 "sha256_checksum": r.sha256_checksum,
 "generation_completed_at": datetime.now(timezone.utc),
 "generation_duration_seconds": r.generation_duration_seconds,
 "submission_timestamp": r.submission_timestamp,
 "submission_method": "SFTP",
 "target_endpoint": cfg.target_hostname,
 "submission_attempt_number": 1,
 "ack_status": r.ack_status,
 "rejection_reason": r.rejection_reason,
 "submission_deadline": datetime.fromisoformat(cfg.deadline_iso)
 if cfg.deadline_iso
 else None,
 "is_late_submission": False,
 "cat_submission_version": cfg.cat_spec_version,
 "submitted_by": "service.account.cat_submission",
 "dv2_source_hk": "|".join(sorted(set(r.dv2_source_hks)))[:1024],
 "load_date": datetime.now(timezone.utc),
 "record_source": "cat_submission_generator",
 }
 for r in results
 ]

 (
 spark.createDataFrame(rows).write.format("delta").mode("append").saveAsTable("gold.cat_submission_batch")
)
 LOG.info("Persisted %d batch rows to gold.cat_submission_batch", len(rows))

# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------
def _parse_args -> argparse.Namespace:
 p = argparse.ArgumentParser(description="CAT submission generator")
 p.add_argument("--reporter-imid", required=True)
 p.add_argument("--event-date", required=True, help="YYYY-MM-DD (T-1 business day)")
 p.add_argument("--submission-cycle", default="INITIAL")
 p.add_argument("--deadline-iso", default="")
 p.add_argument("--hostname", default="sftp.catnmsplan.com")
 p.add_argument("--username", default="")
 p.add_argument("--key-path", default="")
 p.add_argument("--dry-run", action="store_true")
 return p.parse_args

def main -> None:
 args = _parse_args
 cfg = SubmissionConfig(
 reporter_imid=args.reporter_imid,
 event_date=date.fromisoformat(args.event_date),
 submission_cycle=args.submission_cycle,
 target_hostname=args.hostname,
 target_username=args.username,
 target_private_key_path=args.key_path,
 deadline_iso=args.deadline_iso,
 dry_run=args.dry_run,
)
 logging.basicConfig(
 level=logging.INFO,
 format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
 spark = SparkSession.builder.appName("cat_submission_generator").getOrCreate
 LOG.info("Generating CAT submissions for IMID=%s date=%s cycle=%s",
 cfg.reporter_imid, cfg.event_date, cfg.submission_cycle)
 results = generate_and_submit(spark, cfg)
 persist_batch_results(spark, cfg, results)
 LOG.info("Done - generated %d batch file(s)", len(results))

if __name__ == "__main__":
 main
