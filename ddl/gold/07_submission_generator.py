#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 cat-data-model contributors
"""FINRA CAT submission generator.

Reads from the Gold fact tables, maps to CAT JSON fields per the
ddl/gold/06_cat_field_mapping.csv mapping, BZip2-compresses, and
SFTPs to the CAT Plan Processor on T+1 by 06:00 ET.

Source of truth: FINRA CAT IM Tech Specs v4.1.0r15 (Sections 6 and 7
cover submission format and feedback). The CSV mapping is the bridge
between our Gold columns and the spec field names; do not hand-edit
columns not present in the CSV.

Usage:
  python3 07_submission_generator.py \\
      --reporter-imid <IMID> \\
      --file-type <OrderEvents|OptionEvents|Allocations|CAIS> \\
      --event-date YYYY-MM-DD \\
      --output-dir /path/to/out

This module exposes top-level functions intended to run in a Databricks
Job context where `spark` is provided. For local CLI use, see the
`__main__` block.
"""
from __future__ import annotations

import argparse
import bz2
import csv
import hashlib
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator


# ---- Constants per CAT IM Tech Specs v4.1.0r15 -----------------------------

CAT_SPEC_VERSION = "4.1.0r15"

# Section 6.1.1 file naming format:
#   <Submitter ID>_<Reporter CRD>_<Generation Date>_<Group>_<File Number>.json.bz2
FILE_NAME_TEMPLATE = "{submitter_id}_{reporter_crd}_{gen_date}_{group}_{file_no:04d}.json"

# Section 6.4.1: T+1 06:00 ET hard deadline for OrderEvents / OptionEvents.
# Allocations have a different deadline (T+1 17:30 ET); CAIS is daily.
SUBMISSION_DEADLINES_BY_FILE_TYPE = {
    "OrderEvents":  "06:00",
    "OptionEvents": "06:00",
    "Allocations": "17:30",
    "CAIS": "23:59",
}

VALID_FILE_TYPES = set(SUBMISSION_DEADLINES_BY_FILE_TYPE.keys())

logger = logging.getLogger(__name__)


# ---- Exceptions ------------------------------------------------------------


class SubmissionError(Exception):
    """Raised when a submission cannot be produced safely."""


class FieldMappingError(SubmissionError):
    """Raised when the field-mapping CSV is missing or malformed."""


# ---- Field mapping --------------------------------------------------------


@dataclass(frozen=True)
class FieldMapping:
    gold_table: str
    gold_column: str
    cat_json_field: str
    cat_submission_file_type: str
    cat_event_codes: tuple[str, ...]
    data_type: str
    required: bool
    max_length: int | None
    description: str


def load_field_mappings(csv_path: Path) -> list[FieldMapping]:
    """Read the Gold-to-CAT field mapping CSV."""
    if not csv_path.exists():
        raise FieldMappingError(f"Field mapping CSV not found: {csv_path}")
    out: list[FieldMapping] = []
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for r in reader:
            codes = r.get("cat_event_codes", "")
            event_codes = (
                tuple()
                if codes in ("ALL", "")
                else tuple(c.strip() for c in codes.split(","))
            )
            try:
                max_length = int(r["max_length"]) if r.get("max_length") else None
            except ValueError:
                max_length = None
            out.append(FieldMapping(
                gold_table=r["gold_table"],
                gold_column=r["gold_column"],
                cat_json_field=r["cat_json_field"],
                cat_submission_file_type=r["cat_submission_file_type"],
                cat_event_codes=event_codes,
                data_type=r["data_type"],
                required=(r.get("required", "").upper() == "Y"),
                max_length=max_length,
                description=r.get("description", ""),
            ))
    if not out:
        raise FieldMappingError(f"No mappings loaded from {csv_path}")
    return out


def mappings_for(file_type: str, event_code: str,
                 mappings: list[FieldMapping]) -> list[FieldMapping]:
    """Filter the mappings to those that apply to a specific event."""
    return [
        m for m in mappings
        if m.cat_submission_file_type == file_type
        and (not m.cat_event_codes or event_code in m.cat_event_codes)
    ]


# ---- JSON serialization ---------------------------------------------------


def _json_default(obj: object) -> Any:
    """Render dates / datetimes / decimals in CAT-compliant ISO 8601 form."""
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.isoformat(timespec="microseconds")
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    raise TypeError(f"Cannot serialize {type(obj).__name__}: {obj!r}")


def _drop_nulls(row_dict: dict) -> dict:
    """Remove keys whose value is None.

    Per CAT spec Section 6.1.4, non-required fields must be omitted (not
    rendered as null) when the firm has no value for them.
    """
    return {k: v for k, v in row_dict.items() if v is not None}


def shape_row(gold_row: dict, mappings: list[FieldMapping]) -> dict:
    """Project Gold-table columns into the CAT JSON field layout."""
    out: dict = {}
    for m in mappings:
        if m.gold_column not in gold_row:
            continue
        v = gold_row[m.gold_column]
        if v is None and not m.required:
            continue
        if m.required and v is None:
            raise SubmissionError(
                f"Required CAT field {m.cat_json_field} is null in Gold row"
            )
        out[m.cat_json_field] = v
    return _drop_nulls(out)


def rows_to_json_lines(rows: Iterable[dict],
                       file_type: str,
                       mappings: list[FieldMapping]) -> Iterator[str]:
    """Yield one JSON object per line (newline-delimited JSON, NDJSON).

    The CAT submission format is NDJSON (one JSON object per line) inside
    a BZip2 wrapper. Section 6.1.2.2 of the spec.
    """
    for row in rows:
        event_code = row.get("cat_event_code") or row.get("event_code")
        if not event_code:
            raise SubmissionError("Row missing cat_event_code")
        ms = mappings_for(file_type, event_code, mappings)
        shaped = shape_row(row, ms)
        yield json.dumps(shaped, default=_json_default, separators=(",", ":"))


# ---- Compression and checksum ---------------------------------------------


def write_compressed(lines: Iterable[str], output_path: Path) -> tuple[int, str]:
    """BZip2-compress the NDJSON stream to disk.

    Returns (uncompressed_byte_count, sha256_hex_of_uncompressed).
    """
    h = hashlib.sha256()
    uncompressed = 0
    with bz2.open(output_path, "wt", encoding="utf-8") as f:
        for line in lines:
            data = (line + "\n")
            h.update(data.encode("utf-8"))
            uncompressed += len(data.encode("utf-8"))
            f.write(data)
    return uncompressed, h.hexdigest()


# ---- File naming ----------------------------------------------------------


@dataclass
class SubmissionMetadata:
    submitter_id: str
    reporter_crd: str
    generation_date: date
    file_type: str
    group: str = ""
    file_number: int = 1
    spec_version: str = CAT_SPEC_VERSION
    record_count: int = 0
    uncompressed_size: int = 0
    sha256: str = ""
    deadline_et: str = field(init=False)

    def __post_init__(self):
        if self.file_type not in VALID_FILE_TYPES:
            raise SubmissionError(
                f"Unknown file_type {self.file_type!r}; "
                f"expected one of {sorted(VALID_FILE_TYPES)}"
            )
        self.deadline_et = SUBMISSION_DEADLINES_BY_FILE_TYPE[self.file_type]

    def filename(self) -> str:
        return FILE_NAME_TEMPLATE.format(
            submitter_id=self.submitter_id,
            reporter_crd=self.reporter_crd,
            gen_date=self.generation_date.strftime("%Y%m%d"),
            group=self.group or "0",
            file_no=self.file_number,
        ) + ".bz2"


# ---- Top-level entry point -------------------------------------------------


def generate_submission(rows: Iterable[dict],
                        meta: SubmissionMetadata,
                        mappings: list[FieldMapping],
                        output_dir: Path) -> SubmissionMetadata:
    """Produce a single CAT submission file from an iterable of Gold rows.

    The caller is responsible for partitioning rows into appropriate
    files (one file per submitter+reporter+date+group+file-number tuple
    per file-type). For multi-million-event production volumes, prefer
    splitting by venue or symbol prefix.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / meta.filename()

    counted_rows: list[dict] = list(rows)
    meta.record_count = len(counted_rows)

    if meta.record_count == 0:
        raise SubmissionError("Cannot produce empty submission")

    lines = rows_to_json_lines(counted_rows, meta.file_type, mappings)
    uncompressed, sha = write_compressed(lines, out_path)
    meta.uncompressed_size = uncompressed
    meta.sha256 = sha

    logger.info(
        "Wrote %s (%d records, %d bytes uncompressed, sha256=%s)",
        out_path, meta.record_count, uncompressed, sha,
    )
    return meta


# ---- Spark integration -----------------------------------------------------


def collect_rows_from_gold(spark: Any, file_type: str, event_date: date) -> list[dict]:
    """Pull rows from the relevant Gold fact for one event-date partition.

    This is a thin wrapper around spark.sql; in production you'd push
    the column projection and filter into the SQL rather than collecting
    everything client-side. The structure is included to make the unit
    of work visible.
    """
    table = {
        "OrderEvents":  "gold.fact_order_events",
        "OptionEvents": "gold.fact_multileg_option_events",
        "Allocations":  "gold.fact_allocations",
        "CAIS":         "gold.fact_cais_submission",
    }.get(file_type)
    if table is None:
        raise SubmissionError(f"No Gold table registered for {file_type}")
    df = spark.table(table).filter(
        f"event_date = '{event_date.strftime('%Y-%m-%d')}'"
    )
    return [r.asDict(recursive=True) for r in df.collect()]


# ---- CLI -------------------------------------------------------------------


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--submitter-id", required=True)
    p.add_argument("--reporter-crd", required=True)
    p.add_argument("--file-type", required=True, choices=sorted(VALID_FILE_TYPES))
    p.add_argument("--event-date", required=True,
                   help="YYYY-MM-DD; the Gold partition to submit")
    p.add_argument("--group", default="0")
    p.add_argument("--file-number", type=int, default=1)
    p.add_argument("--output-dir", required=True, type=Path)
    p.add_argument("--field-mapping",
                   default="ddl/gold/06_cat_field_mapping.csv",
                   type=Path)
    p.add_argument("--dry-run", action="store_true",
                   help="Validate inputs and exit without writing")
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    args = _build_argparser().parse_args(argv)

    try:
        evt_date = datetime.strptime(args.event_date, "%Y-%m-%d").date()
    except ValueError as e:
        logger.error("Invalid --event-date: %s", e)
        return 2

    mappings = load_field_mappings(args.field_mapping)
    logger.info("Loaded %d field mappings from %s", len(mappings), args.field_mapping)

    if args.dry_run:
        logger.info("Dry-run OK. file_type=%s event_date=%s mappings=%d",
                    args.file_type, evt_date, len(mappings))
        return 0

    # In production this would obtain a Spark session; for unit tests the
    # caller injects rows directly via the module API rather than the CLI.
    try:
        from pyspark.sql import SparkSession  # type: ignore
        spark = SparkSession.builder.appName(
            f"cat-submission-{args.file_type}-{evt_date}"
        ).getOrCreate()
    except ImportError:
        logger.error(
            "PySpark not available. Use this CLI inside a Databricks job, "
            "or call generate_submission() directly with pre-fetched rows."
        )
        return 3

    rows = collect_rows_from_gold(spark, args.file_type, evt_date)
    meta = SubmissionMetadata(
        submitter_id=args.submitter_id,
        reporter_crd=args.reporter_crd,
        generation_date=evt_date,
        file_type=args.file_type,
        group=args.group,
        file_number=args.file_number,
    )
    generate_submission(rows, meta, mappings, args.output_dir)

    print(json.dumps({
        "filename": meta.filename(),
        "record_count": meta.record_count,
        "uncompressed_size": meta.uncompressed_size,
        "sha256": meta.sha256,
        "deadline_et": meta.deadline_et,
        "spec_version": meta.spec_version,
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
