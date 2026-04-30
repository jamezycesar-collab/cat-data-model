#!/usr/bin/env python3
"""Field-level guardrail.

The event-code validator (validate_event_taxonomy.py) catches drift in
the LIST of CAT event codes. This validator catches drift one layer
deeper: in the FIELDS of each event.

It checks ddl/gold/06_cat_field_mapping.csv against:

  1. The verified set of CAT event codes in primary-sources/cat_im_event_types.csv.
     Any code referenced in the cat_event_codes column that isn't in the
     verified set = fabrication or typo.

  2. A heuristic check that each cat_json_field name appears somewhere in
     the spec PDF text. The spec uses camelCase field names like
     'firmDesignatedID' or 'eventTimestamp'. Fields not found in the PDF
     are flagged as warnings (could be a misspelling or invented name).

This is a heuristic, not a complete spec-cross-reference. A future
version could parse the per-event Field Specification tables in the
spec PDF (Tables 64+) and compare the precise required-vs-conditional
column. For now this catches the most common fabrications: typos in
event codes and made-up JSON field names.

Usage:
  python3 guardrails/validate_field_specifications.py
  python3 guardrails/validate_field_specifications.py --strict
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

try:
    from pypdf import PdfReader
except ImportError:
    print("ERROR: pypdf is required. Install with: pip install pypdf", file=sys.stderr)
    sys.exit(2)


REPO = Path(__file__).resolve().parents[1]
PINS_FILE = Path(__file__).parent / "spec_pins.json"


def load_verified_event_codes() -> set[str]:
    csv_path = REPO / "primary-sources" / "cat_im_event_types.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Verified event types CSV missing: {csv_path}")
    with csv_path.open() as f:
        return {r["message_type"] for r in csv.DictReader(f)}


VERIFIED_CSV = REPO / "ddl" / "gold" / "06_cat_field_mapping.csv"
UNVERIFIED_CSV = REPO / "ddl" / "gold" / "06b_cat_field_mapping_unverified_candidates.csv"


def load_field_mapping() -> list[dict[str, str]]:
    if not VERIFIED_CSV.exists():
        raise FileNotFoundError(f"Field mapping CSV missing: {VERIFIED_CSV}")
    with VERIFIED_CSV.open() as f:
        return list(csv.DictReader(f))


def load_unverified_candidates() -> list[dict[str, str]]:
    if not UNVERIFIED_CSV.exists():
        return []
    with UNVERIFIED_CSV.open() as f:
        return list(csv.DictReader(f))


def extract_pdf_text(spec_filename: str) -> str:
    pdf_path = REPO / "primary-sources" / spec_filename
    if not pdf_path.exists():
        raise FileNotFoundError(f"Spec PDF missing: {pdf_path}")
    reader = PdfReader(str(pdf_path))
    return "".join((p.extract_text() or "") for p in reader.pages)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true",
                        help="Fail on warnings as well as errors.")
    args = parser.parse_args()

    if not PINS_FILE.exists():
        print(f"ERROR: pin file missing: {PINS_FILE}", file=sys.stderr)
        return 2
    pins = json.loads(PINS_FILE.read_text())

    errors: list[str] = []
    warnings: list[str] = []

    # ----- Load inputs -----
    try:
        verified_codes = load_verified_event_codes()
    except FileNotFoundError as e:
        errors.append(str(e))
        verified_codes = set()
    try:
        mappings = load_field_mapping()
    except FileNotFoundError as e:
        errors.append(str(e))
        mappings = []

    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        return 1

    # Build PDF text once - both CAT IM and CAIS specs, since field mappings
    # may reference fields from either.
    cat_pin = pins["cat_im_spec"]
    cais_pin = pins["cais_spec"]
    try:
        cat_pdf_text = extract_pdf_text(cat_pin["filename"])
    except FileNotFoundError as e:
        errors.append(str(e))
        cat_pdf_text = ""
    try:
        cais_pdf_text = extract_pdf_text(cais_pin["filename"])
    except FileNotFoundError as e:
        errors.append(str(e))
        cais_pdf_text = ""
    combined_text = cat_pdf_text + "\n" + cais_pdf_text

    # ----- Check 1: cat_event_codes references real codes -----
    rows_with_bad_codes: list[tuple[int, str, list[str]]] = []
    for i, row in enumerate(mappings, start=2):  # +2 for 1-indexed + header
        codes_field = (row.get("cat_event_codes") or "").strip()
        if codes_field in ("", "ALL"):
            continue
        codes = [c.strip() for c in codes_field.split(",") if c.strip()]
        bad = [c for c in codes if c not in verified_codes]
        if bad:
            rows_with_bad_codes.append((i, row.get("gold_column", ""), bad))

    if rows_with_bad_codes:
        for line, col, bad in rows_with_bad_codes:
            errors.append(
                f"06_cat_field_mapping.csv:{line} (gold_column={col!r}) "
                f"references non-existent event codes: {bad}"
            )

    # ----- Check 2: each cat_json_field appears in the spec text -----
    # Common field-name pattern in CAT spec is camelCase, sometimes with digits.
    # Generic ASCII names like "OrderEvents", "Allocations" are file types,
    # not field names.
    skip_fields = {
        "ALL", "OrderEvents", "OptionEvents", "Allocations", "QuoteEvents",
        "CAIS", "recordType",  # recordType is per-CAIS-row, not per-event
    }
    missing_fields: list[tuple[int, str, str]] = []
    for i, row in enumerate(mappings, start=2):
        field = (row.get("cat_json_field") or "").strip()
        if not field or field in skip_fields:
            continue
        if not re.match(r"^[a-zA-Z][a-zA-Z0-9]*$", field):
            # Compound field names with delimiters - skip; not directly grep-able
            continue
        # Look for the field name as a standalone token in either spec PDF
        if not re.search(rf"\b{re.escape(field)}\b", combined_text):
            missing_fields.append((i, row.get("gold_column", ""), field))

    if missing_fields:
        for line, col, field in missing_fields:
            warnings.append(
                f"06_cat_field_mapping.csv:{line} (gold_column={col!r}) "
                f"references CAT JSON field {field!r} not found in spec text. "
                f"Could be a fabrication or a CAIS-only field. Verify manually."
            )

    # ----- Check 3: gold_table values match a known set -----
    known_gold_tables = {
        "fact_cat_order_events",
        "fact_cat_executions",
        "fact_cat_allocations",
        "fact_cat_quotes",
        "fact_cat_customer_records",  # legacy / deprecated
        "fact_order_events",
        "fact_execution_events",
        "fact_allocations",
        "fact_multileg_option_events",
        "fact_multileg_option_legs",
        "fact_cais_fdid",
        "fact_cais_customer",
        "fact_cais_submission",
        "fact_cais_inconsistency",
        # Conformed dimensions, referenced via SK joins from facts
        "dim_party",
        "dim_instrument",
        "dim_venue",
        "dim_account",
        "dim_trader",
        "dim_desk",
        "dim_date",
        "dim_event_type",
        # Quote dimension (work in progress)
        "fact_quotes",
    }
    unknown_tables: list[tuple[int, str]] = []
    for i, row in enumerate(mappings, start=2):
        t = (row.get("gold_table") or "").strip()
        if t and t not in known_gold_tables:
            unknown_tables.append((i, t))
    if unknown_tables:
        for line, t in unknown_tables:
            warnings.append(
                f"06_cat_field_mapping.csv:{line} references unknown gold_table {t!r}. "
                f"If this table exists in the DDL, add it to known_gold_tables in this validator."
            )

    # ----- Also count unverified candidates (informational only) -----
    candidates = load_unverified_candidates()

    # ----- Report -----
    print(f"Verified field mapping rows:   {len(mappings)}")
    print(f"Unverified candidate rows:     {len(candidates)}")
    print(f"Verified CAT event codes:      {len(verified_codes)}")
    print(f"Errors:   {len(errors)}")
    print(f"Warnings: {len(warnings)}")
    print()
    for e in errors:
        print(f"ERROR: {e}\n")
    for w in warnings:
        print(f"WARN:  {w}\n")

    if errors:
        return 1
    if warnings and args.strict:
        return 1
    print("PASS - field mappings reference verified codes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
