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

  3. DDL existence: every gold_table in the field mapping must have a
     CREATE TABLE somewhere under ddl/, and every gold_column must exist
     on its table in at least one dialect. This catches the F3.1 (phantom
     table) and F3.2 (missing column) classes that the previous hardcoded
     known_gold_tables list silently allowed. Existing backlog from
     AUDIT_2026_05_04.md is allowlisted in
     guardrails/known_field_mapping_gaps.csv; new violations fail.

This is a heuristic for check #2, not a complete spec-cross-reference.
A future version could parse the per-event Field Specification tables
in the spec PDF (Tables 64+) and compare the precise required-vs-
conditional column. Checks #1 and #3 are exact.

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
KNOWN_GAPS_CSV = Path(__file__).parent / "known_field_mapping_gaps.csv"

# Pattern: CREATE TABLE [IF NOT EXISTS] [schema.]name ( ... ) <terminator>
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[\w\.]*?(\w+)\s*\(([^;]+?)\)"
    r"\s*(?:USING|PARTITIONED|TBLPROPERTIES|CLUSTER|COMMENT|STORED|;)",
    re.IGNORECASE | re.DOTALL,
)
_COL_NAME_RE = re.compile(r"(\w+)\s+\w")


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


def discover_ddl_tables() -> dict[str, set[str]]:
    """Scan ddl/**/*.sql for CREATE TABLE statements across all dialects.

    Returns a dict mapping table_name (lowercased, schema-stripped) to the
    set of column names defined on it (also lowercased). Columns from any
    dialect (Delta, Hive, Fabric Warehouse, Fabric Lakehouse) are merged,
    so a column present in any dialect satisfies the check.
    """
    tables: dict[str, set[str]] = {}
    ddl_root = REPO / "ddl"
    if not ddl_root.exists():
        return tables
    for f in ddl_root.rglob("*.sql"):
        text = f.read_text()
        for m in _CREATE_TABLE_RE.finditer(text):
            tbl = m.group(1).lower()
            body = m.group(2)
            cols = tables.setdefault(tbl, set())
            for line in body.splitlines():
                line = line.strip().rstrip(",")
                if not line:
                    continue
                if line.startswith("--"):
                    continue
                if line.upper().startswith(("CONSTRAINT", "PRIMARY", "FOREIGN")):
                    continue
                cm = _COL_NAME_RE.match(line)
                if cm:
                    cols.add(cm.group(1).lower())
    return tables


def load_known_gaps() -> set[tuple[str, str]]:
    """Load (gold_table, gold_column) tuples that are documented backlog.

    These are the 212 violations identified by AUDIT_2026_05_04.md (F3.1
    and F3.2). Allowlisted entries become warnings; everything else is
    an error.
    """
    if not KNOWN_GAPS_CSV.exists():
        return set()
    out: set[tuple[str, str]] = set()
    with KNOWN_GAPS_CSV.open() as f:
        for row in csv.DictReader(f):
            t = (row.get("gold_table") or "").strip().lower()
            c = (row.get("gold_column") or "").strip().lower()
            if t and c:
                out.add((t, c))
    return out


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

    # ----- Check 3: gold_table and gold_column values resolve in DDL -----
    #
    # Replaces the previous hardcoded known_gold_tables set with DDL
    # introspection. Closes audit findings F3.1 (phantom tables) and F3.2
    # (missing columns) by making them detectable.
    #
    # The 212 existing violations from AUDIT_2026_05_04.md are listed in
    # guardrails/known_field_mapping_gaps.csv as documented backlog. New
    # violations (table/column not in DDL AND not in allowlist) become
    # errors and fail the validator.
    ddl_tables = discover_ddl_tables()
    allowlist = load_known_gaps()

    phantom_table_violations: list[tuple[int, str, str]] = []
    missing_column_violations: list[tuple[int, str, str]] = []
    backlog_table: list[tuple[int, str, str]] = []
    backlog_column: list[tuple[int, str, str]] = []

    for i, row in enumerate(mappings, start=2):
        t = (row.get("gold_table") or "").strip().lower()
        c = (row.get("gold_column") or "").strip().lower()
        if not t or not c:
            continue
        if t not in ddl_tables:
            if (t, c) in allowlist:
                backlog_table.append((i, t, c))
            else:
                phantom_table_violations.append((i, t, c))
        elif c not in ddl_tables[t]:
            if (t, c) in allowlist:
                backlog_column.append((i, t, c))
            else:
                missing_column_violations.append((i, t, c))

    for line, t, c in phantom_table_violations:
        errors.append(
            f"06_cat_field_mapping.csv:{line} references gold_table {t!r} "
            f"(column {c!r}) that has no CREATE TABLE in any DDL file. "
            f"Either add the DDL or add this row to "
            f"guardrails/known_field_mapping_gaps.csv as documented backlog."
        )
    for line, t, c in missing_column_violations:
        errors.append(
            f"06_cat_field_mapping.csv:{line} references column {t}.{c!r} "
            f"not found on the table in any dialect. Either add the column "
            f"to the DDL or add this row to "
            f"guardrails/known_field_mapping_gaps.csv as documented backlog."
        )
    if backlog_table:
        warnings.append(
            f"{len(backlog_table)} field-mapping rows reference phantom "
            f"gold_tables (allowlisted as backlog per F3.1)."
        )
    if backlog_column:
        warnings.append(
            f"{len(backlog_column)} field-mapping rows reference missing "
            f"gold_columns (allowlisted as backlog per F3.2)."
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
