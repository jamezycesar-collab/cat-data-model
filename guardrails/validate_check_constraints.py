#!/usr/bin/env python3
"""CHECK-constraint enum lint.

Walks every DDL file and extracts enforced SQL CHECK constraints of
the form:

    CHECK (col IN ('a', 'b', 'c'))
    CHECK (col IS NULL OR col IN ('a', 'b', 'c'))

For each (column, value) pair, validates the value against the
appropriate primary-source enumeration:

  - cat_event_code, event_code, quote_event_code
        -> primary-sources/cat_im_event_types.csv  (message_type col)
  - customer_type, fdid_type, cais_fdid_type_bk, etc.
        -> primary-sources/cais_enumerations.csv   (filtered by enum_field)

Any value not in the primary source is an error. This catches the
class of fabrication that audit finding F8.5 called out: a typo in a
CHECK constraint (e.g. 'MONOX' instead of 'MONO') would slip past every
existing validator unless the typo happens to match one of the 24
fabricated codes the no-fabrications validator knows about.

Hive COMMENT 'CHECK in ...' markers are treated as documentation and
not validated. The SQL CHECK constraint on the same column in the
Delta / Fabric WH / Fabric LH dialects is the canonical source of
truth.

Columns with CHECK constraints that aren't mapped to a primary source
(e.g. internal severity codes, leg_side BUY/SELL, risk_framework
CRR/FRTB) are reported informationally so coverage can be extended.

Usage:
  python3 guardrails/validate_check_constraints.py
  python3 guardrails/validate_check_constraints.py --strict
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]

# Map DDL column name -> (csv_path, filter_column, filter_value, value_column)
# filter_column/filter_value are used to narrow rows in a multi-enum CSV;
# pass (None, None) to use all rows.
COLUMN_TO_PRIMARY_SOURCE: dict[str, tuple[Path, str | None, str | None, str]] = {
    "cat_event_code":     (REPO / "primary-sources" / "cat_im_event_types.csv", None, None, "message_type"),
    "event_code":         (REPO / "primary-sources" / "cat_im_event_types.csv", None, None, "message_type"),
    "quote_event_code":   (REPO / "primary-sources" / "cat_im_event_types.csv", None, None, "message_type"),
    "event_type_code":    (REPO / "primary-sources" / "cat_im_event_types.csv", None, None, "message_type"),
    "customer_type":      (REPO / "primary-sources" / "cais_enumerations.csv", "enum_field", "customerType", "enum_value"),
    "fdid_type":          (REPO / "primary-sources" / "cais_enumerations.csv", "enum_field", "fdidType", "enum_value"),
    "cais_fdid_type_bk":  (REPO / "primary-sources" / "cais_enumerations.csv", "enum_field", "fdidType", "enum_value"),
    "customer_role":      (REPO / "primary-sources" / "cais_enumerations.csv", "enum_field", "customerRole", "enum_value"),
    "large_trader_type":  (REPO / "primary-sources" / "cais_enumerations.csv", "enum_field", "largeTraderType", "enum_value"),
    "submission_action":  (REPO / "primary-sources" / "cais_enumerations.csv", "enum_field", "submissionAction", "enum_value"),
    "file_type":          (REPO / "primary-sources" / "cais_enumerations.csv", "enum_field", "fileType", "enum_value"),
    "fdid_end_reason":    (REPO / "primary-sources" / "cais_enumerations.csv", "enum_field", "fdidEndReason", "enum_value"),
    "cais_account_type_bk": (REPO / "primary-sources" / "cais_enumerations.csv", "enum_field", "accountType", "enum_value"),
    "addr_type":          (REPO / "primary-sources" / "cais_enumerations.csv", "enum_field", "addrType", "enum_value"),
}

# Columns with CHECK constraints we don't validate against a primary source
# (internal/operational enums). Recorded for visibility.
KNOWN_UNMAPPED_COLUMNS = {
    "severity",                # CAIS operational: MATERIAL, NON_MATERIAL, DATA_VALIDATION, etc.
    "affected_record_type",    # CAIS operational: FDID, CUSTOMER, FILE
    "leg_side",                # Multileg internal: BUY, SELL
    "risk_framework",          # Expanded-model agreement: CRR, FRTB, SA_CCR
    "action_type",             # CAT IM v4.1.0r15 section 4.1 row 1: NEW, FRC, RPR (fixed 3-value enum, no separate primary-source CSV)
}

# CHECK (col IN ('a','b'))  -- multi-line tolerant
_CHECK_IN_RE = re.compile(
    r"CHECK\s*\(\s*(\w+)\s+IN\s*\(\s*([^)]+?)\s*\)\s*\)",
    re.IGNORECASE | re.DOTALL,
)
# CHECK (col IS NULL OR col IN ('a','b'))
_CHECK_NULL_IN_RE = re.compile(
    r"CHECK\s*\(\s*(\w+)\s+IS\s+NULL\s+OR\s+\1\s+IN\s*\(\s*([^)]+?)\s*\)\s*\)",
    re.IGNORECASE | re.DOTALL,
)


def load_primary_source(
    csv_path: Path,
    filter_col: str | None,
    filter_val: str | None,
    value_col: str,
) -> set[str]:
    """Load values from a primary-source CSV, optionally filtered by column."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Primary source missing: {csv_path}")
    out: set[str] = set()
    with csv_path.open() as f:
        for row in csv.DictReader(f):
            if filter_col and row.get(filter_col, "").strip() != filter_val:
                continue
            v = (row.get(value_col) or "").strip()
            if v:
                out.add(v)
    return out


def extract_check_constraints(
    sql_text: str,
) -> list[tuple[str, list[str]]]:
    """Return [(column_name_lower, [values_in_check_list]), ...].

    Handles both `CHECK (col IN (...))` and `CHECK (col IS NULL OR col IN (...))`.
    """
    out = []
    for regex in (_CHECK_IN_RE, _CHECK_NULL_IN_RE):
        for m in regex.finditer(sql_text):
            col = m.group(1).lower()
            body = m.group(2)
            values = sorted(set(re.findall(r"'([^']+)'", body)))
            if values:
                out.append((col, values))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true",
                        help="Fail on warnings and informational notes.")
    args = parser.parse_args()

    ddl_dir = REPO / "ddl"
    if not ddl_dir.exists():
        print(f"ERROR: ddl/ directory missing", file=sys.stderr)
        return 2

    # Pre-load every primary source we need
    enum_cache: dict[tuple, set[str]] = {}
    for col, (path, fcol, fval, vcol) in COLUMN_TO_PRIMARY_SOURCE.items():
        key = (str(path), fcol, fval, vcol)
        if key not in enum_cache:
            try:
                enum_cache[key] = load_primary_source(path, fcol, fval, vcol)
            except FileNotFoundError as e:
                print(f"ERROR: {e}", file=sys.stderr)
                return 1

    errors: list[str] = []
    warnings: list[str] = []
    info: list[str] = []

    constraint_count = 0
    validated_count = 0
    unmapped_count = 0
    columns_with_constraints: set[str] = set()

    for sql_file in sorted(ddl_dir.rglob("*.sql")):
        text = sql_file.read_text()
        rel = sql_file.relative_to(REPO).as_posix()
        for col, values in extract_check_constraints(text):
            constraint_count += 1
            columns_with_constraints.add(col)
            if col in COLUMN_TO_PRIMARY_SOURCE:
                validated_count += 1
                src = COLUMN_TO_PRIMARY_SOURCE[col]
                key = (str(src[0]), src[1], src[2], src[3])
                allowed = enum_cache[key]
                bad = [v for v in values if v not in allowed]
                if bad:
                    errors.append(
                        f"{rel}: CHECK ({col} IN ...) contains values "
                        f"not in primary source: {sorted(bad)}. "
                        f"Source: {src[0].relative_to(REPO).as_posix()}"
                        + (f" (filtered by {src[1]}={src[2]!r})" if src[1] else "")
                    )
            elif col in KNOWN_UNMAPPED_COLUMNS:
                unmapped_count += 1  # silent: known-unmapped operational enum
            else:
                # Brand-new column with a CHECK we don't know about
                warnings.append(
                    f"{rel}: CHECK ({col} IN ...) on a column not in "
                    f"COLUMN_TO_PRIMARY_SOURCE or KNOWN_UNMAPPED_COLUMNS. "
                    f"Either map it to a primary source or add it to the "
                    f"unmapped set in this validator."
                )

    print(f"DDL files scanned:                {sum(1 for _ in ddl_dir.rglob('*.sql'))}")
    print(f"SQL CHECK constraints found:      {constraint_count}")
    print(f"Constraints validated vs source:  {validated_count}")
    print(f"Constraints on known-unmapped:    {unmapped_count}")
    print(f"Distinct constrained columns:     {len(columns_with_constraints)}")
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
    print("PASS - all CHECK constraint values resolve to primary sources.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
