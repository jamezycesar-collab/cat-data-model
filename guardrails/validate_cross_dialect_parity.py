#!/usr/bin/env python3
"""Cross-dialect parity guardrail.

Validates that tables defined across multiple SQL dialects (Delta, Hive,
Fabric Warehouse, Fabric Lakehouse) have the same column set in each.
Closes audit finding F8.4.

Scope: only the 4-dialect-parallel families are checked:
  - ddl/option/    (silver+gold across 4 dialects)
  - ddl/multileg/  (silver+gold across 4 dialects)
  - ddl/cais/      (silver+gold across 4 dialects)
  - ddl/equity/    (gold across 4 dialects, since Tier 13)

Out of scope (dialect-specific by design):
  - ddl/gold/         (Delta canonical only)
  - ddl/dv2/          (cross-cutting; managed separately)
  - ddl/expanded-model* (enterprise-scope; Delta+Hive only)
  - ddl/CAT_PreTrade_DDL_*.sql  (legacy)

For each table that appears in 2+ dialects within the in-scope dirs, the
column set of every dialect must match. Each (table, column) where the
column is missing from one dialect but present in another is a parity
diff. Diffs not in the allowlist (`guardrails/known_parity_gaps.csv`)
are errors and exit 1.

Parser handles:
  - Standard column declarations (name TYPE [NOT NULL] ...)
  - Hive PARTITIONED BY (col TYPE) - partition columns are real columns
  - Filters out: REFERENCES inline FK clauses (Delta), CONSTRAINT/
    PRIMARY/FOREIGN/CHECK keyword-led rows
  - Schema prefixes (gold.fact_x -> fact_x)

Usage:
  python3 guardrails/validate_cross_dialect_parity.py
  python3 guardrails/validate_cross_dialect_parity.py --strict
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
KNOWN_GAPS_CSV = Path(__file__).parent / "known_parity_gaps.csv"
PARITY_DIRS = {"option", "multileg", "cais", "equity"}

_RESERVED_LINE_STARTS = {
    "constraint", "primary", "foreign", "check", "references",
    "partitioned", "stored", "tblproperties", "using", "options",
    "cluster", "comment", "location", "with", "row", "format",
    "lifecycle", "as", "select",
}

# CREATE TABLE parsing is delegated to guardrails/_ddl_parser.py which uses
# a balanced-paren walker rather than a regex. This is robust to nested
# parens (DECIMAL(38,18), CHECK(...)), per-column COMMENT 'string' clauses,
# parens inside line comments, and SQL escaped quotes.
from _ddl_parser import find_create_tables  # noqa: E402

_PARTITIONED_BY_RE = re.compile(
    r"PARTITIONED\s+BY\s*\(\s*([^)]+?)\s*\)",
    re.IGNORECASE | re.DOTALL,
)
_COL_NAME_RE = re.compile(r"(\w+)\s+\w")


def dialect_of(path: Path) -> str | None:
    """Map filename to dialect tag, or None if unrecognized."""
    n = path.name.lower()
    if "fabric_warehouse" in n:
        return "fab_wh"
    if "fabric_lakehouse" in n:
        return "fab_lh"
    if "_hive" in n:
        return "hive"
    if "_delta" in n:
        return "delta"
    return None


def extract_columns_from_body(body: str) -> set[str]:
    """Extract column names from a CREATE TABLE body, skipping reserved rows."""
    cols: set[str] = set()
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        first_word = line.split()[0].lower() if line.split() else ""
        if first_word in _RESERVED_LINE_STARTS:
            continue
        m = _COL_NAME_RE.match(line)
        if m:
            cols.add(m.group(1).lower())
    return cols


def parse_file(path: Path) -> dict[str, set[str]]:
    """Return {table_name: column_set} for one DDL file."""
    text = path.read_text()
    out: dict[str, set[str]] = {}
    for tbl_name, body, _body_start, body_end, _kw in find_create_tables(text):
        tbl = tbl_name.lower()
        cols = extract_columns_from_body(body)
        # Look at PARTITIONED BY clause starting after the body close
        tail = text[body_end:body_end + 400]
        pm = _PARTITIONED_BY_RE.search(tail)
        if pm:
            for line in pm.group(1).splitlines():
                line = line.strip().rstrip(",")
                if not line:
                    continue
                cm = re.match(r"(\w+)", line)
                if cm:
                    cols.add(cm.group(1).lower())
        out.setdefault(tbl, set()).update(cols)
    return out


def load_known_gaps() -> set[tuple[str, str]]:
    """Load (table, column) tuples from the parity allowlist CSV."""
    if not KNOWN_GAPS_CSV.exists():
        return set()
    out: set[tuple[str, str]] = set()
    with KNOWN_GAPS_CSV.open() as f:
        for row in csv.DictReader(f):
            t = (row.get("table") or "").strip().lower()
            c = (row.get("column") or "").strip().lower()
            if t and c:
                out.add((t, c))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true",
                        help="Fail on warnings as well as errors.")
    args = parser.parse_args()

    ddl_dir = REPO / "ddl"
    if not ddl_dir.exists():
        print(f"ERROR: ddl/ directory missing", file=sys.stderr)
        return 2

    # Build per-dialect column inventory
    dialect_tables: dict[str, dict[str, set[str]]] = {
        "delta": {}, "hive": {}, "fab_wh": {}, "fab_lh": {},
    }
    files_scanned = 0
    for f in sorted(ddl_dir.rglob("*.sql")):
        if f.parent.name not in PARITY_DIRS:
            continue
        d = dialect_of(f)
        if not d:
            continue
        files_scanned += 1
        for tbl, cols in parse_file(f).items():
            dialect_tables[d].setdefault(tbl, set()).update(cols)

    # Tables in 2+ dialects
    all_multi: set[str] = set()
    for d in dialect_tables:
        for tbl in dialect_tables[d]:
            present = [d2 for d2 in dialect_tables if tbl in dialect_tables[d2]]
            if len(present) >= 2:
                all_multi.add(tbl)

    allowlist = load_known_gaps()
    errors: list[str] = []
    warnings: list[str] = []
    backlog_count = 0
    new_diff_count = 0

    for tbl in sorted(all_multi):
        present = sorted([d for d in dialect_tables if tbl in dialect_tables[d]])
        union: set[str] = set()
        for d in present:
            union |= dialect_tables[d][tbl]
        for col in sorted(union):
            missing = sorted([d for d in present if col not in dialect_tables[d][tbl]])
            if not missing:
                continue
            in_d = sorted([d for d in present if col in dialect_tables[d][tbl]])
            if (tbl, col) in allowlist:
                backlog_count += 1
                continue
            new_diff_count += 1
            errors.append(
                f"{tbl}.{col} differs across dialects: "
                f"present in [{','.join(in_d)}], missing from [{','.join(missing)}]. "
                f"Either fix the parity gap or add (table={tbl}, column={col}) to "
                f"guardrails/known_parity_gaps.csv as documented backlog."
            )

    if backlog_count > 0:
        warnings.append(
            f"{backlog_count} parity diffs are allowlisted as documented "
            f"backlog (per F5.1, F5.2, F5.3)."
        )

    print(f"DDL files scanned (parity scope):  {files_scanned}")
    print(f"Tables in 2+ dialects:             {len(all_multi)}")
    print(f"Allowlisted backlog diffs:         {backlog_count}")
    print(f"New parity violations:             {new_diff_count}")
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
    print("PASS - cross-dialect column sets match (modulo allowlist).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
