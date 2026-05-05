#!/usr/bin/env python3
"""Diagram-lint guardrail.

Walks every Mermaid diagram under diagrams/mermaid/ and confirms that
every entity name matching the data-vault / star-schema naming convention
(hub_*, sat_*, link_*, fact_*, dim_*, pit_*, bridge_*) appears as a
CREATE TABLE in ddl/. Phantom entities (referenced in a diagram but
absent from DDL) become errors and fail the validator, unless allowlisted
in guardrails/known_diagram_gaps.csv.

This closes audit findings F7.1 (medallion_flowchart phantoms), F7.2
(gold_star_schema_er phantoms), and F8.3 (no diagram lint).

Mermaid syntax handled:
  classDiagram with namespace blocks (entities inside namespaces are
    transparent - extraction continues; entities inside `class FOO {
    members }` blocks skip the members)
  erDiagram with `ENTITY { columns }` blocks (members skipped)
  flowchart with `A --> B` relationships
  Mermaid `%%` comments stripped before parsing

Filter: tokens ending in `_sk` whose prefix exists as a real DDL table
are treated as FK column references, not entity references, and are
excluded from the phantom check.

Usage:
  python3 guardrails/validate_diagrams.py
  python3 guardrails/validate_diagrams.py --strict
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
DIAGRAMS_DIR = REPO / "diagrams" / "mermaid"
KNOWN_GAPS_CSV = Path(__file__).parent / "known_diagram_gaps.csv"

ENTITY_RE = re.compile(
    r"\b((?:hub|sat|link|fact|dim|pit|bridge)_[a-z0-9_]+)\b",
    re.IGNORECASE,
)
NAMESPACE_OPEN = re.compile(r"\bnamespace\s+\w+\s*\{")

# Pattern: CREATE TABLE [IF NOT EXISTS] [schema.]name (
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[\w\.]*?(\w+)\s*\(",
    re.IGNORECASE,
)


def discover_ddl_tables() -> set[str]:
    """Return set of all CREATE TABLE names found in ddl/, lowercased."""
    out: set[str] = set()
    ddl_root = REPO / "ddl"
    if not ddl_root.exists():
        return out
    for f in ddl_root.rglob("*.sql"):
        text = f.read_text()
        for m in _CREATE_TABLE_RE.finditer(text):
            out.add(m.group(1).lower())
    return out


def extract_entities(path: Path) -> list[tuple[int, str]]:
    """Walk a Mermaid file and return [(line_num, entity_name), ...].

    Distinguishes namespace blocks (transparent) from member/column
    blocks (skipped). Namespace contents continue to be scanned;
    `ENTITY { ... }` block contents are treated as members and skipped.
    """
    out: list[tuple[int, str]] = []
    skip_depth = 0  # depth of member/column blocks
    transparent_depth = 0  # depth of namespace blocks
    with path.open() as f:
        for line_num, raw in enumerate(f, start=1):
            line = re.sub(r"%%.*$", "", raw)  # strip Mermaid comments
            i = 0
            buf_start = 0
            while i < len(line):
                ch = line[i]
                if ch == "{":
                    pre = line[buf_start:i]
                    if NAMESPACE_OPEN.search(pre + "{"):
                        transparent_depth += 1
                    else:
                        if skip_depth == 0:
                            for m in ENTITY_RE.finditer(pre):
                                out.append((line_num, m.group(1).lower()))
                        skip_depth += 1
                    buf_start = i + 1
                elif ch == "}":
                    if transparent_depth > 0:
                        transparent_depth -= 1
                    elif skip_depth > 0:
                        skip_depth -= 1
                    buf_start = i + 1
                i += 1
            tail = line[buf_start:]
            if skip_depth == 0 and tail:
                for m in ENTITY_RE.finditer(tail):
                    out.append((line_num, m.group(1).lower()))
    return out


def is_fk_column_reference(token: str, real_tables: set[str]) -> bool:
    """dim_party_sk is a FK-column reference when dim_party is a table."""
    if token.endswith("_sk"):
        prefix = token[:-3]
        if prefix in real_tables:
            return True
    return False


def load_known_gaps() -> set[tuple[str, str]]:
    """Load (diagram_file, entity_name) tuples from the allowlist CSV."""
    if not KNOWN_GAPS_CSV.exists():
        return set()
    out: set[tuple[str, str]] = set()
    with KNOWN_GAPS_CSV.open() as f:
        for row in csv.DictReader(f):
            d = (row.get("diagram_file") or "").strip()
            e = (row.get("entity_name") or "").strip().lower()
            if d and e:
                out.add((d, e))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true",
                        help="Fail on warnings as well as errors.")
    args = parser.parse_args()

    if not DIAGRAMS_DIR.exists():
        print(f"ERROR: diagrams directory missing: {DIAGRAMS_DIR}", file=sys.stderr)
        return 2

    real_tables = discover_ddl_tables()
    allowlist = load_known_gaps()

    errors: list[str] = []
    warnings: list[str] = []
    file_count = 0
    entity_count = 0
    backlog_count = 0
    new_violation_count = 0

    for mmd_path in sorted(DIAGRAMS_DIR.glob("*.mmd")):
        file_count += 1
        rel = mmd_path.relative_to(REPO).as_posix()
        seen_in_file: set[str] = set()
        for line_num, entity in extract_entities(mmd_path):
            entity_count += 1
            if entity in real_tables:
                continue
            if is_fk_column_reference(entity, real_tables):
                continue
            if entity in seen_in_file:
                continue  # report once per file
            seen_in_file.add(entity)
            if (rel, entity) in allowlist:
                backlog_count += 1
                continue
            new_violation_count += 1
            errors.append(
                f"{rel}:{line_num} references entity {entity!r} that has "
                f"no CREATE TABLE in any DDL file. Either add the DDL or "
                f"add (diagram_file={rel}, entity_name={entity}) to "
                f"guardrails/known_diagram_gaps.csv as documented backlog."
            )

    if backlog_count > 0:
        warnings.append(
            f"{backlog_count} diagram entities are allowlisted as "
            f"documented backlog (per F7.1, F7.2, F7.4)."
        )

    print(f"Diagram files scanned:       {file_count}")
    print(f"Entity tokens scanned:       {entity_count}")
    print(f"DDL tables available:        {len(real_tables)}")
    print(f"Allowlisted backlog hits:    {backlog_count}")
    print(f"New phantom violations:      {new_violation_count}")
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
    print("PASS - all diagram entities resolve in DDL or are allowlisted.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
