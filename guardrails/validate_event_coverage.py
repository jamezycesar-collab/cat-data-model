#!/usr/bin/env python3
"""Event-coverage guardrail.

For every CAT event code in primary-sources/cat_im_event_types.csv, confirms
that at least one row in ddl/gold/06_cat_field_mapping.csv references it
via the cat_event_codes column. Codes with zero references are errors,
unless they're documented in guardrails/known_uncovered_events.csv as
deferred coverage with a written rationale.

This converts audit finding F4.1 from a hidden gap into a tracked one.
The 8 currently-uncovered codes (MEOF/MEOFS/MEFA, MONQ/MORQ/MOQR/MOQC/MOQM)
are explicitly allowlisted with target gold table and per-section deferral
notes. Adding a 9th uncovered code (e.g. when a future spec version adds
new events) fails the validator unless it's also added to the allowlist
with rationale.

Coverage is a binary check: a code is "covered" if any field-mapping row
references it, regardless of how many fields are mapped. Coverage *depth*
(how many fields per code) is reported informationally but not enforced
- depth varies legitimately (a simple cancellation event has fewer fields
than a new-order event).

Usage:
  python3 guardrails/validate_event_coverage.py
  python3 guardrails/validate_event_coverage.py --strict
  python3 guardrails/validate_event_coverage.py --report   # show depth distribution
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
VERIFIED_CSV = REPO / "primary-sources" / "cat_im_event_types.csv"
MAPPING_CSV = REPO / "ddl" / "gold" / "06_cat_field_mapping.csv"
ALLOWLIST_CSV = Path(__file__).parent / "known_uncovered_events.csv"


def load_verified_codes() -> dict[str, dict[str, str]]:
    """Return {message_type: row} for every verified CAT event code."""
    if not VERIFIED_CSV.exists():
        raise FileNotFoundError(f"Verified codes CSV missing: {VERIFIED_CSV}")
    out: dict[str, dict[str, str]] = {}
    with VERIFIED_CSV.open() as f:
        for row in csv.DictReader(f):
            out[row["message_type"]] = row
    return out


def load_field_mapping_coverage() -> dict[str, list[tuple[str, str, str]]]:
    """Return {code: [(gold_table, gold_column, cat_json_field), ...]}."""
    if not MAPPING_CSV.exists():
        raise FileNotFoundError(f"Field mapping CSV missing: {MAPPING_CSV}")
    out: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    with MAPPING_CSV.open() as f:
        for row in csv.DictReader(f):
            codes_field = (row.get("cat_event_codes") or "").strip()
            if not codes_field or codes_field == "ALL":
                continue
            for c in codes_field.split(","):
                c = c.strip()
                if c:
                    out[c].append(
                        (row["gold_table"], row["gold_column"], row["cat_json_field"])
                    )
    return out


def load_allowlist() -> set[str]:
    """Codes explicitly allowlisted as documented uncovered (F4.1 backlog)."""
    if not ALLOWLIST_CSV.exists():
        return set()
    out: set[str] = set()
    with ALLOWLIST_CSV.open() as f:
        for row in csv.DictReader(f):
            c = (row.get("message_type") or "").strip()
            if c:
                out.add(c)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true",
                        help="Fail on warnings as well as errors.")
    parser.add_argument("--report", action="store_true",
                        help="Print coverage-depth distribution and exit 0.")
    args = parser.parse_args()

    try:
        verified = load_verified_codes()
        coverage = load_field_mapping_coverage()
        allowlist = load_allowlist()
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    errors: list[str] = []
    warnings: list[str] = []

    uncovered: list[str] = []
    allowlisted_uncovered: list[str] = []
    for code in sorted(verified):
        if code in coverage:
            continue
        if code in allowlist:
            allowlisted_uncovered.append(code)
        else:
            uncovered.append(code)

    for code in uncovered:
        row = verified[code]
        errors.append(
            f"CAT event code {code!r} (section {row['section']}, "
            f"category {row['category']}) has no field-mapping row "
            f"referencing it. Add a row to "
            f"ddl/gold/06_cat_field_mapping.csv or document the deferral "
            f"in guardrails/known_uncovered_events.csv with rationale."
        )

    if allowlisted_uncovered:
        warnings.append(
            f"{len(allowlisted_uncovered)} CAT event codes are allowlisted "
            f"as documented uncovered (F4.1 backlog): "
            f"{', '.join(sorted(allowlisted_uncovered))}"
        )

    print(f"Verified CAT event codes:        {len(verified)}")
    print(f"Codes with >= 1 mapping row:     {len(coverage & verified.keys())}")
    print(f"Codes uncovered (allowlisted):   {len(allowlisted_uncovered)}")
    print(f"Codes uncovered (NEW):           {len(uncovered)}")
    print(f"Errors:   {len(errors)}")
    print(f"Warnings: {len(warnings)}")

    if args.report:
        print()
        depth_counter: dict[int, int] = defaultdict(int)
        for code in verified:
            depth_counter[len(coverage.get(code, []))] += 1
        print("Coverage-depth distribution (n mappings -> n codes):")
        for n in sorted(depth_counter):
            print(f"  {n:3d} mapping rows: {depth_counter[n]:3d} codes")

    print()
    for e in errors:
        print(f"ERROR: {e}\n")
    for w in warnings:
        print(f"WARN:  {w}\n")

    if errors:
        return 1
    if warnings and args.strict:
        return 1
    print("PASS - every CAT event code has at least one field-mapping "
          "row or is allowlisted.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
