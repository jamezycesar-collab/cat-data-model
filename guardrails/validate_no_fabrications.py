#!/usr/bin/env python3
"""Guardrail that fails CI if any of the 24 fabricated CAT event codes
appear in code, DDL, or documentation files outside the audit context.

The codes were invented by an earlier AI workflow that filled a "50 events"
target without checking against the spec. They appear in no FINRA
specification and were stripped from the model in the v4.1.0r15 remediation.

Allowed contexts (codes referenced by name for auditing/historical reasons):
  - CHANGELOG.md
  - docs/ROOT_CAUSE_AUDIT.md
  - dlt-pipelines/fact_pipelines/dlt_fact_cat_customer_records.py (deprecation marker)
  - guardrails/validate_no_fabrications.py (this file)

Anywhere else, exit non-zero.

Usage:
  python3 guardrails/validate_no_fabrications.py
  python3 guardrails/validate_no_fabrications.py --add-allowed <path>
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]

# The 24 fabricated codes from the v2.0.0 push.
# DO NOT add new codes to this list without updating ROOT_CAUSE_AUDIT.md.
FABRICATED_CODES = [
    "CAIS_A", "CAIS_C", "CAIS_R", "EOT",
    "MEAU", "MEAX", "MEIN", "MEMA_CORR", "MEMA",
    "MEOR_EXT", "MEOTQ", "MEOX", "MEPM", "MEPZ",
    "MERO", "MEVE", "MLOT", "MOCX", "MOOTS", "MOOX",
    "MOQS", "MORA", "MORE", "MORR",
]

# Files allowed to mention the fabricated codes (for audit/historical context).
ALLOWED_FILES = {
    "CHANGELOG.md",
    "docs/ROOT_CAUSE_AUDIT.md",
    "dlt-pipelines/fact_pipelines/dlt_fact_cat_customer_records.py",
    "guardrails/validate_no_fabrications.py",
}

# File suffixes to scan
SCANNED_SUFFIXES = {".py", ".sql", ".md", ".csv", ".mmd", ".txt", ".yml", ".yaml", ".json"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--show-allowed", action="store_true",
                        help="Print the allowed-files list and exit.")
    args = parser.parse_args()

    if args.show_allowed:
        for f in sorted(ALLOWED_FILES):
            print(f)
        return 0

    pattern = re.compile(r"\b(" + "|".join(re.escape(c) for c in FABRICATED_CODES) + r")\b")
    violations: list[tuple[str, int, str, str]] = []

    for p in REPO.rglob("*"):
        if not p.is_file():
            continue
        if any(part.startswith(".") for part in p.relative_to(REPO).parts):
            continue
        rel = p.relative_to(REPO).as_posix()
        if rel in ALLOWED_FILES:
            continue
        if p.suffix not in SCANNED_SUFFIXES:
            continue
        try:
            text = p.read_text()
        except UnicodeDecodeError:
            continue
        for line_num, line in enumerate(text.split("\n"), start=1):
            for m in pattern.finditer(line):
                violations.append((rel, line_num, m.group(1), line.strip()[:140]))

    if not violations:
        print("PASS - no fabricated event codes found outside allowed files.")
        return 0

    print(f"FAIL - {len(violations)} fabricated-code references found:\n")
    for rel, line_num, code, line in violations:
        print(f"  {rel}:{line_num}  [{code}]  {line}")
    print()
    print("Each of these 24 codes was invented by AI generation in v2.0.0 and")
    print("does not appear in any FINRA CAT specification. Either:")
    print(" 1. Remove the reference, or")
    print(" 2. Move the file to ALLOWED_FILES in this script if the reference is")
    print("    legitimate audit context (CHANGELOG, ROOT_CAUSE_AUDIT, etc.)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
