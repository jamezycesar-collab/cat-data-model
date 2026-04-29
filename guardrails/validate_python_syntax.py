#!/usr/bin/env python3
"""Guardrail that fails CI if any tracked Python file in the repo has
invalid syntax.

A Tier 1 sweep revealed many of the existing DLT pipeline files are
pseudo-code: they look like Databricks DLT notebooks but don't parse
as Python. This guardrail catches that class of error.

Files in EXEMPT are exempted (e.g. originals being kept for reference
during a deprecation window). Add to the exempt list rather than
silencing this check.

Usage:
  python3 guardrails/validate_python_syntax.py
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]

# Files known to be pseudo-code awaiting rewrite. Add or remove as the
# rewrite work progresses. Each exemption should reference a tracking
# issue or document the rewrite plan.
EXEMPT = {
    # The original v2.0.0 DLT pipelines are pseudo-code (`def foo:` with
    # no parens, etc). They are scheduled for rewrite in Tier 2. Listed
    # explicitly so this validator doesn't go green silently.
    "dlt-pipelines/dim_pipelines/dlt_dim_account.py",
    "dlt-pipelines/dim_pipelines/dlt_dim_date.py",
    "dlt-pipelines/dim_pipelines/dlt_dim_desk.py",
    "dlt-pipelines/dim_pipelines/dlt_dim_instrument.py",
    "dlt-pipelines/dim_pipelines/dlt_dim_party.py",
    "dlt-pipelines/dim_pipelines/dlt_dim_trader.py",
    "dlt-pipelines/dim_pipelines/dlt_dim_venue.py",
    "dlt-pipelines/fact_pipelines/dlt_fact_cat_allocations.py",
    "dlt-pipelines/fact_pipelines/dlt_fact_cat_order_events.py",
    "dlt-pipelines/fact_pipelines/dlt_fact_cat_quotes.py",
    "dlt-pipelines/tests/test_dim_scd2_invariants.py",
    "dlt-pipelines/tests/test_fact_quality_gates.py",
    "dlt-pipelines/tests/test_referential_integrity.py",
    "ref-data-pipelines/ingestion/dlt_ref_cat_taxonomy.py",
    "ref-data-pipelines/ingestion/dlt_ref_cais_taxonomy.py",
    "ref-data-pipelines/ingestion/dlt_ref_firm_taxonomy.py",
    "ref-data-pipelines/ingestion/dlt_ref_fix_enums.py",
    "ref-data-pipelines/ingestion/dlt_ref_industry.py",
    "ref-data-pipelines/ingestion/dlt_ref_isda_conventions.py",
    "ref-data-pipelines/ingestion/dlt_ref_iso_codes.py",
    "ddl/_delta_to_fabric.py",
    "ddl/expanded-model/_delta_to_hive.py",
    "ddl/gold/07_submission_generator.py",
    "diagrams/drawio/_build_drawio.py",
}


def main() -> int:
    errors = []
    for p in REPO.rglob("*.py"):
        if any(part.startswith(".") for part in p.relative_to(REPO).parts):
            continue
        rel = p.relative_to(REPO).as_posix()
        if rel in EXEMPT:
            continue
        try:
            ast.parse(p.read_text())
        except SyntaxError as e:
            errors.append(f"{rel}:{e.lineno}: {e.msg}")
        except UnicodeDecodeError:
            continue

    if not errors:
        print(f"PASS - {len(list(REPO.rglob('*.py'))) - len(EXEMPT)} Python files parse cleanly.")
        print(f"       {len(EXEMPT)} exempted files awaiting Tier 2 rewrite (see EXEMPT list).")
        return 0

    print(f"FAIL - {len(errors)} syntax errors:\n")
    for e in errors:
        print(f"  {e}")
    print()
    print("Each Python file in this repo must parse with `ast.parse()`. If a file")
    print("is in active rewrite (e.g. an original v2.0.0 pseudo-code pipeline),")
    print("add it to EXEMPT in this script with a comment pointing to the issue.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
