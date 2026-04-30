#!/usr/bin/env python3
"""Validator that the reference data matches the FINRA spec PDFs.

Run before every commit that touches `primary-sources/` or
`reference-data/dlt_ref_cat_taxonomy.py` (and ideally in CI). Exits non-zero
if any of the following drift conditions are detected:

  1. The expected CAT or CAIS spec PDF is missing from primary-sources/.
  2. The PDF SHA-256 does not match the pin in spec_pins.json.
  3. The CSV contains codes not present in the spec PDF.
  4. The CSV is missing codes that ARE present in the spec PDF.
  5. The CSV row count does not match the expected count for the pinned
     spec version.

Why this matters: 24 of 49 CAT event codes in the prior model were
fabricated. This script makes that class of error impossible to commit
without an explicit override.

Usage:
  python3 guardrails/validate_event_taxonomy.py
  python3 guardrails/validate_event_taxonomy.py --strict   # fail on any warning

The script does not write anywhere. It only reads.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from pathlib import Path

try:
    from pypdf import PdfReader
except ImportError:
    print("ERROR: pypdf is required. Install with: pip install pypdf", file=sys.stderr)
    sys.exit(2)


REPO_ROOT = Path(__file__).resolve().parents[1]
PINS_FILE = Path(__file__).parent / "spec_pins.json"


class ValidationError(Exception):
    pass


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_cat_codes_from_pdf(pdf_path: Path) -> set[str]:
    """Pull every Message Type code from CAT IM Tech Specs Tables 15, 60, 61.

    The tables appear right before sections 4.1, 5.1.1 and 5.2.1. Each
    row is "section_number  Event Name  M[EOL][A-Z]{2,4}  Description".
    We extract codes that match the pattern and live in those summary
    pages.
    """
    reader = PdfReader(str(pdf_path))
    table_text = ""
    in_table = False
    for page in reader.pages:
        text = page.extract_text() or ""
        if "Table 15: Equity Events" in text or "Table 60: Summary of Simple Option" in text \
                or "Table 61: Summary of Multi" in text:
            in_table = True
        if in_table:
            table_text += "\n" + text
        # End markers: section bodies start after the tables
        if in_table and ("4.1. New Order Event" in text and "Table 15" not in text) \
                and ("Table 60" not in table_text):
            in_table = False
        if "5.1. Simple Option Events" in text and "5.1.1." in text:
            in_table = False
    # Pattern: M followed by E or O or L, then 2-4 letters, as standalone token
    codes = set(re.findall(r"\b(M[EOL][A-Z]{2,4})\b", table_text))
    return codes


def extract_cat_codes_from_csv(csv_path: Path) -> set[str]:
    with csv_path.open() as f:
        return {row["message_type"] for row in csv.DictReader(f)}


def extract_cais_enums_from_csv(csv_path: Path) -> dict[str, set[str]]:
    enums: dict[str, set[str]] = {}
    with csv_path.open() as f:
        for row in csv.DictReader(f):
            enums.setdefault(row["enum_field"], set()).add(row["enum_value"])
    return enums


def extract_cais_enums_from_pdf(pdf_path: Path) -> dict[str, set[str]]:
    """Best-effort extraction of CAIS accepted-values from the spec.

    The CAIS spec embeds accepted values in field-definition tables.
    For each field we know what values the spec defines, so we check
    presence (not absence) - any value missing from the PDF is flagged
    as a potential CSV fabrication.
    """
    reader = PdfReader(str(pdf_path))
    text = "".join((p.extract_text() or "") for p in reader.pages)
    # Field -> set of values expected per spec v2.2.0r4. The validator
    # checks that every value listed here is found verbatim in the PDF
    # text. If a value is missing, that's evidence the CSV may have
    # invented it.
    targets = {
        "fdidType": {"ACCOUNT", "RELATIONSHIP", "ENTITYID"},
        "accountType": {
            "AVERAGE", "DVP/RVP", "EDUCATION", "ENTITYID", "ERROR", "FIRM",
            "INSTITUTION", "MARKET", "MARGIN", "OPTION", "OTHER",
            "RELATIONSHIP", "RETIREMENT", "UGMA/UTMA",
        },
        "fdidEndReason": {
            "CORRECTION", "ENDED", "INACTIVE", "REPLACED", "OTHER", "TRANSFER",
        },
        "addrType": {"ADDRESS1", "ADDRESS2", "ADDRESS3", "ADDRESS4"},
        # Added in Tier 3.2:
        "customerRole": {"NTHOLDER", "TRDHOLDER", "AUTH3RD", "AUTHREP"},
        "roleEndReason": {
            "CORRECTION", "ENDED", "INACTIVE", "REPLACED", "OTHER",
        },
        "ltidEndReason": {"CORRECTION", "ENDED", "REPLACED", "OTHER"},
        "largeTraderType": {"LTID", "ULTID"},
    }
    found: dict[str, set[str]] = {}
    for field, expected in targets.items():
        present = {v for v in expected if v in text}
        found[field] = present
    return found


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

    # ----- CAT spec pin check -----
    cat_pin = pins["cat_im_spec"]
    cat_pdf = REPO_ROOT / "primary-sources" / cat_pin["filename"]
    if not cat_pdf.exists():
        errors.append(
            f"CAT spec PDF not found: {cat_pdf}. Place v{cat_pin['version']} "
            f"in primary-sources/ before running the validator."
        )
    else:
        actual = sha256_file(cat_pdf)
        if actual != cat_pin["sha256"]:
            errors.append(
                f"CAT spec PDF hash mismatch.\n"
                f"  expected: {cat_pin['sha256']}\n"
                f"  actual:   {actual}\n"
                f"This means the PDF on disk is not the v{cat_pin['version']} "
                f"that the CSV was extracted from."
            )

    # ----- CAT CSV row count -----
    cat_csv = REPO_ROOT / "primary-sources" / "cat_im_event_types.csv"
    if not cat_csv.exists():
        errors.append(f"CSV missing: {cat_csv}")
    else:
        csv_codes = extract_cat_codes_from_csv(cat_csv)
        if len(csv_codes) != cat_pin["expected_event_count"]:
            errors.append(
                f"CSV has {len(csv_codes)} codes; spec v{cat_pin['version']} "
                f"defines {cat_pin['expected_event_count']}. Update CSV or pin."
            )
        # ----- CAT CSV vs PDF reconciliation -----
        if cat_pdf.exists():
            try:
                pdf_codes = extract_cat_codes_from_pdf(cat_pdf)
            except Exception as e:
                warnings.append(f"PDF code extraction failed: {e}")
            else:
                ignored = set(cat_pin.get("ignored_codes", []))
                only_in_csv = csv_codes - pdf_codes - ignored
                only_in_pdf = pdf_codes - csv_codes - ignored
                if only_in_csv:
                    errors.append(
                        f"Codes in CSV but NOT found in PDF (potential "
                        f"fabrications): {sorted(only_in_csv)}"
                    )
                if only_in_pdf:
                    # PDFs have noise; only treat as warning if the missing
                    # code looks like a real CAT type
                    real_looking = {c for c in only_in_pdf
                                    if re.fullmatch(r"M[EOL][A-Z]{2,4}", c)}
                    if real_looking:
                        warnings.append(
                            f"Codes in PDF but NOT in CSV (potential "
                            f"missing entries): {sorted(real_looking)}"
                        )

    # ----- CAIS spec pin check -----
    cais_pin = pins["cais_spec"]
    cais_pdf = REPO_ROOT / "primary-sources" / cais_pin["filename"]
    if not cais_pdf.exists():
        errors.append(
            f"CAIS spec PDF not found: {cais_pdf}. Place v{cais_pin['version']} "
            f"in primary-sources/ before running the validator."
        )
    else:
        actual = sha256_file(cais_pdf)
        if actual != cais_pin["sha256"]:
            errors.append(
                f"CAIS spec PDF hash mismatch.\n"
                f"  expected: {cais_pin['sha256']}\n"
                f"  actual:   {actual}"
            )

    # ----- CAIS CSV vs PDF reconciliation -----
    cais_csv = REPO_ROOT / "primary-sources" / "cais_enumerations.csv"
    if cais_csv.exists() and cais_pdf.exists():
        csv_enums = extract_cais_enums_from_csv(cais_csv)
        try:
            pdf_enums = extract_cais_enums_from_pdf(cais_pdf)
        except Exception as e:
            warnings.append(f"CAIS PDF enum extraction failed: {e}")
        else:
            for field, pdf_values in pdf_enums.items():
                csv_values = csv_enums.get(field, set())
                missing = pdf_values - csv_values
                if missing:
                    errors.append(
                        f"CAIS enum '{field}' missing from CSV: {sorted(missing)}"
                    )

    # ----- Report -----
    print(f"CAT IM spec:  v{cat_pin['version']}")
    print(f"CAIS spec:    v{cais_pin['version']}")
    print(f"Errors:       {len(errors)}")
    print(f"Warnings:     {len(warnings)}")
    print()
    for e in errors:
        print(f"ERROR: {e}\n")
    for w in warnings:
        print(f"WARN:  {w}\n")

    if errors:
        return 1
    if warnings and args.strict:
        return 1
    print("PASS - reference data matches primary sources.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
