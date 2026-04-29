# Verification Protocol

This document defines how reference data, event taxonomies, and regulatory claims in this repository are verified against primary sources. Every contributor (human or AI) must follow this protocol before submitting a change.

It exists because the prior version of this repository contained 24 fabricated CAT event codes that were never present in any FINRA specification. See `docs/ROOT_CAUSE_AUDIT.md` for what went wrong and why.

## The four rules

### Rule 1: Primary source or no claim

Any factual claim about FINRA CAT, FINRA CAIS, SEC Rule 613, or any related regulation must cite a primary source by document version, page or section number, and SHA-256 hash. Pinned spec PDFs live in `primary-sources/`. Allowed primary sources:

- CAT Industry Member Technical Specifications (`catnmsplan.com/specifications/im`)
- CAIS Technical Specifications (`catnmsplan.com/specifications/im`)
- CAT NMS Plan (the underlying SEC-approved document)
- SEC Rule 613 (17 CFR 242.613)
- FINRA Rule 6800 series

Search-result snippets, vendor blog posts, AI-generated paraphrases, and prior versions of this repository are NOT primary sources.

### Rule 2: Reference data is extracted, not authored

Reference tables that enumerate spec values (event types, accepted-values lists, message types) live as CSV files in `primary-sources/`. They are extracted directly from the spec PDFs using documented procedures, not authored from memory or composed from "what makes sense."

If a value cannot be cited to a specific table or section in the pinned PDF, it does not go in the CSV.

### Rule 3: The validator must pass

`guardrails/validate_event_taxonomy.py` compares the CSV reference data against the PDF on disk. It must exit with code 0 before any commit that touches `primary-sources/` or `reference-data/`. The pre-commit hook in `guardrails/pre-commit` runs it automatically.

The validator checks:

- The expected spec PDFs exist in `primary-sources/`
- Their SHA-256 hashes match `spec_pins.json`
- The CSV row count matches `expected_event_count`
- Every code in the CSV is present in the PDF
- Every event-pattern code in the PDF is present in the CSV (warning, not error, for the long tail)

### Rule 4: Counts come from the validator, not from the writer

When the README, wiki, or other documentation cites a count ("99 event types", "16 reference tables", etc.), the count must come from running the validator or counting the CSV. Hand-typed counts drift; "50 of 50" is what we got last time.

Use this pattern in markdown:

```markdown
The model covers 99 reportable event types
([source][cat-spec], extracted to `primary-sources/cat_im_event_types.csv`).

[cat-spec]: primary-sources/SOURCES.md
```

## Workflow for adding or updating reference data

1. Place the new spec PDF in `primary-sources/`.
2. Compute its SHA-256: `sha256sum primary-sources/*.pdf`.
3. Update `guardrails/spec_pins.json` with the new filename, version, hash, and effective date.
4. Re-extract the affected CSV(s) by walking the spec section by section. Do NOT diff against the prior CSV and assume nothing changed.
5. Update `expected_event_count` in `spec_pins.json` to match the new CSV row count.
6. Run `python3 guardrails/validate_event_taxonomy.py`. Fix any errors.
7. Update `CHANGELOG.md` with the spec version, date, and a summary of added / removed / renamed codes.
8. Update version references in:
   - `README.md`
   - `cat-data-model.wiki/Home.md`
   - `cat-data-model.wiki/CAT-Event-Mapping.md`
   - `cat-data-model.wiki/Reference-Data.md`
   - `reference-data/dlt_ref_cat_taxonomy.py` (the `CAT_SPEC_VERSION` constant)
9. Commit. The pre-commit hook will run the validator one more time.

## What the validator cannot catch

The validator catches drift between the CSV and the pinned PDF. It cannot catch:

- A description in the CSV that misrepresents what the spec actually says (the code is present but the text is wrong)
- A category or phase classification that's misaligned (e.g. a quote event tagged as an order event)
- A change in a section number that didn't add or remove a code
- A regulatory interpretation that's wrong but lexically consistent with the spec

For those, manual review by someone who has read the relevant spec sections is required. The PR template in `.github/PULL_REQUEST_TEMPLATE.md` includes a checkbox for it.

## When the validator is wrong

If the validator flags a real spec code as fabricated, that is a bug in the validator (most likely a regex issue) — fix the validator. If it flags a known-noise code (an exchange identifier that happens to start with `ME`), add it to `ignored_codes` in `spec_pins.json` with a comment explaining why.

Do not silence warnings without a paper trail.

## How to report verification status in deliverables

Any document or deliverable that references regulatory claims must include a "Verification status" section listing:

- [VERIFIED] facts: cited to a specific primary-source page or section
- [INFERRED] claims: derived from verified facts via documented logic
- [SPECULATION] claims: educated guesses; flagged for follow-up

Hedging language ("appears to", "likely", "is generally") without an underlying citation is treated as `[SPECULATION]`.

## Override

In emergencies (e.g. a release blocked by a validator bug), the validator can be bypassed with `--no-verify` on the commit. The override must be accompanied by a `OVERRIDE_<date>.md` note in `docs/overrides/` explaining what was bypassed and why. CI fails on an override note older than 14 days.
