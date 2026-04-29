# Runbook: CAT taxonomy version upgrade (ad-hoc)

When FINRA publishes a new version of the CAT IM Technical Specifications, this runbook produces a verified update of the event taxonomy and reference data.

## Prerequisites

- A working clone of `cat-data-model` with the `guardrails/` directory present
- `python3 -m pip install pypdf`
- The new spec PDF downloaded from https://www.catnmsplan.com/specifications/im

## Steps

### 1. Stage the new spec

```bash
cp ~/Downloads/<NEW_VERSION>_CAT_Reporting_Technical_Specifications_for_Industry_Members_v<X.Y.Zr#>_CLEAN.pdf \
 primary-sources/
```

### 2. Compute SHA-256 and update the pin

```bash
sha256sum primary-sources/*.pdf
```

Edit `guardrails/spec_pins.json`:
- Update `cat_im_spec.filename` to the new PDF filename
- Update `cat_im_spec.version` to the new version (e.g. `4.1.0r16`)
- Update `cat_im_spec.effective_date`
- Update `cat_im_spec.sha256`
- Leave `expected_event_count` unchanged for now; you'll set it after step 3

### 3. Re-extract the CSV

Walk the spec PDF section by section. For each event-type table (Tables 15, 60, 61 in the v4.1.0r15 layout), update `primary-sources/cat_im_event_types.csv` to reflect every code with its section number, name, category, phase, and description.

Do NOT diff against the prior CSV and assume nothing changed. New `r-` versions occasionally:
- Add new event codes (always at the end of a section)
- Rename a code's description (the code itself is stable)
- Renumber sections when major sections are inserted
- Move a code from one category to another

### 4. Update expected_event_count

```bash
NEW_COUNT=$(tail -n +2 primary-sources/cat_im_event_types.csv | wc -l)
echo "$NEW_COUNT"
```

Edit `guardrails/spec_pins.json` and set `cat_im_spec.expected_event_count` to `$NEW_COUNT`.

### 5. Run the validator

```bash
python3 guardrails/validate_event_taxonomy.py
```

Resolve every error before continuing. The validator catches:
- PDF SHA-256 mismatches
- CSV/PDF code-set divergence
- Row count drift

### 6. Update version constants in code

Search the tree for the prior version string and update each occurrence:

```bash
git grep -l 'v4\.1\.0r15' | grep -v primary-sources/SOURCES.md
```

Files that need updating typically include:
- `README.md`
- `cat-data-model.wiki/Home.md` (in the wiki repo)
- `cat-data-model.wiki/CAT-Event-Mapping.md`
- `cat-data-model.wiki/Reference-Data.md`
- `dlt-pipelines/dim_pipelines/dlt_dim_event_type.py` (`CAT_SPEC_VERSION` constant)
- `ref-data-pipelines/ingestion/dlt_ref_cat_taxonomy.py` (`CAT_SPEC_VERSION` constant)
- `ddl/gold/01_dim_tables.sql` (table comment)

### 7. Update the changelog

Add an entry to `CHANGELOG.md` under a new release section describing:
- The old and new spec versions
- Codes added (with section numbers)
- Codes removed (with explanation)
- Codes renamed or recategorised
- Any DDL or quality-gate changes that became necessary

### 8. Commit and PR

```bash
git checkout -b cat-spec-upgrade-v<X.Y.Zr#>
git add primary-sources/ guardrails/spec_pins.json README.md CHANGELOG.md \
 dlt-pipelines/ ref-data-pipelines/ ddl/
git commit -m "Upgrade to CAT IM Tech Specs v<X.Y.Zr#>"
git push -u origin cat-spec-upgrade-v<X.Y.Zr#>
gh pr create
```

The CI workflow at `.github/workflows/validate-taxonomy.yml` runs the validator on every push. Merging requires it to pass.

## What can go wrong

| Symptom | Cause | Fix |
|---------|-------|-----|
| Validator says "PDF SHA-256 mismatch" | Wrong PDF on disk, or pin not updated | Re-download the spec or fix the pin |
| Validator says "X codes in CSV but not in PDF" | Old fabricated codes still in CSV, or transcription error | Remove the fabrication or fix the typo |
| Validator says "X codes in PDF but not in CSV" (warning) | Spec added new codes you missed | Add them to the CSV |
| DLT pipeline fails on `exactly_99_events` | Hard count is wrong | Update count in `dlt_dim_event_type.py` AND `spec_pins.json` |

## What this runbook deliberately does NOT do

- Manual review of field-specification tables (DDL column types per event). That is a separate and larger audit; flag it as a follow-on if the new spec changes any data types.
- Reconciliation against the prior CSV. Do the extraction fresh.
