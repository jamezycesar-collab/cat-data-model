<!-- Drop this at .github/PULL_REQUEST_TEMPLATE.md in the main repo. -->

## Summary

<!-- 1-3 sentences describing what changed and why. -->

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Reference data update (requires Verification checklist below)
- [ ] DDL change
- [ ] Pipeline change
- [ ] Documentation only

## Verification (required for any reference-data or regulatory-claim change)

- [ ] Primary source spec PDF is in `primary-sources/`
- [ ] `guardrails/spec_pins.json` updated with filename, version, SHA-256, effective date, and `expected_event_count`
- [ ] CSV in `primary-sources/` re-extracted from the new spec section by section (not diffed against prior)
- [ ] `python3 guardrails/validate_event_taxonomy.py` exits 0
- [ ] `CHANGELOG.md` updated with added / removed / renamed codes
- [ ] All version references updated (README, wiki, `dlt_ref_*.py` constants)
- [ ] Each new factual claim cites a specific section number from the pinned spec

## Self-audit (required for any change that adds or modifies regulatory claims)

The following types of claims need explicit verification before merge:

- [ ] Counts (event types, reference tables, enumerations) come from the validator output, not from memory
- [ ] No `[VERIFIED]` tag is present unless the underlying citation is in the diff
- [ ] No `[INFERRED]` tag is present unless the underlying logic is documented in the same change
- [ ] Round-number claims (50 of 50, 100% coverage, all N events) are checked against actual coverage in `primary-sources/`
- [ ] Any new event code, enum value, or message type is traceable to a specific table or section in a pinned PDF

## Test plan

<!-- How was this verified? -->

- [ ] Validator passes (`python3 guardrails/validate_event_taxonomy.py`)
- [ ] DDL parses on target dialect
- [ ] DLT pipelines run end-to-end against test data (if applicable)
- [ ] Diagrams render
- [ ] Wiki cross-links resolve

## Reviewer checklist

- [ ] I have read the relevant spec section in the pinned PDF, not relied on the description in the diff
- [ ] The validator output is in the CI logs and shows PASS
- [ ] No new `[VERIFIED]` or `[INFERRED]` tag is unsubstantiated
