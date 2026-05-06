# CAT Event Coverage Status

Status of every CAT event code in `primary-sources/cat_im_event_types.csv`
against the field mappings in `ddl/gold/06_cat_field_mapping.csv`.

Coverage is enforced by `guardrails/validate_event_coverage.py`. See also
the coverage-depth distribution at the bottom (`--report` flag output).

## Summary (2026-05-04, post-Tier 12)

| Metric | Count |
|--------|-------|
| Verified CAT event codes | 99 |
| Codes with at least one field-mapping row | 91 |
| Codes uncovered (documented backlog, F4.1) | 8 |
| Codes uncovered (NEW drift, must fail CI) | 0 |

## Documented uncovered codes (F4.1 backlog)

These codes are intentionally allowlisted in `guardrails/known_uncovered_events.csv`
pending dedicated field-table reconciliation against the spec.

| Code | Section | Event | Category | Target gold table | Rationale |
|------|---------|-------|----------|-------------------|-----------|
| MEOF | 4.12.1 | Order Fulfillment | EQUITY_EXECUTION | `fact_execution_events` | Distinct field set vs MEOT/MEOTS; partial extension of MEOT rows is unsafe. Awaits per-section reconciliation. |
| MEOFS | 4.12.2 | Order Fulfillment Supplement | EQUITY_EXECUTION | `fact_execution_events` | Supplement of MEOF; same rationale. |
| MEFA | 4.12.3 | Order Fulfillment Amendment | EQUITY_EXECUTION | `fact_execution_events` | Amendment of MEOF/MEOFS; same rationale. |
| MONQ | 5.1.10.1 | New Option Quote | SIMPLE_OPTION_QUOTE | `fact_option_order_events` | Quote-specific fields (quoteID/bidPx/askPx) distinct from order events. CHECK constraint already accepts the code. |
| MORQ | 5.1.10.2 | Option Routed Quote | SIMPLE_OPTION_QUOTE | `fact_option_order_events` | Same family as MONQ. |
| MOQR | 5.1.10.3 | Option Quote Received | SIMPLE_OPTION_QUOTE | `fact_option_order_events` | Same family as MONQ. |
| MOQC | 5.1.10.4 | Option Quote Cancelled | SIMPLE_OPTION_QUOTE | `fact_option_order_events` | Same family as MONQ. |
| MOQM | 5.1.10.5 | Option Quote Modified | SIMPLE_OPTION_QUOTE | `fact_option_order_events` | Same family as MONQ. |

## Future-tier remediation plan

For each F4.1 code, the work is:

1. Read the per-event field table in the spec PDF (sections 4.12.1, 4.12.2, 4.12.3, 5.1.10.1-5).
2. Either add the code to existing field-mapping rows where the field genuinely applies, or add new rows for event-specific fields (e.g. `quoteID`, `bidPx`, `askPx`).
3. For Order Fulfillment: also resolve whether `fact_execution_events` should be built as real DDL (currently phantom; allowlisted in `known_field_mapping_gaps.csv`) or whether MEOF/MEOFS/MEFA should land on `fact_cat_order_events`.
4. Remove the row from `known_uncovered_events.csv`. Validator now enforces coverage.

## Coverage-depth distribution

How many field-mapping rows reference each code (all 99 codes):

```
  0 mapping rows:    8 codes  (the F4.1 backlog)
  2 mapping rows:   11 codes
  3 mapping rows:    5 codes
  4 mapping rows:   22 codes
  5 mapping rows:    3 codes
  6 mapping rows:    5 codes
  7 mapping rows:    4 codes
  8 mapping rows:    4 codes
 10 mapping rows:    3 codes
 11 mapping rows:    4 codes
 12 mapping rows:    1 code
 13 mapping rows:    2 codes
 14 mapping rows:    1 code
 15 mapping rows:    3 codes
 16 mapping rows:    1 code
 17 mapping rows:    4 codes
 18 mapping rows:    2 codes
 19 mapping rows:    1 code
 20 mapping rows:    1 code
 21 mapping rows:    1 code
 22 mapping rows:    1 code
 24 mapping rows:    1 code
 25 mapping rows:    1 code
 26 mapping rows:    1 code
 28 mapping rows:    1 code
 29 mapping rows:    1 code
 30 mapping rows:    3 codes
 31 mapping rows:    2 codes
 34 mapping rows:    1 code
 39 mapping rows:    1 code
```

Coverage depth is informational only - not enforced. A simple cancellation event with 2 fields is legitimate; a new-order event will have 30+ fields. The validator's only hard requirement is that depth >= 1 for every non-allowlisted code.

## Verification status

- [VERIFIED] 91 of 99 codes have at least one row in `06_cat_field_mapping.csv` (computed by `validate_event_coverage.py`)
- [VERIFIED] The 8 allowlisted codes are exactly the ones flagged by audit step 4 (F4.1)
- [VERIFIED] Every code in the allowlist exists in `primary-sources/cat_im_event_types.csv` (validator joins on `message_type`)
- [INFERRED] Target gold-table assignments per code are reasonable given existing CHECK constraints and event-family naming, but pre-implementation - the actual fact tables (`fact_execution_events` is phantom) may change during reconciliation
