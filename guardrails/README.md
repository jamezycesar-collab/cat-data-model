# Guardrails

This directory contains the CI validators that protect against the failure modes catalogued in `docs/ROOT_CAUSE_AUDIT.md` and the closure work documented in `CHANGELOG.md`. For the high-level policy that motivates each check, see `VERIFICATION_PROTOCOL.md` in this directory.

## Quick start

Run all eight validators from the repo root:

```bash
for v in event_taxonomy no_fabrications python_syntax field_specifications \
         diagrams check_constraints cross_dialect_parity event_coverage; do
  python3 "guardrails/validate_$v.py"
done
```

Each script exits 0 on PASS and non-zero on FAIL. The pre-commit hook in this directory wires the same set into git.

## The eight validators

| Validator | Checks | Allowlist file | Notes |
|---|---|---|---|
| `validate_event_taxonomy.py` | `primary-sources/cat_im_event_types.csv` and `cais_enumerations.csv` match the pinned PDFs | — | Re-runs PDF extraction; pinned hashes live in `spec_pins.json` |
| `validate_no_fabrications.py` | No fabricated CAT/CAIS event codes outside allowlisted files | — | Hardcoded scope; flags any unknown 4-letter code starting with `M` |
| `validate_python_syntax.py` | Every `*.py` file under the repo parses cleanly | — | Has an `EXEMPT` list for files awaiting Tier-2-style rewrites |
| `validate_field_specifications.py` | Every row in `ddl/gold/06_cat_field_mapping.csv` has a matching DDL table + column across at least one dialect | `known_field_mapping_gaps.csv` | **Allowlist is at 0 rows** post-Tier 17.5 |
| `validate_diagrams.py` | Every entity in `diagrams/mermaid/*.mmd` resolves to a CREATE TABLE in `ddl/` | `known_diagram_gaps.csv` | Allowlists F7.x backlog entries |
| `validate_check_constraints.py` | Every SQL `CHECK (col IN (...))` enum resolves to a primary-source CSV | — | Internal enums declared in `KNOWN_UNMAPPED_COLUMNS` |
| `validate_cross_dialect_parity.py` | A table's column set is identical across all dialects it appears in | `known_parity_gaps.csv` | Scope: `option`, `multileg`, `cais`, `equity` directories |
| `validate_event_coverage.py` | Every CAT event code in `cat_im_event_types.csv` has ≥1 field-mapping row | `known_uncovered_events.csv` | F4.1 backlog |

## Allowlist pattern

When the audit finds a gap that can't be closed immediately, the validator allowlists it instead of failing CI. Each allowlist file uses the same shape: per-row rationale, audit finding ID, and source line where the gap was first observed.

| File | Rows (current) | Audit finding |
|---|---|---|
| `known_field_mapping_gaps.csv` | **0** | F3.1 phantom tables (cleared in Tier 13–16) + F3.2 missing columns (cleared in Tier 17.1–17.5) |
| `known_diagram_gaps.csv` | 4 | F7.1, F7.2, F7.4 — diagram entities awaiting DDL backfill |
| `known_parity_gaps.csv` | 12 | F5.1, F5.2, F5.3 — intentional dialect divergences (Hive backward-compat, etc.) |
| `known_uncovered_events.csv` | 8 | F4.1 — event codes documented as deferred (MEOF/MEOFS/MEFA, MONQ/MORQ/MOQR/MOQC/MOQM) |

## Adding a new allowlist entry

If a future audit cycle finds a new gap that's documented-deferred rather than immediately fixable:

1. Open the relevant allowlist CSV.
2. Add a row with the same shape as the existing rows: include the audit finding ID, a one-line `reason`, and the source `line` (or first-line for diagram entries) where the gap appears.
3. Run the affected validator locally to confirm it goes from FAIL to PASS-with-warning.
4. Reference the new entry in the CHANGELOG.

The validator's PASS-with-warning output is intentional: the allowlist is a paper trail, not a silencer. Each warning line tells reviewers what's still open.

## Closing a gap (validator-side)

When DDL is added or fixed to resolve an allowlisted gap, just delete the matching row from the allowlist CSV. The validator will then verify the gap is actually closed before passing. If you remove a row but the gap is still present, the validator will fail with a clear "neither in DDL nor in allowlist" error.

The full audit closure arc (Tiers 13–17.5) cleared 212 such rows; the precedent is well-documented in `CHANGELOG.md`.

## Shared utilities

| File | Purpose |
|---|---|
| `_ddl_parser.py` | Balanced-paren CREATE TABLE walker. Replaces the brittle `_CREATE_TABLE_RE` regex previously duplicated in `validate_field_specifications.py` and `validate_cross_dialect_parity.py`. Handles nested parens in types (`DECIMAL(38, 18)`), parens in comments, per-column `COMMENT 'string'` clauses, SQL escaped quotes, block comments, and quoted identifiers. Added in Tier 18.1 after two regex failure modes were observed during Tier 17 development. |
| `test__ddl_parser.py` | 10 regression tests for the parser. Run via `python3 guardrails/test__ddl_parser.py`. |
| `spec_pins.json` | Pinned versions, SHA-256 hashes, and effective dates for the CAT IM and CAIS spec PDFs. Used by `validate_event_taxonomy.py` to detect drift. |
| `pre-commit` | git pre-commit hook. Runs all 8 validators on every commit. Install via `ln -s ../../guardrails/pre-commit .git/hooks/pre-commit`. |
| `VERIFICATION_PROTOCOL.md` | Policy document: the four rules every contributor must follow. Read this before submitting reference-data or DDL changes. |

## Post-audit state (as of Tier 17.5, PR #23)

The audit-tracked F3.1/F3.2 backlog is closed. All 8 validators pass with these remaining allowlisted items:

- **F5.x parity diffs (12):** Intentional Hive-vs-non-Hive divergences in `link_*` tables and CAIS `sat_cais_fdid_state`. These are deliberate dialect-specific design choices, not gaps to close.
- **F7.x diagram backlog (4):** Diagrams in `diagrams/mermaid/` reference entities that exist in the conceptual model but don't yet have CREATE TABLE statements. Tracked separately from the audit.
- **F4.1 uncovered events (8):** CAT event codes (MEOF/MEOFS/MEFA, MONQ/MORQ/MOQR/MOQC/MOQM) intentionally deferred because they need dedicated field-table reconciliation. Documented in `docs/event_coverage_status.md`.

The `known_field_mapping_gaps.csv` file is a header-only stub kept for contract stability — future audit cycles can populate it again without code changes.

## Failure modes the parser fixed (Tier 18.1)

Two distinct issues were observed during Tier 17 work that the new `_ddl_parser.py` resolves:

1. **Catastrophic backtracking on parens-in-comments** (Tier 17.3). The previous regex's non-greedy `[^;]+?` body capture could not terminate when comments contained `()` pairs. The new parser tracks paren depth structurally.
2. **Premature termination on per-column `COMMENT 'string'`** (Tier 17.4). The previous regex's terminator alternation included `COMMENT` as a body-end marker. After a `DECIMAL(38, 18))` close-paren, a column-level `COMMENT` clause looked indistinguishable from the table-level terminator. The new parser only terminates at body depth 0.

If either failure mode reappears (you'll see "phantom table" errors or silently-truncated column lists), check the parser logic first — not the DDL.

## References

- `docs/ROOT_CAUSE_AUDIT.md` — the original failure analysis that motivated the guardrails suite
- `CHANGELOG.md` — full per-tier history of additions, changes, and rationale
- `VERIFICATION_PROTOCOL.md` — the four rules every contributor follows
- `docs/event_coverage_status.md` — per-event coverage status, including the F4.1 deferred set
- `primary-sources/SOURCES.md` — provenance for every pinned spec PDF
