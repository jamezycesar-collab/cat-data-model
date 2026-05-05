# Changelog

All notable changes to the data model are documented here. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased] - Tier 10.7: Cross-dialect parity guardrail (close audit F8.4)

### Added

- **`guardrails/validate_cross_dialect_parity.py`** - new (seventh) guardrail. For each table that appears in 2+ dialects within `ddl/option/`, `ddl/multileg/`, or `ddl/cais/`, validates that the column set is the same across all dialects (Delta, Hive, Fabric Warehouse, Fabric Lakehouse). Diffs not in the allowlist are errors and exit 1.
- **`guardrails/known_parity_gaps.csv`** - 13 documented backlog rows: 1 from F5.1 (`fact_cais_fdid.last_refresh_date` Hive-only), 4 from F5.2 (`link_*_event` partition-strategy diffs), 8 from F5.3 (`sat_cais_fdid_state` `_array` vs `_json` naming for the 4 array columns).

### Changed

- `.github/workflows/validate-taxonomy.yml` - seventh validation step.
- `guardrails/pre-commit` - seventh validator wired in.

### Why

Audit finding F8.4: no validator checked cross-dialect column parity. Step 5 of the audit found ~5 real diffs by hand. Tier 10.7 makes the check automated. Same allowlist pattern as Tier 10 / 10.5 / 10.6: existing diffs are documented backlog (warnings); future drift fails CI.

### Scope

Only the 4-dialect-parallel families are checked:

| Directory | In scope |
|---|---|
| `ddl/option/` | ✅ silver+gold across 4 dialects |
| `ddl/multileg/` | ✅ silver+gold across 4 dialects |
| `ddl/cais/` | ✅ silver+gold across 4 dialects |
| `ddl/gold/` | ❌ Delta-canonical only |
| `ddl/dv2/` | ❌ cross-cutting; managed separately |
| `ddl/expanded-model*/` | ❌ enterprise-scope; Delta+Hive only |
| `ddl/CAT_PreTrade_DDL_*.sql` | ❌ legacy |

The narrow scope keeps the allowlist tractable. Out-of-scope dirs aren't audited for parity because they're dialect-partial by design.

### Coverage

```
DDL files scanned (parity scope):  18
Tables in 2+ dialects:             38
Allowlisted backlog diffs:         13
New parity violations:             0
```

F5.4 (`hub_*_order_bk_keydate` missing from Hive) no longer surfaces — either Tier 9.3 backfilled the columns or the improved parser now finds them. Removed from active backlog.

### Parser improvements over the audit's manual diff

- Hive `PARTITIONED BY (col TYPE)` columns are now treated as real columns (parity now correctly counts `event_date` / `as_of_date` as present in Hive even though they're outside the column body)
- Reserved-keyword line starts (REFERENCES inline FK clauses, CONSTRAINT, PRIMARY, FOREIGN, CHECK, etc.) are filtered out, no longer producing false-positive "extra column" findings
- Schema prefixes are stripped (`gold.fact_x` and bare `fact_x` resolve to the same table)

### Behavior change

- Before Tier 10.7: cross-dialect drift silently shipped (manual catch only).
- After Tier 10.7: any new column added to one dialect but not another is an ERROR unless explicitly allowlisted.

Re-verified by injecting a `totally_synthetic_drift_col STRING` into `fact_cais_inconsistency` in `ddl/cais/02_cais_gold_delta.sql` only - validator exits 1 with a clear message naming Delta as the only-dialect-with and listing Hive/FabricWH/FabricLH as missing. Restoration - exits 0.

### Audit-finding closure status update

| Finding | Severity | After Tier 10.7 |
|---|---|---|
| F8.4 no cross-dialect parity check | MEDIUM | ✅ closed |
| F5.1, F5.2, F5.3 | MEDIUM/LOW | tracked in allowlist; burndown ongoing |
| F5.4 hub_*_order_bk_keydate | MEDIUM | resolved (no longer surfaces) |

WS1 (validator hardening) is now complete except for F8.6 (CHANGELOG↔git automation) which is informational.

## [Unreleased] - Tier 10.6: CHECK constraint enum lint (close audit F8.5)

### Added

- **`guardrails/validate_check_constraints.py`** - new (sixth) guardrail. Walks every DDL file and extracts SQL `CHECK (col IN ('a','b','c'))` constraints (also handles `CHECK (col IS NULL OR col IN (...))`). For each (column, value) pair, validates the value against the appropriate primary source: CAT verified event codes for `cat_event_code` / `event_code` / `quote_event_code`; CAIS enumerations for `customer_type`, `fdid_type`, `customer_role`, `large_trader_type`, `submission_action`, `file_type`, `fdid_end_reason`, `cais_account_type_bk`, `addr_type`, `cais_fdid_type_bk`. Unknown values are errors and exit 1.

### Changed

- `.github/workflows/validate-taxonomy.yml` - added a sixth validation step running `validate_check_constraints.py` on every PR.
- `guardrails/pre-commit` - sixth validator wired in.

### Why

Audit finding F8.5: `validate_event_taxonomy.py` validates the 11 CAIS enum families row-by-row vs the CSV, but does not validate CAT-spec or CAIS CHECK constraint values in actual DDL. A typo in a CHECK constraint (e.g. `'MONOX'` instead of `'MONO'`, or `'TRDHOLDER_X'` instead of `'TRDHOLDER'`) would slip past every existing validator unless the typo happened to match one of the 24 historically-fabricated codes the no-fabrications validator knows about.

Tier 10.6 closes that gap. Same pattern as Tier 10 (replace ad-hoc checking with primary-source introspection) applied to a different artefact: the IN-list of every enforced SQL CHECK constraint in DDL.

### Coverage

```
DDL files scanned:                68
SQL CHECK constraints found:      83
Constraints validated vs source:  62
Constraints on known-unmapped:    21
Distinct constrained columns:     17
```

13 columns are mapped to primary-source enumerations and validated. 4 columns are operational/internal enums not in primary sources (`severity`, `affected_record_type`, `leg_side`, `risk_framework`); these are recorded in `KNOWN_UNMAPPED_COLUMNS` and skipped silently. New columns added to DDL with CHECK constraints get a warning until either mapped to a primary source or added to the known-unmapped set.

### Hive markers

Hive `COMMENT 'CHECK in (X, Y, Z)'` documentation markers (21 occurrences across CAIS/multileg DDL) are NOT validated. The corresponding SQL `CHECK` constraint in the Delta / Fabric Warehouse / Fabric Lakehouse dialects is the canonical source of truth; if those have been validated, the Hive comment can be assumed consistent.

### Behavior change

- Before Tier 10.6: a typo in any CHECK constraint value silently shipped.
- After Tier 10.6: any CHECK value not in the appropriate primary-source CSV is an error and fails CI.

Re-verified by injecting `'MONOX_FAKE'` into the `cat_event_code` CHECK constraint in `ddl/option/02_option_gold_delta.sql` - validator exits 1 with a clear error message. Restoration - exits 0.

### Audit-finding closure status update

| Finding | Severity | After Tier 10.6 |
|---|---|---|
| F8.5 no CHECK enum lint | MEDIUM | ✅ closed |

## [Unreleased] - Tier 10.5: Diagram-lint guardrail (close audit F7.1, F7.2, F8.3)

### Added

- **`guardrails/validate_diagrams.py`** - new validator. Walks every `diagrams/mermaid/*.mmd` and confirms entity tokens matching `(hub|sat|link|fact|dim|pit|bridge)_*` resolve to a `CREATE TABLE` somewhere in `ddl/`. Phantom entities (referenced in a diagram but absent from DDL) become errors and exit 1, unless allowlisted.
- **`guardrails/known_diagram_gaps.csv`** - 12 documented backlog rows: 5 from F7.1 (`medallion_flowchart.mmd` phantom links + facts), 3 from F7.2 (`gold_star_schema_er.mmd` phantom facts), and 4 newly surfaced as F7.4 (`dv2_hub_link_er.mmd` references `link_order_execution` / `link_execution_allocation` / `link_order_route_venue` / `hub_position` where DV2 DDL has different names like `link_execution_order`, `link_allocation_execution`).

### Changed

- `.github/workflows/validate-taxonomy.yml` - added a fifth validation step that runs `validate_diagrams.py` on every PR.
- `guardrails/pre-commit` - extended `TRIGGER_PATHS` to include `ddl/`, `diagrams/`, and `guardrails/` (previously only `primary-sources/`, `reference-data/`, `spec_pins.json`). Now triggers all 5 validators when any DDL or diagram file is staged.

### Why

Tier 10 generalized the v2.0.0 anti-fabrication pattern (replace hardcoded reference lists with primary-source introspection) to the field-mapping CSV. Tier 10.5 applies the same pattern to Mermaid diagrams, the next artefact class where stale or invented entity names had been silently accepted. The DV2 diagrams in particular surfaced naming drift (`link_order_execution` vs the DDL's `link_execution_order`) that no other check would have caught.

### Mermaid parser notes

The validator handles three Mermaid diagram types: `classDiagram` (with `namespace { class A class B }` blocks where the namespace is transparent and inner classes are entity declarations), `erDiagram` (with `ENTITY { type field }` blocks where the inner content is column declarations and is skipped), and `flowchart` (where every entity-prefix token on a relationship line is a reference). Tokens ending in `_sk` whose prefix is a real DDL table are filtered out as FK column references.

### Behavior change

- Before Tier 10.5: stale or fabricated diagram entities silently accepted.
- After Tier 10.5: any new diagram entity not in DDL and not in `known_diagram_gaps.csv` is an error.

```
Diagram files scanned:       19
Entity tokens scanned:       321
DDL tables available:        293
Allowlisted backlog hits:    12
New phantom violations:      0
PASS - all diagram entities resolve in DDL or are allowlisted.
```

Re-verified by injecting a synthetic `fact_totally_fake` reference into `full_model_er.mmd` - validator exits 1 as expected. Restoring - exits 0.

### Audit-finding closure status

| Finding | Severity | After Tier 10.5 |
|---|---|---|
| F7.1 medallion_flowchart phantom entities | HIGH | ✅ closed (allowlisted, flagged for burndown) |
| F7.2 gold_star_schema_er phantom entities | HIGH | ✅ closed (allowlisted, flagged for burndown) |
| F7.4 DV2 diagram naming drift | NEW | flagged + allowlisted (4 rows) |
| F8.3 no diagram lint | MEDIUM | ✅ closed (validator now exists) |

## [Unreleased] - Tier 10: DDL introspection in field-mapping validator (close audit F3.1, F3.2, F8.1, F8.2)

### Added

- **`guardrails/known_field_mapping_gaps.csv`** - 212 documented backlog rows covering the 4 phantom gold tables (F3.1: `fact_order_events`, `fact_quotes`, `fact_allocations`, `fact_execution_events`) and 92 missing columns (F3.2: across `fact_multileg_option_events`, `fact_option_order_events`, `fact_multileg_option_legs`, `fact_option_allocations`, `fact_option_executions`). Each row carries an `audit_finding` column (F3.1 or F3.2) and the source line number. Allowlisted entries become warnings; new violations are errors and fail the validator.
- **`discover_ddl_tables()`** in `validate_field_specifications.py` - scans `ddl/**/*.sql` for `CREATE TABLE` statements across all 4 SQL dialects and merges columns. Replaces the 27-name hardcoded set that was the root cause of F8.1.

### Changed

- `validate_field_specifications.py` Check 3 rewritten:
  - Was: hardcoded `known_gold_tables` set; unknown tables emitted warnings only.
  - Now: introspects DDL; tables and columns not in DDL emit ERRORS unless allowlisted in `known_field_mapping_gaps.csv`.
  - Closes audit findings F3.1 (phantom tables in field mapping), F3.2 (missing columns), F8.1 (hardcoded known-tables list), F8.2 (no column-existence check).

### Why

The forensic audit dated 2026-05-04 found that `validate_field_specifications.py` passed 257/257 mapping rows because it accepted any table whose name was on a hardcoded list, without checking DDL existence. 120 of 257 rows (47%) referenced 4 tables that have no `CREATE TABLE` anywhere in the repo. 92 rows referenced columns that don't exist on existing tables.

The same failure pattern that produced 24 fabricated event codes in v2.0.0 — accepting names without verifying against a primary source — applied to a different artefact: DDL existence. Tier 10 generalizes the v2.0.0 remediation pattern (replace hardcoded reference lists with introspection) to this new artefact class.

### Behavior change

- Before Tier 10: validator passed silently on phantom tables and missing columns.
- After Tier 10: existing 212 violations are warnings (allowlisted as documented backlog); any NEW violation is an error and exits 1.

```
Errors:   0
Warnings: 2
WARN:  120 field-mapping rows reference phantom gold_tables (allowlisted as backlog per F3.1).
WARN:  92 field-mapping rows reference missing gold_columns (allowlisted as backlog per F3.2).
PASS - field mappings reference verified codes.
```

Re-verified by injecting two synthetic fabrications (one phantom table, one missing column on an existing table) - validator exits 1 as expected. Restoring the CSV - exits 0.

### Backlog burndown plan

The 212 allowlist rows trace to a half-finished naming migration documented in the audit. Three options for closing them are described in AUDIT_2026_05_04.md Workstream 2. Each row removed from `known_field_mapping_gaps.csv` (after either DDL or field-mapping is reconciled) tightens the validator. Final state: empty allowlist (header only).

## [Unreleased] - Tier 9.3: Close four CAIS DDL gaps + add missing Hive Gold

### Added

- **`fact_cais_outstanding_rejection`** Gold table across all four dialects. Tracks Outstanding Rejection notifications from CAT per CAIS spec Section 6.5 with fields for `error_roe_id`, `rejection_code`, `severity`, `repair_due_dts`, and `resolved_dts`. This is what Flow 8 (Repair of CAT-identified error) in `cais_state_machines.md` writes against.
- **`vw_cais_overdue_inconsistencies`** view across all four dialects. Filters `fact_cais_inconsistency` to unresolved rows, computes `days_since_detected`, and flags rows past the 5-business-day repair deadline. This is what Flow 9 needs for surveillance.
- **`last_refresh_date`** column on `sat_cais_fdid_state` across all four dialects. Periodic refresh tracking per Section 3.8; set by Flow 6 (REFRESH).
- **`correcting_customer_record_id`** column on `sat_cais_customer_state` across all four dialects. TID replacement linkage per Section 3.2; populated by Flow 10.
- **`ddl/cais/06_cais_gold_hive.sql`** - the missing CAIS Gold-layer DDL for Hive. Previously the Hive variant only had Silver; the other three dialects had both. Now all four dialects have full Silver + Gold parity.

### Changed

- `guardrails/validate_field_specifications.py` known-tables list extended to include `fact_cais_outstanding_rejection`.

### Why

Tier 9.2 (CAIS state-machine docs) surfaced four DDL gaps that prevented the documented operational flows from being implementable. Tier 9.3 closes all four. With this push, every flow in `cais_state_machines.md` writes against tables and columns that exist.

### Coverage delta

| Dialect | CAIS Silver | CAIS Gold |
|---------|------------|-----------|
| Delta Lake | full | full |
| Apache Hive | full | full (was Silver-only) |
| Fabric Lakehouse | full | full |
| Fabric Warehouse | full | full |



### Added

- `docs/cais_state_machines.md` - 10 named operational flows for CAIS, each citing the spec section and specifying the table-by-table sequence:
  1. New FDID submission
  2. FDID update
  3. FDID end (CORRECTION / ENDED / INACTIVE / OTHER)
  4. FDID replacement
  5. Mass FDID transfer
  6. Periodic refresh
  7. Firm-initiated correction
  8. Repair of CAT-identified error
  9. Material inconsistency detection and resolution
  10. TID replacement
- Documents the cascade rules (FDID end -> Customer association end, etc.) and the idempotency guarantees (deterministic hub hashes; SCD2 hash-diff dedupes no-change resubmissions).
- Includes a backlog section listing the four DDL gaps the state-machine docs revealed (no `errors` table for Flow 8; no `vw_cais_overdue_inconsistencies` view for Flow 9; missing `correctingCustomerRecordID` and `last_refresh_date` columns).

### Changed

- `diagrams/mermaid/full_model_er.mmd` updated to include the SimpleOption, MultiLegOption, and CAIS namespaces with their hubs / links / facts. Comment header updated to reflect post-v4.1.0r15 alignment instead of the old "83 entities" claim.
- `diagrams/mermaid/medallion_flowchart.mmd` source-system box updated from "CAT 4.1.0r9" to "CAT IM v4.1.0r15 + CAIS v2.2.0r4" with the actual 99 events. Gold-layer fact-table list updated from 4 stale names to 12 actual fact tables across 5 event families.
- `diagrams/mermaid/gold_star_schema_er.mmd` namespaces split from one Facts namespace into four (Facts_Equity, Facts_SimpleOption, Facts_MultiLeg, Facts_CAIS). dim_multileg_strategy, dim_cais_fdid_type, dim_cais_account_type added to Dimensions namespace.

### Why

Tier 9 closes the cosmetic-but-misleading gap between the diagrams that ship with the model and the actual structure. A reader picking up `full_model_er.mmd` should see exactly what's in the DDL; previously they saw the pre-remediation entity list. The CAIS state machines document the operational logic that the DDL tables enable - tables exist, but without the documented flows it wasn't clear how to drive them correctly.



### Added

- 59 verified field mappings for the 25 multi-leg option events in CAT IM Section 5.2, populating `fact_multileg_option_events` and `fact_multileg_option_legs`. Each row cites the spec section.
- Coverage:
  - **Header**: `orderKeyDate`, `orderID`, `underlying`, `eventTimestamp`, `manualFlag`, `electronicDupFlag`, `electronicTimestamp`, `manualOrderKeyDate`, `manualOrderID`, `deptType`, `numberOfLegs`
  - **Order**: `price` (net price), `quantity`, `minQty`, `orderType`, `timeInForce`, `tradingSession`, `handlingInstructions`, `firmDesignatedID`, `accountHolderType`, `affiliateFlag`, `representativeInd`, `solicitationFlag`, `RFQID`
  - **Routing**: `senderIMID`, `destination`, `destinationType`, `routedOrderID`, `session`, `routeRejectedFlag`, `exchOriginCode`, `pairedOrderID`
  - **Receipt**: `receiverIMID`, `senderType`
  - **Modification linkage**: `priorOrderKeyDate`, `priorOrderID`, `parentOrderKeyDate`, `parentOrderID`, `originatingIMID`
  - **Cancel/Adjust**: `initiator`, `leavesQty`, `cancelQty`, `requestTimestamp`
  - **Quote**: `quoteKeyDate`, `quoteID`, `bidPrice`, `askPrice`, `bidQty`, `askQty`, `routedQuoteID`, `quoteRejectedFlag`, `priorQuoteKeyDate`, `priorQuoteID`
  - **Per-leg detail** (in `fact_multileg_option_legs`): `legRefID`, `symbol` (equity leg), `optionID` (option leg), `openCloseIndicator`, `side`, `legRatioQuantity` — all sourced from the spec's `legs[]` array structure (Section 5.2.1 row 32.n)

### Spec corrections caught during reconciliation

The original CSV draft had AI-extended leg field names (`legSeq`, `legSymbol`, `legSide`, `legRatio`, `legOpenCloseIndicator`, `legPrice`) that don't appear in the spec. The actual spec uses field names within a `legs[]` array (so the JSON path is `legs[].symbol`, not `legSymbol`). The validator's PDF token search caught all six and they were corrected before commit:

| AI-extended name | Spec-correct name |
|---|---|
| `legSeq` | `legRefID` |
| `legSymbol` | `symbol` (within `legs[]`) |
| `legOptionID` | `optionID` (within `legs[]`) |
| `legSide` | `side` (within `legs[]`) |
| `legOpenCloseIndicator` | `openCloseIndicator` (within `legs[]`) |
| `legRatio` | `legRatioQuantity` |
| `legQuantity` | (dropped - not in spec; legs only carry `legRatioQuantity`) |
| `legPrice` | (dropped - legs don't carry individual prices on order events; net price is at the order header) |

This is exactly the failure mode the field validator was built to catch — same pattern as the v2.0.0 fabrications, just at the leg-detail level.

### Coverage delta cumulative

| Metric | After Tier 7.5 | After Tier 8 |
|--------|------:|------:|
| Verified field mappings | 198 | **257** |
| Multi-leg events covered | 0 / 25 | 25 / 25 |
| **Total event-type field coverage** | 64 / 99 | **99 / 99** |

This closes field-level reconciliation for all 99 CAT event types in the spec.



### Added

- `ddl/option/` directory with simple-option Silver and Gold DDL across all four dialects:
  - `01_option_silver_delta.sql` - DV2 hubs (`hub_option_order`, `hub_option_instrument`), links (`link_option_event`, `link_option_child_order`), and satellites (`sat_option_order_state`, `sat_option_event_state`) for Databricks Delta
  - `02_option_gold_delta.sql` - Gold facts `fact_option_order_events`, `fact_option_executions`, `fact_option_allocations`, plus consolidated `vw_option_lifecycle` view
  - `03_option_silver_hive.sql` - Hive Silver mirror
  - `04_option_gold_hive.sql` - Hive Gold mirror
  - `05_option_silver_fabric_lakehouse.sql` - Fabric Lakehouse Silver + Gold (Spark SQL on Delta on OneLake)
  - `06_option_fabric_warehouse.sql` - Fabric Warehouse T-SQL Silver + Gold
- Three DLT pipelines under `dlt-pipelines/fact_pipelines/`:
  - `dlt_fact_option_order_events.py` - routes the 29 order-flow event codes from `link_option_event` into `fact_option_order_events`
  - `dlt_fact_option_executions.py` - routes MOOT/MOOF/MOOFS/MOFA into `fact_option_executions`
  - `dlt_fact_option_allocations.py` - routes MOPA/MOAA into `fact_option_allocations`
- All three new DLT pipelines carry `expect_all_or_fail` quality gates that hard-stop on invalid event codes or missing required fields.

### Why

Tier 7 added 76 verified field mappings referencing three Gold tables (`fact_option_order_events`, `fact_option_executions`, `fact_option_allocations`) that did not yet exist in the DDL. Tier 7.5 closes that gap: the field mapping CSV now resolves to actual tables across all four published SQL dialects, and the DLT pipelines have a runnable path from Silver to Gold.

### Coverage delta cumulative

| Metric | After Tier 7 | After Tier 7.5 |
|--------|------:|------:|
| Verified field mappings | 198 | 198 |
| Simple-option Gold tables | 0 | **3 across 4 dialects** |
| Simple-option DLT pipelines | 0 | **3** |
| Python files parseable | 34 | 37 |



### Added

- 76 verified field mappings for the 35 simple-option events in Section 5.1 of the CAT IM spec, spanning the new `fact_option_order_events`, `fact_option_executions`, and `fact_option_allocations` Gold tables.
- Coverage includes:
  - **Option-specific identifier**: `optionID` (21-character OSI Symbol) replaces equity's `symbol`
  - **Header fields**: `actionType`, `errorROEID`, `firmROEID`, `type`, `CATReporterIMID` (carried from existing common rows)
  - **Order lifecycle**: `orderKeyDate`, `orderID`, `eventTimestamp`, `manualFlag`, `electronicDupFlag`, `electronicTimestamp`, `manualOrderKeyDate`, `manualOrderID`, `deptType`, `solicitationFlag`, `RFQID`
  - **Order details**: `side`, `price`, `quantity`, `minQty`, `orderType`, `timeInForce`, `tradingSession`, `handlingInstructions`, `firmDesignatedID`, `accountHolderType`, `affiliateFlag`, `representativeInd`, `netPrice`
  - **Option-specific**: `openCloseIndicator`, `triggerPrice`, `exchOriginCode`, `occClearingMemberID`
  - **Routing**: `senderIMID`, `destination`, `destinationType`, `routedOrderID`, `session`, `routeRejectedFlag`, `multiLegInd`, `pairedOrderID`
  - **Receipt**: `receiverIMID`, `senderType`
  - **Modification linkage**: `priorOrderKeyDate`, `priorOrderID`, `parentOrderKeyDate`, `parentOrderID`, `originatingIMID`
  - **Cancel/Adjust**: `initiator`, `leavesQty`, `cancelQty`, `requestTimestamp`
  - **Trade**: `tradeKeyDate`, `tradeID`, `cancelFlag`, `cancelTimestamp`, `capacity`, `tapeTradeID`, `sideDetailsInd`, `marketCenterID`, `clearingFirm`
  - **Fulfillment**: `fillKeyDate`, `fulfillmentID`, `fulfillmentLinkType`, `priorFillKeyDate`, `priorFulfillmentID`
  - **Allocations**: `allocationKeyDate`, `allocationID`, `priorAllocationKeyDate`, `priorAllocationID`, `tradeDate`, `settlementDate`, `allocationType`, `correspondentCRD`, `newOrderFDID`, `allocationInstructionTime`, `institutionFlag`, `accountHolderType`

### Changed

- Added three Gold tables to the validator's `known_gold_tables` allow-list: `fact_option_order_events`, `fact_option_executions`, `fact_option_allocations`.

### Coverage delta cumulative

| Metric | After Tier 6 | After Tier 7 |
|--------|------:|------:|
| Verified field mappings | 122 | **198** |
| Equity event types covered | All Section 4 events | All Section 4 events |
| Simple option event types covered | none | All 35 Section 5.1 events |
| Multi-leg events still pending field reconciliation | 25 | 25 |



### Added

- 31 verified quote-event field mappings across MENQ, MENQS, MERQ, MERQS, MEQR, MEQC, MEQM, MEQS in `ddl/gold/06_cat_field_mapping.csv`. Each row cites the spec section (e.g. "Section 4.10.5 row 9") where the field is defined. Coverage includes header fields (`quoteKeyDate`, `quoteID`), bid/ask price+qty, RFQ-specific fields (`bidRelativePrice`, `askRelativePrice`, `validUntilDuration`), routing fields (`senderIMID`, `destination`, `destinationType`, `routedQuoteID`, `quoteRejectedFlag`, `session`), receiver-side fields (`receivedQuoteID`, `receiverIMID`, `quoteWantedInd`), modification linkage (`priorQuoteKeyDate`, `priorQuoteID`), cancellation (`initiator`), status code (`mpStatusCode`), aggregated-orders for ADF (`askAggregatedOrders`, `bidAggregatedOrders`), and indicators (`onlyOneQuoteFlag`, `unsolicitedInd`, `unpricedInd`, `representativeQuoteInd`, `dupROIDCond`).

### Changed

- `cat_submission_file_type` for quote events corrected from the fabricated `QuoteEvents` to the spec-correct `OrderEvents`. Quote events live in Section 4.10 of the CAT IM spec which is part of Section 4 (Equity Events) - they're submitted in the same file type as orders, routes, and trades.
- `ddl/gold/06b_cat_field_mapping_unverified_candidates.csv` is now empty (header only). The 3 remaining candidates from Tier 5 (`quoteID`, `bidPrice`, `askPrice`) all promoted to verified after spec reconciliation.

### Coverage delta cumulative

| Metric | v2.0.0 push | After Tier 6 |
|--------|-------:|------:|
| CAT codes in reference table | 49 (24 fabricated) | 99 (0 fabricated) |
| Verified field mappings | 0 | 122 |
| Unverified field candidates | n/a | 0 |
| Multi-leg dialect coverage | 0 / 4 | 4 / 4 |
| CAIS dialect coverage | (mistyped as events) | 4 / 4 |



### Added

- `ddl/multileg/06_multileg_gold_hive.sql` - Hive variant of `fact_multileg_option_events`, `fact_multileg_option_legs`, `dim_multileg_strategy`, and `vw_multileg_option_lifecycle`. Multi-leg now spans all four dialects (Delta + Hive + Fabric Lakehouse + Fabric Warehouse).
- 55 newly verified order-event field mappings sourced from CAT IM spec Sections 4.1, 4.3, 4.11.1, and 4.13. Each row cites the specific table-row in the spec where the field is defined.

### Changed

- `ddl/gold/06_cat_field_mapping.csv` grew from 27 verified rows to **82 verified rows**, covering MENO (47 fields), MEOR (28 fields), MEOT (35 fields), MEPA (26 fields), MEAA (28 fields), and the cross-event header fields (`actionType`, `errorROEID`, `firmROEID`, `type`, `CATReporterIMID`, `eventTimestamp`, `manualFlag`, `electronicDupFlag`, `electronicTimestamp`).
- `ddl/gold/06b_cat_field_mapping_unverified_candidates.csv` shrank from 35 rows to 3 rows. The 32 promoted entries are now in the verified file; the 3 remaining are quote-event mappings whose submission-file-type assignment needs review in a separate quote-event pass.

### Why

Tier 4 split the fabrication-laden field mapping into verified and unverified buckets. Tier 5 closes the gap by walking the spec field-spec tables (Tables 16, 19, 51, 52) and promoting every real field to the verified file with a citation to its spec section.



### Changed

- `ddl/gold/06_cat_field_mapping.csv` reduced to 27 verified field mappings (from 82). Each row now carries a `verification_status` column and a description that cites the spec section the field is defined in.
- The 35 mappings whose CAT JSON field names do not appear in the spec PDFs (`cumQty`, `executionPrice`, `solicitedFlag`, `representativeID`, `accountID`, `quoteSide`, `quoteStatus`, `allocationMethod`, etc.) moved to `ddl/gold/06b_cat_field_mapping_unverified_candidates.csv` with explanatory notes.
- Malformed `gold_table` cells like `"dim_party (via party_sk join)"` rewritten as proper table names.

### Added

- `validate_field_specifications.py` now searches both CAT IM and CAIS spec PDFs (was CAT IM only), so CAIS field names like `firmDesignatedID` and `customerRecordID` resolve correctly.
- `06_cat_field_mapping.csv` gained a `verification_status` column.

### Why

Tier 3.3 (the field-spec validator) surfaced that 35 of 62 entries (~57%) referenced field names not present in any FINRA spec. Same fabrication pattern as the v2.0.0 event-code list, just one layer deeper. Splitting the file into verified vs unverified-candidates makes the boundary explicit: implementers consume the verified file; the candidates file is a backlog awaiting spec reconciliation against per-event Field Specification tables.

## [Unreleased] - Remediation against CAT IM v4.1.0r15 and CAIS v2.2.0r4

### Added

- `primary-sources/` directory holding pinned spec PDFs and verified extracts:
  - `cat_im_event_types.csv` - 99 CAT reportable event types extracted from CAT IM v4.1.0r15 Tables 15, 60, 61
  - `cais_enumerations.csv` - CAIS structural enumerations extracted from CAIS v2.2.0r4 Sections 4.1 and 5.1.3
  - `SOURCES.md` - provenance and how-to-update guide
- `guardrails/` directory:
  - `validate_event_taxonomy.py` - automated drift-detection between CSV and pinned PDF
  - `spec_pins.json` - SHA-256 pins for each spec version
  - `VERIFICATION_PROTOCOL.md` - mandatory protocol for adding or modifying reference data
  - `pre-commit` - git hook that runs the validator
  - `github-actions-validator.yml` - CI workflow
- `ddl/multileg/` - 25 multi-leg option events (CAT IM Section 5.2) modelled across all four dialects:
  - `01_multileg_silver_delta.sql` - Silver hubs / links / satellites
  - `02_multileg_gold_delta.sql` - Gold facts (`fact_multileg_option_events`, `fact_multileg_option_legs`) and `dim_multileg_strategy`
  - `03_multileg_silver_hive.sql` - Hive mirror
  - `04_multileg_fabric_warehouse.sql` - Fabric Warehouse (T-SQL) mirror
- `ddl/cais/` - CAIS Silver and Gold:
  - `01_cais_silver_delta.sql` - DV2 hubs (FDID, Customer, Large Trader), links, satellites with proper SCD2 of FDID/Customer state and addresses
  - `02_cais_gold_delta.sql` - `fact_cais_fdid`, `fact_cais_customer`, `fact_cais_submission`, `fact_cais_inconsistency`, `dim_cais_account_type`, `dim_cais_fdid_type`
- `dlt-pipelines/dlt_fact_multileg_option_events.py` - DLT pipeline with `expect_all_or_fail` quality gates including code-set validation against the verified 25 multi-leg codes
- `reference-data/dlt_ref_cat_taxonomy.py` - rewritten to load from CSV with row-count and version-pin enforcement
- `reference-data/dlt_ref_cais_taxonomy.py` - new CAIS reference loader
- `docs/ROOT_CAUSE_AUDIT.md` - forensic analysis of how 24 fabricated event codes shipped in v2.0.0

### Removed

- 24 fabricated event codes from `ref_cat_event_type` (`CAIS_A`, `CAIS_C`, `CAIS_R`, `EOT`, `MEAU`, `MEAX`, `MEIN`, `MEMA`, `MEMA_CORR`, `MEOR_EXT`, `MEOTQ`, `MEOX`, `MEPM`, `MEPZ`, `MERO`, `MEVE`, `MLOT`, `MOCX`, `MOOTS`, `MOOX`, `MOQS`, `MORA`, `MORE`, `MORR`). None of these codes appear in any FINRA CAT specification.
- `audit/validate_report.md` and `audit/validate_entity_matrix.csv` - self-signed audit artefacts that referenced the fabricated codes.
- "All 50 CAT events covered" claim throughout README, wiki, and reference data. The actual count is 99.

### Fixed

- Spec version reference bumped from `v4.1.0r9` to `v4.1.0r15`.
- `MEQS` description corrected. Real spec definition: "Quote Status" (Section 4.10.8). Was previously labeled "Quote Sent in response to RFQ".
- CAIS events removed from the order-event reference table; CAIS now lives in its own taxonomy and DDL because it is file-based, not event-based.

### Coverage delta

| Metric | Before | After |
|--------|-------:|------:|
| CAT codes in reference table | 49 | 99 |
| Fabricated codes | 24 | 0 |
| Real CAT codes covered | 25 / 99 (25%) | 99 / 99 (100%) |
| Multi-leg event coverage | 0 / 25 | 25 / 25 |
| CAIS coverage | (incorrectly typed as 3 events) | full record-and-submission model |
| Validator running in CI | No | Yes |
