# CAIS State Machines

This document specifies the operational state-transition flows for the CAIS Customer & Account reporting system. The DDL has tables for these flows but the logic - what to update, in what order, with what idempotency guarantees - lives here.

Source: FINRA CAT CAIS Tech Specs v2.2.0r4. Section numbers below cite the spec.

## Scope

CAIS is file-based, not event-based. Each operational flow is triggered by one or more paired CAIS Data File + Transformed Identifiers File submissions. The Silver and Gold tables that participate are:

| Layer | Table | Purpose |
|-------|-------|---------|
| Silver | `hub_cais_fdid` | One row per FDID (immutable identity) |
| Silver | `hub_cais_customer` | One row per Customer (immutable identity) |
| Silver | `hub_cais_large_trader` | One row per LTID/ULTID |
| Silver | `link_cais_fdid_customer` | FDID-to-Customer relationships with `customer_role`, `role_start_date`, `role_end_date`, `role_end_reason` |
| Silver | `link_cais_fdid_large_trader` | FDID-to-LTID relationships with `ltid_effective_date`, `ltid_end_date`, `ltid_end_reason` |
| Silver | `link_cais_submission` | Per-file submission registry |
| Silver | `sat_cais_fdid_state` | SCD2 attributes of FDID (type, account types, dates, etc.) |
| Silver | `sat_cais_customer_state` | SCD2 attributes of Customer |
| Silver | `sat_cais_fdid_address` | SCD2 addresses for FDID (1..4) |
| Silver | `sat_cais_customer_address` | SCD2 addresses for Customer (1..4) |
| Gold | `fact_cais_fdid` | Snapshot fact for FDID per as-of date |
| Gold | `fact_cais_customer` | Snapshot fact for Customer per as-of date |
| Gold | `fact_cais_submission` | Per-submission registry |
| Gold | `fact_cais_inconsistency` | Material inconsistency tracking |

## Common preconditions

Every CAIS flow shares these checks before any DDL write:

1. The submission is a **paired set**: a CAIS Data File and a Transformed Identifiers File with matching `<CAT Submitter ID>_<CAT Reporter CRD>_<File Generation Date>_<Group>_<File Number>` filename components (Section 5.1.3 rules 5-9).
2. The pair was posted within 60 minutes of each other (Section 5.1.3 rules 6-7); otherwise both are rejected.
3. The `version` field in the CAIS Data File equals `2.0.0` (the schema version for full Customer and Account reporting).
4. The `catReporterCRD` in the file equals the `catReporterCRD` component of the filename.
5. The pre-commit guardrail `validate_event_taxonomy.py` passes against the pinned spec PDF.

If any precondition fails, the submission is rejected and the flow does not start. A row is still written to `link_cais_submission` and `fact_cais_submission` with `accepted = false` for traceability.

---

## Flow 1: New FDID submission (`submission_action = NEW`)

**Trigger**: CAIS Data File contains an `fdidRecordList` with FDID Records that have not previously been accepted by CAT CAIS.

**Section reference**: 5.1.3.1

**State transitions**:

```
[non-existent] --NEW--> [active]
```

**Sequence**:

1. `link_cais_submission` insert with `submission_action = 'NEW'`, `accepted = true`, `file_type = 'CAIS_DATA_FILE'`.
2. For each FDID Record in the file:
   1. Insert into `hub_cais_fdid` if the SHA-256 of `(catReporterCRD, firmDesignatedID)` is new.
   2. Insert into `sat_cais_fdid_state` with all the FDID attributes from the submission. `load_dts = submission.load_dts`, `load_end_dts = NULL` (current).
   3. For each `addressList[]` entry, insert into `sat_cais_fdid_address` keyed on `(cais_fdid_hk, addr_type)`.
   4. For each `fdidCustomerList[]` entry, ensure the Customer exists in `hub_cais_customer` (it must — a Customer Record with the matching `customerRecordID` must be in the same file or a prior accepted submission per Section 5.1.3.1 rule 1).
   5. Insert into `link_cais_fdid_customer` with `customer_role`, `role_start_date = submission.eventTimestamp` (or the explicit `roleStartDate` if provided), `role_end_date = NULL`.
   6. For each `largeTraderList[]` entry, insert into `hub_cais_large_trader` if new, then `link_cais_fdid_large_trader` with `ltid_effective_date` and `ltid_end_date = NULL`.
3. After all FDIDs are processed, snapshot to `fact_cais_fdid` with `as_of_date = submission.file_generation_date`.

**Idempotency**: Re-running the same NEW submission file produces no duplicate hub rows (because `cais_fdid_hk` is deterministic from the BK), and the SCD2 satellite `hash_diff` check skips rows where attributes are unchanged. Re-running ALWAYS produces a new row in `link_cais_submission` for traceability.

---

## Flow 2: FDID update (`submission_action = UPDATE`)

**Trigger**: CAIS Data File contains an FDID Record whose `firmDesignatedID` has been previously accepted.

**Section reference**: 5.1.3 rule 14-15. Records replace prior state in CAIS.

**State transitions**:

```
[active] --UPDATE--> [active with new attributes]
```

**Sequence**:

1. `link_cais_submission` insert with `submission_action = 'UPDATE'`.
2. For each FDID Record:
   1. Look up `cais_fdid_hk` in `hub_cais_fdid`. Must exist; otherwise the submission is converted to NEW.
   2. Compute `hash_diff` over the new attribute tuple.
   3. If different from the current-open `sat_cais_fdid_state` row, close the prior row (`load_end_dts = submission.load_dts`) and insert the new state.
   4. Walk `fdidCustomerList[]` and reconcile:
      - **Implicit ends**: Any Customer that was in the prior state but is omitted from this submission has its `link_cais_fdid_customer.role_end_date` set to `submission.load_dts` and `role_end_reason = 'ENDED'` (per Section 5.1.3.1 rule 25).
      - **Active changes**: A Customer present in both the prior state and the submission with a changed role gets its current link row closed (with reason from the spec or `'OTHER'`), and a new link row inserted with the new role.
      - **New additions**: A Customer present in the submission but not in the prior state gets a fresh `link_cais_fdid_customer` row.
   5. Same logic for `largeTraderList[]` and address records.
3. Re-snapshot to `fact_cais_fdid` with `as_of_date = submission.file_generation_date`.

**Idempotency**: A no-change resubmission produces no new satellite rows (hash_diff matches). The submission row in `fact_cais_submission` is always inserted.

---

## Flow 3: FDID end (`fdidEndReason` populated, not REPLACED or TRANSFER)

**Trigger**: CAIS Data File contains an FDID Record with `fdidEndDate` and `fdidEndReason ∈ {CORRECTION, ENDED, INACTIVE, OTHER}` populated.

**Section reference**: 5.1.3.1 rules 7-13, 38

**State transitions**:

```
[active] --end--> [ended] (terminal until reopened or REPLACED)
```

**Sequence**:

1. `link_cais_submission` insert.
2. For each FDID Record:
   1. Update `sat_cais_fdid_state` current row with `fdid_end_date`, `fdid_end_reason`. Close the row (`load_end_dts = submission.load_dts`).
   2. **Cascade**: For each currently-active LTID association, set `link_cais_fdid_large_trader.ltid_end_date = fdid_end_date` and `ltid_end_reason = fdid_end_reason` if the submission omits the LTID list (Section 5.1.3.1 rule 7), or if the submission explicitly closes the association (rule 9).
   3. **Cascade**: For each currently-active Customer association, set `link_cais_fdid_customer.role_end_date = fdid_end_date` and `role_end_reason = fdid_end_reason` if the submission omits the Customer list (Section 5.1.3.1 rule 21).
   4. **Special case `fdidEndReason = CORRECTION`**: `fdid_end_date` MUST equal `fdid_date` (Section 5.1.3.1 rule 38). Validate; reject the submission if not.
3. Snapshot to `fact_cais_fdid` with `as_of_date = submission.file_generation_date`.

**Reopening**: If the same `firmDesignatedID` is later resubmitted with `fdid_date` updated to the reopening date and no `fdid_end_date`, the SCD2 satellite gets a new current row (Section 5.1.3.1 rule 37). Implementation note: this means SCD2 satellites for a single FDID can have multiple `fdid_date` values across history; the as-of snapshot should use the current open row.

---

## Flow 4: FDID replacement (`fdidEndReason = REPLACED`)

**Trigger**: CAIS Data File contains an FDID Record with `fdidEndReason = 'REPLACED'` and `replacedByFDID` populated.

**Section reference**: 3.1, 5.1.3.1 rules 15-16

**State transitions**:

```
[active] --REPLACED--> [ended-replaced]
[non-existent or active] --target of REPLACED--> [active] (new FDID)
```

**Sequence**:

1. The replacing FDID (the value in `replacedByFDID`) MUST be either:
   - Already accepted in a prior submission, OR
   - Present in the current submission file (Section 5.1.3.1 rule 15)
2. Validate the replacing FDID exists or is in the file. If not, reject the submission and write `fact_cais_inconsistency` with `inconsistency_code = 'REPLACING_FDID_NOT_FOUND'`.
3. End the original FDID per Flow 3, with `fdid_end_reason = 'REPLACED'`.
4. Insert a `link_cais_replacement` row (or, if no such link table is needed for traceability, store the `replaced_by_fdid` value on the closed `sat_cais_fdid_state` row — currently the satellite carries this column).
5. The replacing FDID's flow is independent: if it's new, run Flow 1; if it's already active, run Flow 2.

**Cascading associations**: All Customer and LTID associations on the original FDID are ended (Flow 3 cascade rules). Reporters are responsible for re-establishing the same associations on the replacing FDID via a NEW or UPDATE submission of that FDID.

---

## Flow 5: Mass FDID transfer (`submission_action = MASS_TRANSFER`)

**Trigger**: CAIS Data File submitted by a new acquiring Industry Member (`catReporterCRD` is the new owner) with FDID Records whose `priorCATReporterCRD` and `priorCATReporterFDID` are populated.

**Section reference**: 3.3

**State transitions**:

```
[active under prior IM] --TRANSFER--> [ended-transfer at prior IM]
[non-existent under acquiring IM] --acquired--> [active under acquiring IM] (new FDID with same firmDesignatedID OR a new firmDesignatedID)
```

**Sequence**:

1. The acquiring IM submits a CAIS Data File with `submission_action = 'MASS_TRANSFER'`.
2. For each FDID Record:
   1. Look up the prior FDID via `priorCATReporterCRD` and `priorCATReporterFDID`. Must exist (otherwise the submission is rejected).
   2. End the prior FDID's `sat_cais_fdid_state` current row with `fdid_end_reason = 'TRANSFER'` and `fdid_end_date = submission.eventTimestamp`.
   3. Cascade Customer and LTID associations to ended state with `role_end_reason = 'TRANSFER'` and `ltid_end_reason = 'TRANSFER'` if the file omits them.
   4. Insert into `hub_cais_fdid` for the acquired FDID (under the new IM's CRD). Insert into `sat_cais_fdid_state` with the full new attribute set, including the prior IM/FDID lineage in `prior_cat_reporter_crd` and `prior_cat_reporter_fdid` columns.
3. Snapshot the affected FDIDs and Customers in `fact_cais_fdid` and `fact_cais_customer`.

**Out of band**: The prior IM's reporting obligation for these FDIDs ends on the transfer date. The acquiring IM picks up reporting from the next CAT-reportable event onward.

---

## Flow 6: Periodic refresh (`submission_action = REFRESH`)

**Trigger**: Scheduled refresh per Section 3.8 (annual customer-information re-attestation).

**Section reference**: 3.8

**State transitions**:

```
[active, last refreshed N days ago] --REFRESH--> [active, last refreshed today]
```

**Sequence**:

1. `link_cais_submission` insert with `submission_action = 'REFRESH'`. The submission contains the **complete current state** of FDID and Customer Records as defined in Section 5.1.3 rule 15.
2. For each record, run the UPDATE flow (Flow 2): SCD2 satellite hash-diff comparison, implicit-end of omitted associations, etc.
3. Mark the FDID's `last_refresh_date` column on `sat_cais_fdid_state` (or compute from `fact_cais_submission` lookups). Used for refresh-due reporting.

**Validation**: A refresh submission with ANY FDID `fdid_end_reason = 'INACTIVE'` is allowed; INACTIVE FDIDs are explicitly out of scope for refresh per Section 5.1.3.1 rule 22.

---

## Flow 7: Firm-initiated correction (`submission_action = FIRM_CORRECTION`)

**Trigger**: The Industry Member discovers an error in a previously-accepted submission and submits a correction without waiting for CAT to detect it.

**Section reference**: 6.6.2 (Firm Initiated Corrections)

**State transitions**:

```
[active with bad attributes] --FIRM_CORRECTION--> [active with corrected attributes]
```

**Sequence**: Identical to Flow 2 (UPDATE), except:

1. `link_cais_submission.submission_action = 'FIRM_CORRECTION'`.
2. The corrected SCD2 satellite row's `correction_indicator = 'FIRM'` (carried in a satellite metadata column).
3. If the correction affects an open Material Inconsistency, that inconsistency record's `resolved_dts` and `resolution_submission_sk` are populated.

---

## Flow 8: Repair of CAT-identified error (`submission_action = REPAIR`)

**Trigger**: CAT identifies a Data Validation Error (per Section 6.3) and the firm submits a repair within the deadline (Section 6.4.2).

**Section reference**: 6.6.1 (Repair CAT Errors)

**State transitions**:

```
[error reported by CAT] --REPAIR--> [error resolved]
```

**Sequence**:

1. The submission references the prior `errorROEID` from the CAT feedback file. The error tracking lives in an `errors` table (not yet in the model — see Backlog below).
2. `link_cais_submission.submission_action = 'REPAIR'`.
3. Otherwise identical to Flow 2.

---

## Flow 9: Material inconsistency detection and resolution

**Trigger**: CAT compares the FDID and Customer state across reporters and detects an inconsistency (Section 6.4.1).

**Section reference**: 6.4

**State transitions**:

```
[no inconsistency] --DETECTED--> [open inconsistency]
[open inconsistency] --(corrected by reporter, accepted by CAT)--> [resolved]
```

**Sequence**:

1. CAT publishes an Outstanding Material Inconsistencies feedback file (Section 6.4.2).
2. The feedback ingestion job inserts rows into `fact_cais_inconsistency`:
   - `detected_dts = feedback.publication_dts`
   - `inconsistency_code = feedback.code` (one of the values in Appendix B.5)
   - `severity = 'MATERIAL'`
   - `affected_record_id = feedback.firmDesignatedID` or `feedback.customerRecordID`
   - `affected_record_type = 'FDID' | 'CUSTOMER'`
   - `cais_submission_sk = the submission that triggered detection`
   - `resolved_dts = NULL`
3. Reporter submits a corrective `UPDATE` or `FIRM_CORRECTION` (Flow 2 or 7) that resolves the inconsistency.
4. CAT's next inconsistency-feedback file omits the resolved inconsistency. The reconciliation job updates `fact_cais_inconsistency` with `resolved_dts = current_date()` and `resolution_submission_sk = the corrective submission's sk`.

**Open inconsistencies past the deadline (Section 6.4.3)** result in escalation to the firm's compliance team. The model supports this via a view `vw_cais_overdue_inconsistencies` (not yet built — see Backlog).

---

## Flow 10: TID replacement

**Trigger**: A Customer's TID changes (e.g., new SSN, change of foreign identifier).

**Section reference**: 3.2

**State transitions**:

```
[active TID] --REPLACEMENT--> [retired TID, new active TID for same Customer]
```

**Sequence**:

1. The Transformed Identifiers File contains the new TID for an existing `customerRecordID`. The CAIS Data File for the same submission contains the Customer Record with `correctingCustomerRecordID` populated pointing to the prior Customer Record (Section 3.2 rule on replacement linkage).
2. `link_cais_submission.submission_action = 'REPLACEMENT'`.
3. The prior Customer Record's `sat_cais_customer_state` current row is closed.
4. A new Customer Record is created with the new TID (in the paired TID file, not in any of the in-scope tables here — TID values are intentionally segregated for PII reasons).
5. `link_cais_fdid_customer` rows referencing the prior `customerRecordID` are NOT auto-migrated. The reporter must explicitly resubmit the FDID Records that reference the Customer with the new `customerRecordID` per Section 3.2's caution about implicit-end behaviour.

---

## Backlog

All four DDL gaps surfaced by this state-machine analysis were closed in Tier 9.3 (commit `eb68b26`). The backlog is empty.

| Flow | Gap | Status |
|------|-----|--------|
| Repair (Flow 8) | `fact_cais_outstanding_rejection` table for `errorROEID` -> Outstanding Rejection feedback file. | Closed in Tier 9.3 - table added across all 4 dialects. |
| Inconsistency past deadline (Flow 9) | `vw_cais_overdue_inconsistencies` view. | Closed in Tier 9.3 - view added across all 4 dialects. |
| TID replacement linkage (Flow 10) | `correcting_customer_record_id` column on `sat_cais_customer_state`. | Closed in Tier 9.3 - column added across all 4 dialects. |
| Last-refresh tracking (Flow 6) | `last_refresh_date` column on `sat_cais_fdid_state`. | Closed in Tier 9.3 - column added across all 4 dialects. |

Note: an over-eager Tier 9.3 also placed `last_refresh_date` on Hive Gold `fact_cais_fdid` (a duplicate of the Silver state column). Tier 11 corrected that - the column now lives only on the Silver SCD2 satellite, matching the design intent across all dialects.

## Verification status

- [VERIFIED] Section numbers 3.1 - 3.13, 4.1, 5.1.3, 6.4, 6.6 cited above are in CAIS Tech Specs v2.2.0r4 (table of contents extracted by `validate_event_taxonomy.py`).
- [VERIFIED] All `submission_action` values listed above match the values in `primary-sources/cais_enumerations.csv` (NEW, UPDATE, REFRESH, FIRM_CORRECTION, REPAIR, REPLACEMENT, MASS_TRANSFER).
- [VERIFIED] All `fdidEndReason` values match the spec (CORRECTION, ENDED, INACTIVE, REPLACED, OTHER, TRANSFER).
- [INFERRED] The implementation sequence within each flow is interpreted from the spec rules; the spec describes the data shape and semantics, not the SQL execution order. The order documented here is one consistent way to achieve the spec semantics; an implementer may reorder steps as long as the SCD2 invariants hold and the snapshot is correct.
