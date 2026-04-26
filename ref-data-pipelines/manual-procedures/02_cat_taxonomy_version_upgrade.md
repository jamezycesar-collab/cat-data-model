<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 cat-pretrade-data-model contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Runbook 02 - FINRA CAT Taxonomy Version Upgrade

| Attribute | Value |
|-------------------|--------------------------------------------------------------------|
| Cadence | Ad-hoc, triggered by FINRA CAT publication of a new version |
| Owner team | CAT Compliance Engineering |
| Escalation | Head of Regulatory Reporting |
| Target tables | `ref_cat_event_type`, `ref_submission_file_type` |
| DLT pipeline | `ref_ingest_adhoc` |
| Source authority | FINRA CAT LLC (Consolidated Audit Trail) |
| Expected duration | 2 business days (includes impact review + UAT) |

## 1. Purpose

When FINRA CAT releases a new version of the Industry Member Technical Specifications, the CAT event taxonomy seeded in `ingestion/dlt_ref_cat_taxonomy.py` must be upgraded in lockstep. This runbook sequences the impact review, code update, peer review, UAT run, and production cutover.

The taxonomy is the authoritative enumeration used by every order-, execution-, and CAIS-reporting fact table. Drift between a production event code and the seeded enum will cause hard-fail DLT expectations on every downstream fact pipeline.

## 2. Pre-Checks

- Confirm the new FINRA CAT version by checking https://www.catnmsplan.com/specifications
- Download the official release notes PDF (Industry Member Technical Specifications v4.1.1r0 format)
- Confirm the compliance team has filed a Taxonomy Change Ticket in Jira (`CATCOMP-xxxxx`)
- Confirm no active trade-date lives under the pipeline window that would be corrupted by an enum shift

## 3. Impact Review

Produce and circulate a diff document listing, for each changed event code:

- Added events (new code, new description, new phase)
- Retired events (code end-dated)
- Renamed events (old-code superseded-by new-code)
- Changed metadata (description, category, lifecycle stage, phase, submission file type)

Use this SQL against the current seed to generate the reference baseline:

```sql
SELECT cat_event_type_code, cat_event_type_name, category,
 lifecycle_stage, phase, submission_file_type,
 source_version
FROM gold.ref_cat_event_type
WHERE is_active = TRUE
ORDER BY phase, cat_event_type_code;
```

Circulate the diff to:

- CAT Compliance Engineering (author)
- Head of Regulatory Reporting (approver)
- Trade Operations (impact assessment)
- Data Governance (formal sign-off)

A written approval to proceed is required before step 4.

## 4. Code Update

Edit `ingestion/dlt_ref_cat_taxonomy.py` on a feature branch:

```bash
git checkout -b feature/cat-taxonomy-v4.1.1r0
```

Update in exact order:

1. Bump the module-level version string:
 ```python
 CAT_VERSION = "v4.1.1r0"
 CAT_URL = "https://www.catnmsplan.com/sites/default/files/2026-04/CAT-IM-Tech-Specs-v4.1.1r0.pdf"
 ```
2. Update the `CAT_EVENTS` list:
 - Add new event tuples at the correct phase grouping
 - Remove retired events
 - Update descriptions/category/phase for changed events
3. Update `@dlt.expect_all_or_fail` phase/submission enums if the new version adds a phase or file type:
 ```python
 "phase_enum": "phase IN ('2a','2b','2c','2d','2e','2f')", # new 2f
 ```
4. Commit with a structured message:
 ```
 feat(ref): upgrade CAT taxonomy to v4.1.1r0

 - Added 2 new MEOT extension events (MOOTX, MEOTX)
 - Retired MOQS (superseded by MEQS)
 - Updated MEMA phase from 2d to 2e

 Jira: CATCOMP-12345
 ```

## 5. Peer Review

Open a Pull Request into `main` with the following required reviewers:

- CAT Compliance Engineering Lead (technical)
- Data Governance Steward (taxonomy owner)
- Trade Operations SME (impact)

Required CI gates:

- Unit tests pass (`pytest tests/test_ref_cat_taxonomy.py`)
- Schema diff check confirms no breaking column change
- Row-count expectation: seed list length changed as expected

## 6. UAT Run

Deploy to UAT workspace (`cat-uat`) and run the `ref_ingest_adhoc` pipeline in full-refresh mode:

```bash
databricks pipelines start \
 --pipeline-id $(cat /opt/cat/config/uat/ref_ingest_adhoc.id) \
 --full-refresh true
```

Validate in UAT:

```sql
-- New version appears with current-day start
SELECT source_version, COUNT(*), MIN(effective_start_date)
FROM cat_uat.gold.ref_cat_event_type
WHERE is_active = TRUE
GROUP BY source_version;

-- Retired events are end-dated
SELECT cat_event_type_code, effective_start_date, effective_end_date, is_active
FROM cat_uat.gold.ref_cat_event_type
WHERE cat_event_type_code IN ('MOQS') -- example retired code
ORDER BY effective_start_date;

-- All fact-table cat_event_code values still resolve
SELECT f.cat_event_code, COUNT(*) AS orphan_rows
FROM cat_uat.gold.fact_order_events f
LEFT JOIN cat_uat.gold.ref_cat_event_type r
 ON f.cat_event_code = r.cat_event_type_code
 AND f.event_date BETWEEN r.effective_start_date AND r.effective_end_date
WHERE r.cat_event_type_code IS NULL
GROUP BY f.cat_event_code;
-- Expected: 0 rows
```

Run the end-to-end UAT regression suite (`tests/test_ref_cat_taxonomy.py` + fact-gate tests from). All green required.

## 7. Production Cutover

After UAT sign-off:

1. Merge the PR to `main` with a `Signed-off-by` line from Head of Regulatory Reporting
2. CI tag the release: `ref-data-v2026.04.1`
3. Production DLT pipeline auto-deploys via the `prod-ref` GitHub Action
4. Trigger the pipeline for prod:
 ```bash
 databricks pipelines start \
 --pipeline-id $(cat /opt/cat/config/prod/ref_ingest_adhoc.id) \
 --full-refresh false
 ```
5. Watch the pipeline for the SCD2 version crossover - expect N new rows with today's `effective_start_date` and N retired rows with today's `effective_end_date`

## 8. Post-Cutover Validation

```sql
-- Confirm exactly one current version per code
SELECT cat_event_type_code, COUNT(*) AS open_rows
FROM gold.ref_cat_event_type
WHERE is_active = TRUE
GROUP BY cat_event_type_code
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- Confirm source_version advances
SELECT source_version, MIN(effective_start_date), MAX(effective_start_date)
FROM gold.ref_cat_event_type
WHERE is_active = TRUE
GROUP BY source_version;
```

Notify FINRA CAT submission operations via `#cat-submissions` Slack channel that the taxonomy has cut over.

## 9. Rollback Procedure

If production fact-table pipelines start failing on the new taxonomy:

1. Pause all fact pipelines that depend on `ref_cat_event_type`:
 ```bash
 databricks pipelines stop --pipeline-id fact_order_events
 databricks pipelines stop --pipeline-id fact_execution_events
 databricks pipelines stop --pipeline-id fact_cais_snapshots
 ```
2. Restore the prior taxonomy version:
 ```sql
 RESTORE TABLE gold.ref_cat_event_type TO VERSION AS OF <N-1>;
 ```
3. Revert the GitHub tag and re-deploy the prior notebook
4. Resume fact pipelines
5. Declare a P1 incident and schedule a forensic review before attempting the upgrade again

## 10. Sign-Off

| Role | Required |
|-----------------------------------|--------------------|
| CAT Compliance Engineering Lead | Technical sign-off |
| Head of Regulatory Reporting | Production cutover |
| Data Governance Steward | Taxonomy update |
| Trade Operations Lead | Impact sign-off |

All four signatures must be captured on the Taxonomy Change Ticket before the runbook is closed.
