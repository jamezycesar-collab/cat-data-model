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

# Silver (Data Vault 2.0) -> Gold (Star Schema) ETL Design

**Platform:** Azure Databricks (Delta Lake, Spark SQL)
**Source:** Silver - Data Vault 2.0 Raw/Business Vault, PIT/Bridge query helpers
**Target:** Gold - 8 conformed SCD2 dims, 4 event-grain facts, 2 ops tables (see `01_dim_tables.sql`, `02_fact_tables.sql`, `03_operational_tables.sql`)
**Cadence:** Intraday micro-batch (15 min) for facts; nightly for dims; daily for CAIS snapshot

---

## 1. Orchestration Overview

```
Silver hubs/links/sats ──► Silver PIT/Bridge ──► Gold Dim Loader ──► Gold Fact Loader ──► Submission Generator
 (streaming bronze feed) (snapshot views) (SCD2 merge) (append-only) (07_*.py)
```

Load order (enforced in Databricks Workflow DAG):

1. `dim_date` - static seed (run once on setup; refresh annually for trading calendar)
2. `dim_event_type` - static seed (50 CAT events; refresh only on CAT tech spec upgrade)
3. `dim_party`, `dim_instrument`, `dim_venue`, `dim_account`, `dim_desk`, `dim_trader` - nightly SCD2 merge
4. `fact_cat_order_events`, `fact_cat_allocations`, `fact_cat_quotes` - intraday append
5. `fact_cat_customer_records` - daily snapshot at 18:00 ET
6. `cat_submission_batch` - populated by `07_submission_generator.py`
7. `cat_feedback_error` - populated by CAT feedback ingestion job

---

## 2. Dimension Load Pattern (SCD Type 2)

Every SCD2 dim is loaded via the same pattern:

### 2.1 Source - Silver PIT joined to current sats

```sql
-- Example: dim_party source
WITH current_party AS (SELECT
 h.party_hk,
 h.party_id_bk,
 sd.lei,
 sc.cat_imid,
 sc.crd,
 sc.fdid,
 sd.party_type,
 sd.party_status,
 sd.legal_form,
 sd.incorporation_country,
 sd.domicile_country,
 sd.tax_residency,
 sr.ultimate_parent_party_id,
 sr.g_sib_flag,
 sd.pep_flag,
 sd.mifid_ii_category,
 pit.load_date_effective AS effective_start_date,
 sha2(concat_ws('|', sd.lei, sc.cat_imid, sc.crd, sd.party_type,
 sd.party_status, sd.legal_form, sd.incorporation_country,
 sd.domicile_country, sd.tax_residency),
 256) AS row_hash,
 h.party_hk AS dv2_source_hk
 FROM silver.hub_party h
 JOIN silver.pit_party pit ON h.party_hk = pit.party_hk
 JOIN silver.sat_party_details sd ON pit.sat_party_details_pk = sd.sat_party_details_pk
 LEFT JOIN silver.sat_party_cat_ids sc ON pit.sat_party_cat_ids_pk = sc.sat_party_cat_ids_pk
 LEFT JOIN silver.sat_party_regulatory sr ON pit.sat_party_regulatory_pk = sr.sat_party_regulatory_pk
 WHERE pit.is_current_snapshot = TRUE)
```

### 2.2 SCD2 MERGE (atomic close-and-insert)

```sql
MERGE INTO gold.dim_party AS tgt
USING (SELECT *, CURRENT_TIMESTAMP AS load_date FROM current_party) AS src
ON tgt.party_id_bk = src.party_id_bk
AND tgt.is_current = TRUE
WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
 UPDATE SET
 tgt.is_current = FALSE,
 tgt.effective_end_date = src.effective_start_date - INTERVAL 1 DAY
WHEN NOT MATCHED THEN
 INSERT (dim_party_sk, party_id_bk, lei, cat_imid, crd, fdid, party_type, party_status,
 legal_form, incorporation_country, domicile_country, tax_residency,
 ultimate_parent_party_id, g_sib_flag, pep_flag, mifid_ii_category,
 effective_start_date, effective_end_date, is_current, row_hash,
 dv2_source_hk, load_date, record_source) VALUES (monotonically_increasing_id, src.party_id_bk, src.lei, src.cat_imid,
 src.crd, src.fdid, src.party_type, src.party_status, src.legal_form,
 src.incorporation_country, src.domicile_country, src.tax_residency,
 src.ultimate_parent_party_id, src.g_sib_flag, src.pep_flag,
 src.mifid_ii_category, src.effective_start_date, DATE '9999-12-31',
 TRUE, src.row_hash, src.dv2_source_hk, src.load_date, 'silver.hub_party');
```

After the UPDATE closes the prior version, a follow-on INSERT opens the new version (two-step). Production implementation uses `MERGE... WHEN NOT MATCHED BY SOURCE` + `WHEN MATCHED AND changed` with a generated view; see `senior-data-engineer` skill notes on two-step SCD2.

### 2.3 Change detection via `row_hash`

`row_hash = SHA-256(non-key business attributes)`. On mismatch -> new SCD2 row.

---

## 3. Fact Load Pattern (append-only, event-grain)

Facts are intraday append-only from Silver links. Example: `fact_cat_order_events`:

| Gold column | Source | Notes |
|-------------|--------|-------|
| `order_event_fact_sk` | `monotonically_increasing_id` | surrogate |
| `event_date` | `lnk_order_event.event_date` | partition key |
| `firm_roe_id` | `lnk_order_event.firm_roe_id` | CAT unique-per-day-IMID |
| `cat_order_id` | `hub_order.order_id_bk` | via hub |
| `cat_event_code` | `sat_order_event_details.cat_event_code` | FK to dim_event_type |
| `dim_party_sk` | `dim_party` lookup on `hub_party.party_id_bk` | SCD2 current |
| `dim_instrument_sk` | `dim_instrument` lookup | SCD2 current |
| `dim_venue_sk` | `dim_venue` lookup on venue MIC | nullable |
| `dim_account_sk` | `dim_account` lookup | nullable |
| `dim_desk_sk` | `dim_desk` lookup | nullable |
| `dim_trader_sk` | `dim_trader` lookup | nullable |
| `event_timestamp` | `sat_order_event_details.event_timestamp` | DATETIME2(7) |
| `event_nanos` | `sat_order_event_details.event_nanos` | for MEOTQ |
| `quantity`, `price`, `side`, etc. | `sat_order_event_details.*` | 1-to-1 |
| `dv2_source_hk` | `lnk_order_event.event_link_hk` | BCBS 239 lineage |

### 3.1 Merge strategy

```sql
MERGE INTO gold.fact_cat_order_events AS tgt
USING staging_order_events AS src
ON tgt.firm_roe_id = src.firm_roe_id
AND tgt.cat_event_code = src.cat_event_code
AND tgt.event_date = src.event_date
WHEN NOT MATCHED THEN INSERT (...) -- append-only, idempotent
WHEN MATCHED AND src.event_timestamp > tgt.event_timestamp THEN UPDATE SET...
;
```

Because CAT corrections (MEOJ) are modeled as **new events** (not updates), the merge only inserts when `(firm_roe_id, cat_event_code, event_date)` is new.

### 3.2 SCD2 dimension lookup (point-in-time correct)

Every FK lookup resolves against the dim row **effective at `event_timestamp`**:

```sql
LEFT JOIN gold.dim_party dp
 ON src.party_id_bk = dp.party_id_bk
 AND src.event_timestamp BETWEEN dp.effective_start_date AND dp.effective_end_date
```

---

## 4. Schedule

| Job | Target | Cadence | Window | SLA |
|-----|--------|---------|--------|-----|
| `gold_dim_scd2_merge` | 6 SCD2 dims | Nightly 02:00 ET | 02:00-03:00 | 60 min |
| `gold_dim_static_refresh` | `dim_date`, `dim_event_type` | On-demand | n/a | 15 min |
| `gold_fact_order_events_incremental` | `fact_cat_order_events` | Every 15 min intraday | 04:00-20:00 ET | 8 min |
| `gold_fact_allocations_incremental` | `fact_cat_allocations` | Every 15 min intraday | 09:00-22:00 ET | 8 min |
| `gold_fact_quotes_incremental` | `fact_cat_quotes` | Every 15 min intraday | 04:00-20:00 ET | 8 min |
| `gold_fact_cais_snapshot` | `fact_cat_customer_records` | Daily 18:00 ET | 18:00-18:30 | 30 min |
| `cat_submission_generator` | CAT SFTP | Daily 06:00 ET T+1 | 06:00-07:45 ET | hard 08:00 ET deadline |
| `cat_feedback_ingestion` | `cat_feedback_error` | Hourly from CAT feedback SFTP | 24×7 | 30 min |

---

## 5. Data Quality Gates

Each load enforces GE expectations before commit:

- `expect_column_values_to_not_be_null(firm_roe_id)` - required on every fact row
- `expect_column_values_to_not_be_null(dv2_source_hk)` - BCBS 239 lineage
- `expect_column_values_to_match_regex(cat_event_code, '^[A-Z]{3,7}$')` - CAT taxonomy
- `expect_column_value_lengths_to_be_between(sha256_checksum, 64, 64)` - SHA-256 digest
- `expect_column_values_to_be_in_set(ack_status, [...])` - enum conformance
- Referential: every `dim_*_sk` on fact must exist in the dim - blocked via Databricks constraint `ENFORCED`

---

## 6. Lineage & Audit

- `dv2_source_hk` on every Gold row points back to Silver hub/link hash -> BCBS 239
- `cat_submission_batch.sha256_checksum` + `batch_id` traced into every submitted event -> CAT audit
- `cat_feedback_error.batch_id` + `firm_roe_id` closes the loop to originating Gold fact row
- All tables have `delta.enableChangeDataFeed=true` - change log is queryable for audit

---

## 7. Idempotency

All fact loads are idempotent via Delta MERGE on the unique key
`(firm_roe_id, cat_event_code, event_date)` - replays are safe.
All SCD2 dim loads are idempotent via the `row_hash` check - no-op when source unchanged.

---

