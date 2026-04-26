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

# Data Vault 2.0 Source-to-Target Mapping

**Generated:** (cat-pretrade-refactor skill)
**Purpose:** Complete source-to-target mapping between the Bronze/3NF model and the Data Vault 2.0 Silver layer.
**Consumers:** (Fabric migration), (DLT pipelines), (reference data ingestion).

---

## 1. Layer Overview

```
Bronze (3NF, 83 entities)
 |
 v [, hash_diff, SCD2]
 |
Silver Data Vault 2.0
 |-- Raw Vault:
 | * 20 Hubs (dv2_hubs.sql)
 | * 32 Links (dv2_links.sql)
 | * 52 Satellites (dv2_satellites.sql) <-- 23 append-only per SEC 613
 | * 16 Reference Tables + 3 Reference Hubs (dv2_reference.sql)
 |-- Business Vault:
 | * 5 Computed Satellites + 2 Derived Links + 4 Bridges (dv2_business_vault.sql)
 |-- Query helpers:
 | * 16 PIT tables (dv2_pit_tables.sql)
 | * 8 Bridge tables (dv2_bridge_tables.sql)
 |
 v
Gold
```

**Total DV2 tables:** 20 + 32 + 52 + 19 + 11 + 16 + 8 = **158 tables**.

---

## 2. Hash Key Generation

All business keys are hashed with **SHA-256** for privacy, collision resistance, and downstream portability.

Standard pattern (Databricks SQL):

```sql
SHA2(CONCAT_WS('||', LOWER(TRIM(business_key)), LOWER(TRIM(record_source))), 256)
```

Normalization rules:

1. `LOWER(TRIM(...))` on every component to avoid case/whitespace drift.
2. `||` delimiter (never appears in business keys).
3. `NULL` values are coalesced to the literal string `'__NULL__'` to keep hashes deterministic.
4. For multi-column business keys, concatenate columns in the order listed in the Bronze CREATE TABLE primary key.

---

## 3. Hub Mapping

| Hub | Source Entity (Bronze) | Business Key Column | record_source | Business Key Source |
|------|-------------------------|---------------------|---------------|---------------------|
| hub_party | `party` | `party_id` | varies by feed | LEI preferred; else internal `party_id` |
| hub_account | `account` | `account_id` | `OMS_ACCOUNT_MASTER` | Account number (natural) |
| hub_desk | `desk` | `desk_id` | `ORG_MASTER` | Desk code |
| hub_agreement | `agreement` | `agreement_id` | `LEGAL_DOCS` | Agreement reference |
| hub_instrument | `instrument` | `instrument_id` | `SECURITY_MASTER` | ISIN > CUSIP > FIGI |
| hub_venue | `venue` | `venue_id` | `VENUE_REGISTRY` | ISO 10383 MIC |
| hub_order_request | `order_request` | `order_request_id` | `OMS_ORDER_REQUEST` | Request ID |
| hub_quote_request | `quote_request` | `quote_request_id` | `RFQ_PLATFORM` | RFQ ID |
| hub_quote | `quote_event` | `quote_id` | `QUOTE_ENGINE` | Market-maker quote ID |
| hub_rfe | `request_for_execution` | `rfe_id` | `OMS_RFE` | RFE ID |
| hub_ioi | `indication_of_interest` | `ioi_id` | `IOI_PLATFORM` | IOI ID |
| hub_order | `order_event` | `cat_order_id` | `OMS_ORDER` | **cat_order_id - SEC Rule 613 mandated (NOT a surrogate)** |
| hub_execution | `execution` | `execution_id` | `TRADE_CAPTURE` | Execution ID |
| hub_allocation | `allocation` | `allocation_id` | `ALLOCATION_ENGINE` | Allocation ID |
| hub_confirmation | `trade_confirmation` | `confirmation_id` | `CONFIRM_SYSTEM` | Confirmation ID |
| hub_clearing | `clearing_record` | `clearing_id` | `CCP_FEED` | Clearing ID |
| hub_settlement | `settlement_instruction` | `settlement_id` | `SETTLEMENT_ENGINE` | Settlement ID |
| hub_submission | `reporting_submission` | `submission_id` | `REG_REPORTING` | Submission ID |
| hub_repo | `repo_transaction` | `repo_id` | `REPO_BOOK` | Repo ID |
| hub_securities_loan | `securities_loan` | `loan_id` | `SLM_SYSTEM` | Loan ID |

---

## 4. Link Mapping (key subset)

| Link | Source FKs (Bronze) | Hubs Joined | Notes |
|-------|----------------------|-------------|-------|
| link_order_request_party_instrument | `order_request.requesting_party_role_id` + `.instrument_id` | hub_party + hub_instrument + hub_order_request | 3-way link |
| link_order_request_order | `order_request.order_event_id` | hub_order_request + hub_order | Systematization linkage |
| link_quote_response | `quote_response.quote_request_id +.quote_event_id` | hub_quote_request + hub_quote + hub_party | 3-way junction |
| link_execution_order | `execution.order_event_id` | hub_execution + hub_order | Has fill-progression satellite |
| link_allocation_execution | `allocation.execution_id` | hub_allocation + hub_execution | - |
| link_allocation_account | `allocation.allocated_account_id` | hub_allocation + hub_account | - |
| link_confirmation_allocation | `trade_confirmation.allocation_id` | hub_confirmation + hub_allocation | - |
| link_clearing_execution | `clearing_record.execution_id` | hub_clearing + hub_execution | - |
| link_settlement_allocation | `settlement_instruction.allocation_id` | hub_settlement + hub_allocation | - |
| link_party_hierarchy | `ownership_structure` | hub_party × hub_party | Self-referencing |
| link_party_agreement | `agreement.party_role_id +.counterparty_role_id` | hub_party × hub_party × hub_agreement | 3-way |
| link_netting_set | `agreement_netting_set` | hub_agreement × hub_party × hub_party | - |
| link_instrument_underlying | `underlying_reference` | hub_instrument × hub_instrument | Self-referencing |
| link_repo_parties | `repo_transaction` | hub_repo × hub_party × hub_party | 3-way |
| link_loan_parties | `securities_loan` | hub_securities_loan × hub_party × hub_party | 3-way |

(Full 32-link mapping in `dv2_links.sql`.)

---

## 5. Satellite Mapping

### 5.1 Non-append-only Satellites (SCD2)

| Satellite | Parent Hub | Source Columns (Bronze) |
|-----------|-----------|-------------------------|
| sat_party_details | hub_party | `party.*` (name, type, jurisdiction, LEI status) + `legal_entity.*` + `natural_person.*` |
| sat_party_classification | hub_party | `party_kyc.*` + AML risk flags |
| sat_party_contact | hub_party | `party_address.*` + `party_contact.*` |
| sat_account_details | hub_account | `account.*` |
| sat_account_status | hub_account | `account.account_status` over time |
| sat_desk_details | hub_desk | `desk.*` |
| sat_agreement_details | hub_agreement | `agreement.*` header |
| sat_agreement_terms | hub_agreement | `collateral_agreement.*` + `agreement_schedule.*` |
| sat_agreement_status | hub_agreement | `agreement.agreement_status` |
| sat_instrument_details | hub_instrument | `instrument.*` |
| sat_instrument_classification | hub_instrument | `instrument_classification.*` |
| sat_instrument_status | hub_instrument | `instrument.*` status cols |
| sat_instrument_derivative_terms | hub_instrument | `option_contract_details` / `future_contract_details` / `swap_contract_details` |
| sat_venue_details | hub_venue | `venue.*` |
| sat_venue_connectivity | hub_venue | `venue_connectivity.*` + `venue_trading_session.*` |
| sat_ioi_details / sat_ioi_status | hub_ioi | `indication_of_interest.*` |
| sat_repo_details | hub_repo | `repo_transaction.*` |
| sat_securities_loan_details | hub_securities_loan | `securities_loan.*` |
| Link satellites (8) | respective Links | descriptive attrs from Bronze junction rows |

### 5.2 Append-only Satellites (SEC Rule 613 - 23 total)

| Satellite | Parent Hub | CAT Event Type(s) |
|-----------|-----------|--------------------|
| sat_order_request_details | hub_order_request | MEIR, MOIR |
| sat_order_request_status | hub_order_request | (status over time) |
| sat_quote_request_details | hub_quote_request | MEQR |
| sat_quote_details | hub_quote | MEQS |
| sat_rfe_details | hub_rfe | (RFE event) |
| sat_order_details | hub_order | MENO, MONO, MLNO, MECO |
| sat_order_routing | hub_order | MEOR, MEIR, MLOR |
| sat_order_status | hub_order | (status events) |
| sat_order_modification | hub_order | MEOM, MEIM, MLIM, MEOMR |
| sat_execution_details | hub_execution | MEOT, MEOTQ, MOOT, MLOT, EOT |
| sat_execution_classification | hub_execution | (ExecType/Capacity/Liq) |
| sat_execution_reversal | hub_execution | (BUST/CORRECT) |
| sat_allocation_details | hub_allocation | MEAA |
| sat_allocation_status | hub_allocation | - |
| sat_confirmation_details | hub_confirmation | - |
| sat_confirmation_status | hub_confirmation | - |
| sat_clearing_details | hub_clearing | - |
| sat_clearing_status | hub_clearing | - |
| sat_settlement_details | hub_settlement | - |
| sat_settlement_status | hub_settlement | - |
| sat_submission_details | hub_submission | - |
| sat_submission_status | hub_submission | - |
| sat_link_execution_order_fill_attrs | link_execution_order | (fill progression) |
| sat_link_allocation_account_allocation_pct | link_allocation_account | (allocation pct) |

> **Append-only** means: every insert creates a new row; no UPDATE ever rewrites an existing row. The `load_end_date` column is omitted on these satellites. Query with `MAX(load_date) OVER (PARTITION BY parent_hk)` for "current" state.

---

## 6. Reference Table Mapping

Each Bronze `ref_{name}` table maps 1:1 to a Silver `ref_{name}_dv` with three added columns:

| Added Column | Type | Purpose |
|--------------|------|---------|
| `load_date` | TIMESTAMP | Pipeline load timestamp |
| `hash_diff` | STRING | SHA-256 of all descriptive cols - change detection |
| `record_source` | STRING | Source system lineage |

16 reference tables: `ref_cat_event_type_dv`, `ref_mic_dv`, `ref_country_dv`, `ref_currency_dv`, `ref_cfi_category_dv`, `ref_entity_legal_form_dv`, `ref_order_type_dv`, `ref_time_in_force_dv`, `ref_handling_instruction_dv`, `ref_party_role_dv`, `ref_instrument_type_dv`, `ref_day_count_dv`, `ref_settlement_venue_dv`, `ref_regulator_dv`, `ref_credit_rating_scale_dv`, `ref_tax_jurisdiction_dv`.

Plus 3 Reference Hubs (`hub_mic`, `hub_currency`, `hub_country`) for fully DV-compliant joins when satellites are needed.

---

## 7. PIT Table Build Logic

Each PIT is rebuilt daily from its parent Hub + Satellites via this pattern (pseudo-SQL):

```sql
INSERT INTO pit_<hub> (<hub>_hk, snapshot_date, sat_*_load_date, record_source, _pit_load_ts)
SELECT
 h.<hub>_hk,
 DATE'<today>' AS snapshot_date,
 (SELECT MAX(load_date) FROM sat_<hub>_details s
 WHERE s.<hub>_hk = h.<hub>_hk
 AND s.load_date <= TIMESTAMP'<today EOD>') AS sat_<hub>_details_load_date,
 --... one CTE per satellite...
 h.record_source,
 current_timestamp AS _pit_load_ts
FROM hub_<hub> h;
```

DLT implementation uses `@dlt.table` with `MERGE` on `(<hub>_hk, snapshot_date)` - .

---

## 8. Bridge Build Logic

Bridges are built from Links + relevant Satellites joined into one flat row.

Example - `bridge_trade_lifecycle` build pseudo-SQL:

```sql
WITH ord AS (SELECT * FROM hub_order)
SELECT
 SHA2(CONCAT_WS('||',
 COALESCE(ord.order_hk,'__N__'),
 COALESCE(exe.execution_hk,'__N__'),
 COALESCE(alloc.allocation_hk,'__N__'),
 COALESCE(settle.settlement_hk,'__N__'),
 party.party_hk, inst.instrument_hk), 256) AS bridge_hk,
 ord.order_hk, exe.execution_hk, alloc.allocation_hk,...
FROM ord
LEFT JOIN link_execution_order leo ON leo.order_hk = ord.order_hk
LEFT JOIN hub_execution exe...
LEFT JOIN link_allocation_execution lae...
-- etc.
WHERE ord.trade_date = DATE'<today>';
```

---

## 9. Record Source Taxonomy

`record_source` is the lineage breadcrumb. Allowed values (extensible):

| Category | Values |
|----------|--------|
| OMS | `OMS_ORDER`, `OMS_ORDER_REQUEST`, `OMS_RFE` |
| EMS | `EMS_ROUTE`, `EMS_ALGO` |
| Trade Capture | `TRADE_CAPTURE`, `TRADE_BOOKING` |
| Post-Trade | `ALLOCATION_ENGINE`, `CONFIRM_SYSTEM` |
| Clearing | `CCP_OCC`, `CCP_NSCC`, `CCP_FICC`, `CCP_LCH` |
| Settlement | `DTCC_SETTLE`, `EUROCLEAR`, `CLEARSTREAM`, `FED` |
| Master Data | `SECURITY_MASTER`, `VENUE_REGISTRY`, `ORG_MASTER`, `ACCOUNT_MASTER` |
| KYC / AML | `KYC_SYSTEM`, `SANCTIONS_FEED` |
| Legal | `LEGAL_DOCS`, `CSA_ENGINE` |
| Reg Reporting | `REG_REPORTING`, `CAT_SUBMITTER`, `TRACE_SUBMITTER` |
| External feeds | `ISO_10383_MIC`, `ISO_4217_FX`, `GLEIF_LEI`, `FIGI_FEED` |
| Derived | `BUSINESS_VAULT` |

---

## 10. Load Order Dependencies

Within a single DLT pipeline run, tables must load in this order:

```
1. Reference tables (dv2_reference.sql) - no dependencies
2. Reference Hubs (hub_mic, hub_currency, hub_country)
3. Business Hubs (all 20 in dv2_hubs.sql) - independent of each other
4. Business Satellites - depend on their parent Hub
5. Links - depend on all participating Hubs
6. Link Satellites - depend on their parent Link
7. PIT tables - depend on Hub + all its Satellites
8. Bridges - depend on Hubs + Links + relevant Satellites
9. Business Vault - depends on full Raw Vault being loaded
10. Gold star schema - depends on Business Vault + Bridges
```

---

## 11. Data Quality Gates (implemented in

| Gate | Applied To | Rule |
|------|-----------|------|
| Hub business key non-null | all hubs | `<entity>_bk IS NOT NULL` |
| Hub hash key correctness | all hubs | `<entity>_hk = SHA2(...)` |
| Link hub FK existence | all links | `EXISTS (hub_x.<x>_hk)` for each component |
| Satellite parent existence | all satellites | `EXISTS (hub_y.<y>_hk)` |
| Append-only immutability | 23 sats | `NO UPDATE` on these tables - enforced by Unity Catalog grant policy |
| State-machine validity | status sats | transitions must match `state_machines.md` |
| Clock-sync compliance | order_request, order_event, execution sats | request->event delta ≤ 50 ms (electronic) or ≤ 1 s (manual) |
| Enumeration membership | all code columns | value IN (SELECT code FROM ref_<topic>_dv WHERE is_active) |
| PIT snapshot completeness | all PITs | every active Hub row has a snapshot_date row |
| Bridge FK integrity | all bridges | each hub_hk in bridge exists in corresponding hub |

---

## 12. Retention & Compliance

| Policy | Scope | Duration | Reference |
|--------|-------|----------|-----------|
| SEC 17a-4 | All order/execution audit tables | 6 years | SEC Rule 17a-4 |
| FINRA CAT | Order/route/exec/alloc | 6 years | FINRA CAT NMS Plan |
| BCBS 239 | Risk data aggregation | 7 years | BCBS 239 |
| MiFID II | EU trading records | 5 years | MiFID II Art. 16 |
| CFTC | Swaps / derivatives | 5 years | CFTC Part 45 |
| GDPR PII | Natural person satellites | Purpose-limited; right-to-erasure | GDPR Art. 17 |

Implementation:

* Delta retention via `delta.logRetentionDuration = interval 7 years`.
* VACUUM only after retention window + 60-day regulatory hold buffer.
* Unity Catalog Column-Masking for PII satellites per GDPR.

---

