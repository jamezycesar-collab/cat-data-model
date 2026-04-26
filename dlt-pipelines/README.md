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

# Silver to Gold DLT Pipelines

Delta Live Tables (DLT) pipelines that transform the Silver Data Vault 2.0
layer into the Gold star schema documented in `output/gold/`.

## Contents

```
dlt-pipelines/
├── README.md (this file)
├── config/
│ └── pipeline_config.json DLT pipeline settings (clusters, storage, triggers)
├── dim_pipelines/ Dimension pipelines (SCD2 merge + static seeds)
│ ├── dlt_dim_date.py
│ ├── dlt_dim_event_type.py
│ ├── dlt_dim_party.py
│ ├── dlt_dim_instrument.py
│ ├── dlt_dim_venue.py
│ ├── dlt_dim_account.py
│ ├── dlt_dim_desk.py
│ └── dlt_dim_trader.py
├── fact_pipelines/ Event-grain facts (append-only merge)
│ ├── dlt_fact_cat_order_events.py
│ ├── dlt_fact_cat_allocations.py
│ ├── dlt_fact_cat_quotes.py
│ └── dlt_fact_cat_customer_records.py
└── tests/ PyTest / chispa unit + integrity suites
 ├── test_dim_scd2_invariants.py
 ├── test_fact_quality_gates.py
 └── test_referential_integrity.py
```

## Execution Topology

```
┌────────────────────────┐ ┌────────────────────────┐
│ Silver (Data Vault 2.0)│ │ Gold (Star Schema) │
│ hubs / links / sats │──DLT──▶│ dim_* / fact_cat_* │
│ pit / bridge / BV │ │ cat_submission_batch │
└────────────────────────┘ │ cat_feedback_error │
 └────────────────────────┘
 │
 ▼
 ┌────────────────────────┐
 │ 07_submission_generator│
 │ (BZip2 JSON -> SFTP) │
 └────────────────────────┘
```

Two DLT pipelines are defined:

| Pipeline | Purpose | Trigger | Edition | Channel |
|----------|---------|---------|---------|---------|
| `gold_dim_pipeline` | SCD2 merge of 6 dims + seed of 2 static dims | Nightly 02:00 ET | Advanced | Current |
| `gold_fact_pipeline` | Append-only merge of 4 facts | Every 15 min intraday | Advanced | Current |

`Advanced` edition is required for expectations (`@dlt.expect_all_or_fail`),
change data capture (`dlt.apply_changes`), and CDF consumption.

## Quality Gates

Each fact pipeline enforces the following expectations inline:

- `firm_roe_id IS NOT NULL`
- `dv2_source_hk IS NOT NULL` (BCBS 239 lineage)
- `cat_event_code RLIKE '^[A-Z]{3,10}$'` (CAT taxonomy)
- `cat_event_code IN (seeded event list)` (referential to `dim_event_type`)
- `event_date IS NOT NULL`

Each dim pipeline enforces:

- `row_hash LENGTH = 64` (SHA-256 hex digest)
- `effective_start_date <= effective_end_date`
- Exactly one `is_current = TRUE` per business key

Violations set `on_violation='drop'` for soft expectations or `'fail'` for
hard regulatory gates (see individual files).

## Lineage

Every Gold row carries `dv2_source_hk` - the concatenated SHA-256 hash key
of the originating Silver hub/link. The DLT graph renders the full lineage
tree; `DESCRIBE HISTORY` on any Gold table shows which Silver commit version
fed it.

## Deploy

```bash
databricks bundle deploy --profile cat-prod -t prod
# or create manually from the Databricks UI -> Workflows -> Delta Live Tables
```

See `config/pipeline_config.json` for the pipeline JSON definitions.
