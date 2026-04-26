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

# Reference Data Ingestion

Ingestion pipelines for the **16 `ref_*` tables** that enforce 72 enumerations
across the enterprise CAT trade lifecycle model.

```
ref-data-pipelines/
├── README.md (this file)
├── configs/
│ ├── pipeline_config.json DLT pipeline definitions
│ ├── source_registry.json Per-table source authority + refresh cadence
│ └── validation_rules.json Per-table DQ thresholds
├── ingestion/ 6 DLT pipelines grouped by source authority
│ ├── dlt_ref_iso_codes.py ISO 3166, 4217, 10383, 10962, 20275
│ ├── dlt_ref_cat_taxonomy.py FINRA CAT Tech Specs v4.1.0r9 (50 events)
│ ├── dlt_ref_fix_enums.py FIX tags 21, 40, 59
│ ├── dlt_ref_isda_conventions.py ISDA 2006/2021 day-count
│ ├── dlt_ref_industry.py CSDs, regulators, rating scales, tax
│ └── dlt_ref_firm_taxonomy.py ref_party_role, ref_instrument_type
└── manual-procedures/ 4 runbooks for periodic refresh
 ├── 01_monthly_iso_mic_refresh.md
 ├── 02_cat_taxonomy_version_upgrade.md
 ├── 03_credit_rating_quarterly_refresh.md
 └── 04_tax_treaty_annual_refresh.md
```

## Target tables (16)

| Table | Rows | Source Authority | Cadence | Pipeline |
|---|---:|---|---|---|
| `ref_cat_event_type` | 50 | FINRA CAT Tech Specs v4.1.0r9 | Per-release | `dlt_ref_cat_taxonomy.py` |
| `ref_mic` | ~3,000 | ISO 10383 | Monthly (1st business day) | `dlt_ref_iso_codes.py` |
| `ref_country` | ~250 | ISO 3166-1 | Quarterly | `dlt_ref_iso_codes.py` |
| `ref_currency` | ~180 | ISO 4217 | Quarterly | `dlt_ref_iso_codes.py` |
| `ref_cfi_category` | ~1,000 | ISO 10962:2019 | Annual | `dlt_ref_iso_codes.py` |
| `ref_entity_legal_form` | ~3,000 | ISO 20275 / GLEIF | Monthly | `dlt_ref_iso_codes.py` |
| `ref_order_type` | 25 | FIX 4.4 tag 40 | Per-release | `dlt_ref_fix_enums.py` |
| `ref_time_in_force` | 8 | FIX 4.4 tag 59 | Per-release | `dlt_ref_fix_enums.py` |
| `ref_handling_instruction` | 3 | FIX 4.4 tag 21 | Per-release | `dlt_ref_fix_enums.py` |
| `ref_day_count` | 15 | ISDA 2006/2021 | Per-release | `dlt_ref_isda_conventions.py` |
| `ref_settlement_venue` | 18 | Firm + industry | Semi-annual | `dlt_ref_industry.py` |
| `ref_regulator` | 25 | Firm | Semi-annual | `dlt_ref_industry.py` |
| `ref_credit_rating_scale` | ~100 | S&P, Moody's, Fitch, DBRS | Quarterly | `dlt_ref_industry.py` |
| `ref_tax_jurisdiction` | ~250 | OECD + IRS + local | Annual | `dlt_ref_industry.py` |
| `ref_party_role` | 20 | Firm taxonomy | Per-release | `dlt_ref_firm_taxonomy.py` |
| `ref_instrument_type` | 30 | Firm taxonomy | Per-release | `dlt_ref_firm_taxonomy.py` |

## Load pattern - SCD2 via MERGE

All ref tables are **low-velocity, high-authority**. Standard pattern:

```text
Bronze (raw feed)
 └─▶ DLT @expect_all_or_fail gates (code format, category enum, date sanity)
 └─▶ Silver stage (ref_{name}_stage) - one row per source code
 └─▶ MERGE into ref_{name} - close old effective window,
 insert new row when content changes
 └─▶ ref_{name} (Gold) - SCD2 published reference
```

The `(code, effective_start_date)` PK means a code can appear multiple times;
`is_active = TRUE` + `effective_end_date = 9999-12-31` marks the current version.
Downstream entities (order_event, instrument, party, etc.) FK onto
`(code,...)` and join point-in-time using
`trade_date BETWEEN effective_start_date AND effective_end_date`.

## Quality gates

Every pipeline enforces (as `@dlt.expect_all_or_fail`):

- `{name}_code IS NOT NULL`
- `{name}_name IS NOT NULL`
- `effective_start_date IS NOT NULL`
- `effective_end_date >= effective_start_date`
- `source_authority IS NOT NULL`
- `record_source IS NOT NULL`

Per-table rules (from `configs/validation_rules.json`):

- `ref_country.country_code` - exactly 2 chars, upper-case alpha
- `ref_currency.currency_code` - exactly 3 chars, upper-case alpha
- `ref_mic.mic_code` - exactly 4 chars, upper-case alphanumeric
- `ref_cat_event_type.cat_event_type_code` - matches `^[A-Z]{3,10}$`
- `ref_credit_rating_scale.rating_agency` - `IN ('SP','MOODYS','FITCH','DBRS','JCR','R_AND_I','SCOPE','KROLL')`

## Orchestration

Two Databricks DLT pipelines defined in `configs/pipeline_config.json`:

| Pipeline | Schedule | Purpose |
|---|---|---|
| `ref_ingest_monthly` | Cron `0 0 6 1 * ?` (1st of month, 06:00 ET) | ISO refresh + GLEIF |
| `ref_ingest_adhoc` | Manual trigger | CAT/FIX/ISDA version releases, rating-agency refresh |

All pipelines target `gold` database (same as dims/facts) to keep ref
tables visible to downstream joins without cross-catalog overhead.

## Lineage

Every row lands with:
- `record_source` = canonical source URL or system identifier
 (e.g. `https://www.iso20022.org/market-identifier-codes`)
- `source_authority` = normative body (`ISO`, `FINRA_CAT`, `FIX_PROTOCOL`, `ISDA`, `INTERNAL_TAXONOMY`)
- `source_version` = feed version pinned in the pipeline run
- `load_date` = `current_timestamp` at DLT materialisation

These four columns satisfy BCBS 239 principle 6 (accuracy/integrity) and
DCAM 3.3 (reference-data stewardship).

## Runbooks

See `manual-procedures/` for the four periodic refresh runbooks. Each runbook
includes: pre-checks, source download procedure, staging commands, DLT trigger,
post-load validation, rollback steps.
