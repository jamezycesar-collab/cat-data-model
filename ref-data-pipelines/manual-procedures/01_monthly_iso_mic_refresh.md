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

# Runbook 01 - Monthly ISO MIC + Country + Currency Refresh

| Attribute | Value |
|-------------------|---------------------------------------------------------------|
| Cadence | Monthly, first business day by 09:00 UTC |
| Owner team | Reference Data Operations |
| Escalation | Enterprise Data Governance |
| Target tables | `ref_mic`, `ref_country`, `ref_currency` |
| DLT pipeline | `ref_ingest_monthly` |
| Source authority | ISO 20022 Registration Authority (SWIFT) |
| Expected duration | 25 minutes end-to-end |

## 1. Purpose

Refresh the three ISO reference tables from the official monthly publications. These tables anchor MIC-based venue identification (`ref_mic`), ISO 3166-1 country resolution (`ref_country`), and ISO 4217 currency resolution (`ref_currency`) for every downstream fact and dimension.

## 2. Pre-Checks

Before beginning the refresh, confirm the following in order:

- The previous monthly run ended `SUCCEEDED` in the DLT pipeline event log
- No open P1/P2 incidents against the `ref_*` domain
- OneLake storage `abfss://onelake@cat-prod.dfs.fabric.microsoft.com/ref/` is reachable from the Fabric workspace
- The ingestion seed folders exist and are empty (prior drop was archived):
 - `ref/seeds/mic/`
 - `ref/seeds/country/`
 - `ref/seeds/currency/`
- Azure Key Vault secret `kv-cat-refdata-prod/iso-proxy-token` is current and unexpired

## 3. Source Download Procedure

Authoritative download URLs, filenames, and parse formats:

| Table | Source URL | Filename | Format |
|----------------|----------------------------------------------------------------------------------|--------------------|---------------|
| `ref_mic` | https://www.iso20022.org/market-identifier-codes | ISO10383_MIC.csv | CSV |
| `ref_country` | https://www.iso.org/iso-3166-country-codes.html | ISO3166-1.csv | CSV |
| `ref_currency` | https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml | ISO4217_current.xml | XML |

Execute the following from the reference-data staging VM:

```bash
export REF_DATE=$(date -u +%Y%m%d)
export STAGING=/mnt/refdata/staging/${REF_DATE}

mkdir -p ${STAGING}/mic ${STAGING}/country ${STAGING}/currency

# 1. ISO MIC (ISO 10383)
curl -f -o ${STAGING}/mic/ISO10383_MIC.csv \
 "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"

# 2. ISO 3166-1 country codes
curl -f -o ${STAGING}/country/ISO3166-1.csv \
 "https://www.iso.org/obp/ui/static/files/iso3166-1.csv"

# 3. ISO 4217 currency (SIX Group XML publication)
curl -f -o ${STAGING}/currency/ISO4217_current.xml \
 "https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml"
```

Verify download integrity:

```bash
wc -l ${STAGING}/mic/ISO10383_MIC.csv # expect >= 2200 rows
wc -l ${STAGING}/country/ISO3166-1.csv # expect 249 rows + header
xmllint --noout ${STAGING}/currency/ISO4217_current.xml
```

If any file fails to download or parse, stop and raise a P3 incident against the Reference Data Operations team.

## 4. Staging into OneLake

Upload the three files to OneLake under their canonical Auto Loader landing folders:

```bash
az storage fs file upload \
 --source ${STAGING}/mic/ISO10383_MIC.csv \
 --path "ref/seeds/mic/ISO10383_MIC_${REF_DATE}.csv" \
 --account-name onelake --file-system cat-prod

az storage fs file upload \
 --source ${STAGING}/country/ISO3166-1.csv \
 --path "ref/seeds/country/ISO3166-1_${REF_DATE}.csv" \
 --account-name onelake --file-system cat-prod

az storage fs file upload \
 --source ${STAGING}/currency/ISO4217_current.xml \
 --path "ref/seeds/currency/ISO4217_${REF_DATE}.xml" \
 --account-name onelake --file-system cat-prod
```

Record the upload audit log to `ref/audit/${REF_DATE}_upload.json` with byte counts, SHA-256 hashes, and operator ID.

## 5. DLT Pipeline Trigger

From the Fabric workspace, start the `ref_ingest_monthly` pipeline:

```bash
databricks pipelines start \
 --pipeline-id $(cat /opt/cat/config/ref_ingest_monthly.id) \
 --full-refresh false
```

The pipeline runs the following notebooks in sequence:

1. `ingestion/dlt_ref_iso_codes.py` - Auto Loader picks up new files, validates, applies SCD2
2. `ingestion/dlt_ref_industry.py` - downstream settlement venues that FK to `ref_mic`

Monitor via the DLT event log. Expect:

- 100% of rows to pass `@dlt.expect_all_or_fail` gates
- Zero rejected rows
- Stream progress: 1 microbatch per table, then idle

## 6. Post-Load Validation

Execute these SQL checks from a serverless SQL warehouse. Each must return `PASS`:

```sql
-- Row count check
SELECT
 CASE WHEN COUNT(*) BETWEEN 2200 AND 3000 THEN 'PASS' ELSE 'FAIL' END AS mic_count_check
FROM gold.ref_mic WHERE is_active = TRUE;

SELECT
 CASE WHEN COUNT(*) BETWEEN 240 AND 260 THEN 'PASS' ELSE 'FAIL' END AS country_count_check
FROM gold.ref_country WHERE is_active = TRUE;

SELECT
 CASE WHEN COUNT(*) BETWEEN 170 AND 185 THEN 'PASS' ELSE 'FAIL' END AS ccy_count_check
FROM gold.ref_currency WHERE is_active = TRUE;

-- Freshness check
SELECT
 CASE WHEN MAX(last_updated_date) >= current_date - 7 THEN 'PASS' ELSE 'FAIL' END AS freshness_check
FROM gold.ref_mic;

-- SCD2 integrity
SELECT
 CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS scd2_overlap_check
FROM (
 SELECT mic_code, COUNT(*) AS open_rows
 FROM gold.ref_mic
 WHERE is_active = TRUE
 GROUP BY mic_code
 HAVING COUNT(*) > 1
);
```

Additional smoke check - sample three known MICs and confirm they resolve:

```sql
SELECT mic_code, mic_name, country_code, is_active
FROM gold.ref_mic
WHERE mic_code IN ('XNYS', 'XLON', 'XTKS') AND is_active = TRUE;
```

## 7. Downstream Notifications

If all checks pass, notify:

- `#ref-data-ops` Slack channel with the run manifest link
- Trade Operations leads via email alias `trade-ops-leads@firm.example.com` if any MIC was added or retired

## 8. Rollback Procedure

If any validation check returns `FAIL`, execute the rollback:

1. Stop the pipeline: `databricks pipelines stop --pipeline-id...`
2. Identify the last good batch:
 ```sql
 DESCRIBE HISTORY gold.ref_mic LIMIT 10;
 ```
3. Restore the prior version:
 ```sql
 RESTORE TABLE gold.ref_mic TO VERSION AS OF <N-1>;
 RESTORE TABLE gold.ref_country TO VERSION AS OF <N-1>;
 RESTORE TABLE gold.ref_currency TO VERSION AS OF <N-1>;
 ```
4. Archive the failed staging files:
 ```bash
 az storage fs directory move \
 --from "ref/seeds/mic" --to "ref/seeds/_failed/${REF_DATE}/mic" \
 --account-name onelake --file-system cat-prod
 ```
5. Open a P2 incident, attach the DLT event log, and schedule a remediation window.

## 9. Sign-Off

| Role | Required |
|------------------------------|------------------|
| Ref Data Operator | Confirms run |
| Ref Data Lead | Reviews sign-off |
| Enterprise Data Governance | Monthly audit |

Archive the completed runbook output (console log, validation SQL output, Slack confirmation) to `ref/audit/${REF_DATE}_runbook_01.pdf`.
