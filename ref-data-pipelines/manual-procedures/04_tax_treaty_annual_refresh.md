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

# Runbook 04 - Tax Jurisdiction + Treaty Rate Annual Refresh

| Attribute | Value |
|-------------------|--------------------------------------------------------------------|
| Cadence | Annual, first business day after the OECD treaty-rate publication |
| Owner team | Tax Operations |
| Escalation | Chief Tax Officer |
| Target tables | `ref_tax_jurisdiction` |
| DLT pipeline | `ref_ingest_adhoc` |
| Source authority | OECD / IRS / local national tax authorities |
| Expected duration | 5 business days (compile + legal review + load) |

## 1. Purpose

Refresh the annual tax jurisdiction table that drives cross-border withholding calculations on dividends, interest, royalties, and swap payments. Inputs include the OECD Model Tax Convention update, IRS Publication 515 (Withholding Rates), and bilateral treaty amendments published by each participating jurisdiction.

The table feeds the tax-reclaim engine, FATCA/CRS reporting obligations, and per-trade withholding tax calculation on clearing and settlement events.

## 2. Pre-Checks

- OECD Multilateral Instrument (MLI) tracker refreshed for the year
- IRS Publication 515 is the current revision (typically published in Jan for the preceding calendar year)
- Legal + Tax Counsel has reviewed any treaty amendments effective since last refresh
- Enterprise licenses current for:
 - OECD iLibrary
 - IBFD Research Platform
 - Deloitte International Tax Highlights
- Azure Key Vault: `kv-cat-refdata-prod/ibfd-api-token` and `kv-cat-refdata-prod/deloitte-itx-token` unexpired
- Tax Operations has compiled the annual treaty-change log to `/opt/cat/tax/treaty_changelog_${YEAR}.xlsx`

## 3. Source Compilation

Tax jurisdiction data is not downloadable from a single canonical feed. Tax Operations compiles the annual refresh from five authoritative inputs and produces a single harmonized CSV.

### 3.1 Download inputs

```bash
export YEAR=2026
export STAGING=/mnt/refdata/staging/tax/${YEAR}

mkdir -p ${STAGING}/{oecd,irs,ibfd,deloitte,fatca,harmonized}

# OECD MLI positions
curl -f -o ${STAGING}/oecd/mli_positions_${YEAR}.xlsx \
 "https://www.oecd.org/content/dam/oecd/en/topics/policy-sub-issues/bepsmli/mli-matching-database.xlsx"

# IRS Publication 515 (withholding of tax on nonresident aliens and foreign entities)
curl -f -o ${STAGING}/irs/pub515_${YEAR}.pdf \
 "https://www.irs.gov/pub/irs-pdf/p515.pdf"

# IBFD treaty withholding rates dataset
curl -f -H "Authorization: Bearer ${IBFD_TOKEN}" \
 -o ${STAGING}/ibfd/ibfd_treaty_rates_${YEAR}.csv \
 "https://api.ibfd.org/v1/treaty-rates?year=${YEAR}"

# Deloitte International Tax Highlights 2026
curl -f -H "Authorization: Bearer ${DELOITTE_TOKEN}" \
 -o ${STAGING}/deloitte/itx_highlights_${YEAR}.csv \
 "https://api.deloitte.com/tax/highlights?year=${YEAR}"

# FATCA participating jurisdictions list (Treasury)
curl -f -o ${STAGING}/fatca/fatca_participating_${YEAR}.csv \
 "https://www.treasury.gov/resource-center/tax-policy/treaties/Documents/FATCA-Participating.csv"
```

### 3.2 Harmonize

```bash
python /opt/cat/tax/harmonize_tax_jurisdictions.py \
 --oecd-mli ${STAGING}/oecd/mli_positions_${YEAR}.xlsx \
 --irs-p515 ${STAGING}/irs/pub515_${YEAR}.pdf \
 --ibfd ${STAGING}/ibfd/ibfd_treaty_rates_${YEAR}.csv \
 --deloitte ${STAGING}/deloitte/itx_highlights_${YEAR}.csv \
 --fatca ${STAGING}/fatca/fatca_participating_${YEAR}.csv \
 --changelog /opt/cat/tax/treaty_changelog_${YEAR}.xlsx \
 --output ${STAGING}/harmonized/tax_jurisdictions.csv \
 --year ${YEAR}
```

The harmonization script produces a CSV matching the `ref_tax_jurisdiction` schema:

`tax_jurisdiction_code, tax_jurisdiction_name, category, country_code, subdivision_code, tax_authority, default_withholding_rate, treaty_withholding_rate, fatca_participant, crs_participant, is_treaty_jurisdiction, effective_start_date, effective_end_date, source_version`

## 4. Legal and Tax Counsel Review

Before upload, the Tax Legal team must confirm:

- All new treaty-amendment effective dates are captured
- Retroactive treaty provisions applied only where legally binding as of the effective date
- Withholding-rate changes consistent with the IRS Pub 515 revision
- FATCA participating list matches the Treasury publication
- CRS participating list matches the OECD automatic-exchange agreements

Sign-off is evidenced by the Chief Tax Officer's countersignature on the Annual Tax Refresh memo.

## 5. Upload to OneLake

```bash
az storage fs file upload \
 --source ${STAGING}/harmonized/tax_jurisdictions.csv \
 --path "ref/seeds/tax_jurisdictions/tax_jurisdictions_${YEAR}.csv" \
 --account-name onelake --file-system cat-prod
```

## 6. DLT Pipeline Trigger

```bash
databricks pipelines start \
 --pipeline-id $(cat /opt/cat/config/prod/ref_ingest_adhoc.id) \
 --full-refresh false
```

Monitor `ingestion/dlt_ref_industry.py` for the `ref_tax_jurisdiction` target. Confirm:

- 100% of rows pass `country_code_iso3166` expectation (`^[A-Z]{2}$`)
- 100% of rows pass `default_withhold_range BETWEEN 0 AND 1`
- 100% of rows pass `treaty_withhold_range BETWEEN 0 AND 1`
- 100% of rows pass `category_enum IN ('FEDERAL','STATE','PROVINCIAL','MUNICIPAL','SUPRANATIONAL')`

## 7. Post-Load Validation

```sql
-- Confirm at least one row for every ISO country held in ref_country
SELECT c.country_code
FROM gold.ref_country c
LEFT JOIN gold.ref_tax_jurisdiction t
 ON c.country_code = t.country_code
 AND t.is_active = TRUE
 AND t.category = 'FEDERAL'
WHERE c.is_active = TRUE
 AND t.tax_jurisdiction_code IS NULL;
-- Expected: only countries the firm does not trade in

-- Confirm treaty rate <= default rate for every pair
SELECT tax_jurisdiction_code,
 default_withholding_rate, treaty_withholding_rate
FROM gold.ref_tax_jurisdiction
WHERE is_active = TRUE
 AND treaty_withholding_rate IS NOT NULL
 AND default_withholding_rate IS NOT NULL
 AND treaty_withholding_rate > default_withholding_rate;
-- Expected: 0 rows

-- Confirm FATCA + CRS participation set is monotonic vs prior year
SELECT
 SUM(CASE WHEN fatca_participant THEN 1 ELSE 0 END) AS fatca_count,
 SUM(CASE WHEN crs_participant THEN 1 ELSE 0 END) AS crs_count
FROM gold.ref_tax_jurisdiction
WHERE is_active = TRUE;
-- Expected: >= prior year counts unless a jurisdiction formally withdrew

-- SCD2 closure check
SELECT tax_jurisdiction_code, COUNT(*) AS open_rows
FROM gold.ref_tax_jurisdiction
WHERE is_active = TRUE
GROUP BY tax_jurisdiction_code
HAVING COUNT(*) > 1;
-- Expected: 0 rows
```

## 8. Downstream Recomputation

Trigger the tax-reclaim recalculation job for in-flight settlement events that straddle the refresh effective date:

```bash
databricks jobs run-now --job-id $(cat /opt/cat/config/prod/tax_reclaim_recalc.id)
```

Tax Operations validates that the delta matches the expected annual impact projection before allowing affected entries to settle.

## 9. Regulatory Reporting Handoff

Notify:

- FATCA reporting team - any change in participating list
- CRS reporting team - any change in participating list
- Corporate Actions Operations - any change to dividend withholding defaults
- Fund Admin - any change affecting fund-level withholding tables

## 10. Rollback Procedure

If legal review withdraws sign-off or post-load validation fails:

1. Stop downstream tax recalculation job immediately
2. Restore the prior version:
 ```sql
 RESTORE TABLE gold.ref_tax_jurisdiction TO VERSION AS OF <N-1>;
 ```
3. Archive the failed staging file:
 ```bash
 az storage fs directory move \
 --from "ref/seeds/tax_jurisdictions" \
 --to "ref/seeds/_failed/${YEAR}/tax_jurisdictions" \
 --account-name onelake --file-system cat-prod
 ```
4. Reverse any pending tax-reclaim entries that referenced the new rates
5. Open a P2 incident; root-cause review within 72 hours

## 11. Sign-Off

| Role | Required |
|-----------------------------------|---------------------------|
| Tax Operations Lead | Harmonization + load |
| Tax Legal Counsel | Treaty amendment review |
| Chief Tax Officer (or delegate) | Annual approval |
| Chief Compliance Officer | FATCA/CRS participation |
| Fund Admin Lead | Fund-level impact |

Archive the runbook output, harmonization diff, legal sign-off memo, and validation SQL output to `ref/audit/${YEAR}_runbook_04.pdf`.
