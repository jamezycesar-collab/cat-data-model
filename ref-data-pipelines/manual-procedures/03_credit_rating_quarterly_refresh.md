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

# Runbook 03 - Credit Rating Scale Quarterly Refresh

| Attribute | Value |
|-------------------|----------------------------------------------------------------|
| Cadence | Quarterly, last business day of Mar / Jun / Sep / Dec |
| Owner team | Credit Risk Data Operations |
| Escalation | Chief Credit Officer |
| Target tables | `ref_credit_rating_scale` |
| DLT pipeline | `ref_ingest_adhoc` |
| Source authority | S&P Global / Moody's / Fitch / DBRS Morningstar composite |
| Expected duration | 3 business days (vendor download + harmonization + load) |

## 1. Purpose

Refresh the composite credit rating scale table which harmonizes S&P, Moody's, Fitch, and DBRS long-term and short-term rating scales into a single cross-walk with numeric equivalents, Basel III risk weights, default probabilities, and investment-grade flags.

The table is keyed by `(credit_rating_code, rating_agency, agency_scale)` because a rating label is meaningful only within an agency and scale (for example `A+` in S&P long-term is not the same as `A+` in DBRS short-term).

## 2. Pre-Checks

- Active enterprise licenses for S&P RatingsDirect, Moody's RMS, Fitch Connect, DBRS Viewpoint
- Azure Key Vault secrets refreshed for each vendor:
 - `kv-cat-refdata-prod/sp-ratingsdirect-token`
 - `kv-cat-refdata-prod/moodys-rms-token`
 - `kv-cat-refdata-prod/fitch-connect-token`
 - `kv-cat-refdata-prod/dbrs-viewpoint-token`
- Basel III risk-weight schedule from internal Capital Reporting team is approved for the quarter (`CRED-QTR-2026Q1-WEIGHTS.xlsx`)
- Default probability model from Credit Risk Analytics has been locked for the quarter

## 3. Vendor Downloads

Execute each vendor fetch from the credit-risk staging VM. Each vendor publishes its full scale as of quarter-end; retrieve the point-in-time snapshot.

```bash
export QTR=2026Q1
export STAGING=/mnt/refdata/staging/credit_rating/${QTR}

mkdir -p ${STAGING}/{sp,moodys,fitch,dbrs,harmonized}

# S&P Global
curl -f -H "Authorization: Bearer ${SP_TOKEN}" \
 -o ${STAGING}/sp/sp_rating_scale_${QTR}.csv \
 "https://api.spratings.com/v1/ratings/scales?asOf=2026-03-31"

# Moody's
curl -f -H "Authorization: Bearer ${MOODYS_TOKEN}" \
 -o ${STAGING}/moodys/moodys_rating_scale_${QTR}.csv \
 "https://api.moodys.com/rms/v1/scales?asOf=2026-03-31"

# Fitch
curl -f -H "X-Api-Key: ${FITCH_TOKEN}" \
 -o ${STAGING}/fitch/fitch_rating_scale_${QTR}.csv \
 "https://api.fitchconnect.com/v2/rating-scales?asOf=2026-03-31"

# DBRS
curl -f -H "Authorization: Bearer ${DBRS_TOKEN}" \
 -o ${STAGING}/dbrs/dbrs_rating_scale_${QTR}.csv \
 "https://api.dbrsmorningstar.com/viewpoint/v1/scales?asOf=2026-03-31"
```

Row-count sanity check per vendor:

| Agency | Long-term scale | Short-term scale | Issuer + issue |
|---------|-----------------|------------------|----------------|
| S&P | 22 | 6 | ~50 |
| Moody's | 21 | 4 | ~45 |
| Fitch | 22 | 5 | ~48 |
| DBRS | 23 | 6 | ~55 |

Any vendor outside a ±10% row count window requires re-fetching before proceeding.

## 4. Harmonization

The four vendor scales do not share a column convention. Apply the firm's canonical harmonization recipe (maintained by Credit Risk Data Operations) - located at `/opt/cat/credit/harmonize_ratings.py`:

```bash
python /opt/cat/credit/harmonize_ratings.py \
 --sp ${STAGING}/sp/sp_rating_scale_${QTR}.csv \
 --moodys ${STAGING}/moodys/moodys_rating_scale_${QTR}.csv \
 --fitch ${STAGING}/fitch/fitch_rating_scale_${QTR}.csv \
 --dbrs ${STAGING}/dbrs/dbrs_rating_scale_${QTR}.csv \
 --basel /opt/cat/credit/CRED-QTR-2026Q1-WEIGHTS.xlsx \
 --pd-model /opt/cat/credit/pd_model_2026Q1.json \
 --output ${STAGING}/harmonized/rating_scales.csv \
 --quarter ${QTR}
```

The harmonization script produces a single CSV matching the `ref_credit_rating_scale` schema, with columns:

`credit_rating_code, credit_rating_name, rating_agency, agency_scale, numeric_equivalent, basel_risk_weight, default_probability, is_investment_grade, effective_start_date, effective_end_date, source_version`

Credit Risk Analytics must sign off on the harmonized file before upload. Sign-off is recorded in the quarterly rating-scale Confluence page.

## 5. Upload to OneLake

```bash
az storage fs file upload \
 --source ${STAGING}/harmonized/rating_scales.csv \
 --path "ref/seeds/rating_scales/rating_scales_${QTR}.csv" \
 --account-name onelake --file-system cat-prod
```

## 6. DLT Pipeline Trigger

```bash
databricks pipelines start \
 --pipeline-id $(cat /opt/cat/config/prod/ref_ingest_adhoc.id) \
 --full-refresh false
```

Monitor `ingestion/dlt_ref_industry.py` for the `ref_credit_rating_scale` target table. Confirm:

- All rows pass `rating_agency_enum` expectation
- All rows pass `basel_weight_range BETWEEN 0 AND 1.5` expectation
- All rows pass `default_prob_range BETWEEN 0 AND 1` expectation

## 7. Post-Load Validation

```sql
-- Every current row has a defined Basel weight
SELECT COUNT(*) AS missing_weights
FROM gold.ref_credit_rating_scale
WHERE is_active = TRUE AND basel_risk_weight IS NULL;
-- Expected: 0

-- Sample key ratings across the four agencies
SELECT rating_agency, credit_rating_code, agency_scale,
 numeric_equivalent, basel_risk_weight, default_probability, is_investment_grade
FROM gold.ref_credit_rating_scale
WHERE is_active = TRUE
 AND credit_rating_code IN ('AAA','AA+','A','BBB-','BB+','B','CCC','D')
ORDER BY rating_agency, numeric_equivalent;

-- Confirm default probability monotonicity within each agency-scale
WITH ranked AS (
 SELECT rating_agency, agency_scale, credit_rating_code,
 numeric_equivalent, default_probability,
 LAG(default_probability) OVER (
 PARTITION BY rating_agency, agency_scale
 ORDER BY numeric_equivalent
) AS prev_pd
 FROM gold.ref_credit_rating_scale
 WHERE is_active = TRUE
)
SELECT COUNT(*) AS monotonicity_violations
FROM ranked
WHERE prev_pd IS NOT NULL AND default_probability < prev_pd;
-- Expected: 0

-- SCD2 closure check
SELECT rating_agency, agency_scale, credit_rating_code, COUNT(*) AS open_rows
FROM gold.ref_credit_rating_scale
WHERE is_active = TRUE
GROUP BY rating_agency, agency_scale, credit_rating_code
HAVING COUNT(*) > 1;
-- Expected: 0 rows
```

## 8. Downstream Recomputation

After load, trigger the downstream portfolio rating re-evaluation job. Positions held against issuers whose rating changed class (e.g. BBB- to BB+, investment to non-investment) must be recapitalized:

```bash
databricks jobs run-now --job-id $(cat /opt/cat/config/prod/recap_on_rating_change.id)
```

Attach the resulting recapitalization report to the quarterly Risk Committee pack.

## 9. Rollback Procedure

If validation fails or if Credit Risk Analytics rescinds sign-off:

1. Restore the prior table version:
 ```sql
 RESTORE TABLE gold.ref_credit_rating_scale TO VERSION AS OF <N-1>;
 ```
2. Archive the failed staging folder:
 ```bash
 az storage fs directory move \
 --from "ref/seeds/rating_scales" \
 --to "ref/seeds/_failed/${QTR}/rating_scales" \
 --account-name onelake --file-system cat-prod
 ```
3. Revert any downstream recapitalization entries pending commitment
4. Open a P2 incident, root-cause analysis within 48 hours

## 10. Sign-Off

| Role | Required |
|---------------------------------------|----------------------|
| Credit Risk Data Operator | Vendor fetch + load |
| Credit Risk Analytics Lead | Harmonization sign |
| Chief Credit Officer (or delegate) | Quarterly approval |
| Capital Reporting Lead | Basel weights signed |

Archive the runbook, harmonization diff, and sign-off sheet to `ref/audit/${QTR}_runbook_03.pdf`.
