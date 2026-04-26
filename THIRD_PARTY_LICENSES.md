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

# Third-Party Licenses and Attributions

This document provides the detailed attribution required by Section 4 of the Apache License, Version 2.0, for third-party works referenced, seeded, or interoperated with by this repository.

Inclusion here is an acknowledgement of contribution and does not imply endorsement by the listed copyright holder.

| Category | Count |
|----------|-------|
| Regulatory and industry specifications | 5 |
| ISO standards | 6 |
| Platform and tooling | 4 |
| Data modeling methodologies | 4 |
| Public regulatory frameworks | 5 |
| **Total distinct attributions** | **24** |

---

## 1. Regulatory and Industry Specifications

### 1.1 FINRA Consolidated Audit Trail (CAT)

- **Copyright holder**: FINRA CAT, LLC (a subsidiary of Financial Industry Regulatory Authority, Inc.)
- **Work**: Industry Member Technical Specifications, version 4.1.0r9
- **Source**: https://www.catnmsplan.com/specifications
- **Use in this repository**:
 - The 50-event CAT taxonomy is seeded in `ref-data-pipelines/ingestion/dlt_ref_cat_taxonomy.py`
 - Event codes, category, lifecycle stage, phase, and submission file type are encoded as a Python data literal
 - Fact table column naming (e.g. `firm_roe_id`, `cat_event_code`) follows the CAT camelCase-to-snake_case convention
 - The submission-generator at `gold/07_submission_generator.py` produces JSON in the CAT-specified schema
- **License basis**: Fair-use for regulatory compliance implementation by registered broker-dealers subject to SEC Rule 613 and FINRA Rule 6800 series
- **Not redistributed**: No FINRA source code, JSON Schema, or PDF specification text
- **Responsibility**: Implementers are responsible for validating the taxonomy against the then-current FINRA CAT specification

### 1.2 SEC Rule 613

- **Issuing authority**: United States Securities and Exchange Commission
- **Citation**: 17 CFR 242.613
- **Status**: Public law of the United States; not copyrighted
- **Use**: Pre-trade compliance model conformance

### 1.3 ISDA Definitions and Master Agreements

- **Copyright holder**: International Swaps and Derivatives Association, Inc.
- **Works referenced**:
 - ISDA 2006 Interest Rate Derivatives Definitions
 - ISDA 2021 Interest Rate Derivatives Definitions
 - ISDA Master Agreement (1992, 2002)
 - ISDA Credit Support Annex (CSA)
- **Source**: https://www.isda.org
- **Use in this repository**:
 - 15 canonical day-count conventions seeded in `ref-data-pipelines/ingestion/dlt_ref_isda_conventions.py`
 - Agreement model (`expanded-model/04_agreement_model_delta.sql`) references ISDA Master Agreement and CSA as conceptual inputs to the `agreement` and `agreement_netting_set` tables
- **License basis**: Industry-standard data model references; day-count codes are factual identifiers
- **Not redistributed**: No ISDA legal text, definition document, or proprietary XML

### 1.4 FIX Protocol

- **Copyright holder**: FIX Protocol Limited / FIX Trading Community
- **Work**: FIX Protocol Specification (versions 4.4, 5.0, 5.0 SP2)
- **Source**: https://www.fixtrading.org
- **Use in this repository**:
 - FIX tag-to-column mappings (60+ tags) included as machine-readable CSV at `expanded-model/fix_tag_mappings.csv`
 - FIX enum values for order type (tag 40), time-in-force (tag 59), and handling instruction (tag 21) are seeded in `ref-data-pipelines/ingestion/dlt_ref_fix_enums.py`
- **License basis**: Open industry protocol; tag numbers and enum values are factual identifiers
- **Not redistributed**: No FIX Protocol source document

### 1.5 FpML (Financial products Markup Language)

- **Copyright holder**: International Swaps and Derivatives Association, Inc.
- **Work**: FpML 5.x Recommendation
- **Source**: https://www.fpml.org
- **Use in this repository**: Referenced for derivative contract schema elements (tables `derivative_contract`, `structured_product`, `instrument_leg`)
- **Not redistributed**: No FpML XML Schema Definition

---

## 2. ISO Standards

ISO seed data is fetched from the ISO or authorized-distributor publication endpoint at pipeline runtime. No ISO-published file is redistributed inside this repository.

### 2.1 ISO 3166-1 Country Codes

- **Copyright holder**: International Organization for Standardization
- **Source**: https://www.iso.org/iso-3166-country-codes.html
- **Target table**: `ref_country`
- **Ingestion**: `ref-data-pipelines/ingestion/dlt_ref_iso_codes.py`

### 2.2 ISO 4217 Currency Codes

- **Copyright holder**: International Organization for Standardization
- **Publisher**: SIX Group (Maintenance Agency)
- **Source**: https://www.six-group.com/en/products-services/financial-information/data-standards.html
- **Target table**: `ref_currency`

### 2.3 ISO 10383 Market Identifier Codes (MIC)

- **Copyright holder**: International Organization for Standardization
- **Publisher**: SWIFT (Registration Authority)
- **Source**: https://www.iso20022.org/market-identifier-codes
- **Target table**: `ref_mic`

### 2.4 ISO 10962 Classification of Financial Instruments (CFI)

- **Copyright holder**: International Organization for Standardization
- **Target table**: `ref_cfi_category`

### 2.5 ISO 17442 Legal Entity Identifier (LEI)

- **Copyright holder**: International Organization for Standardization
- **Publisher**: Global Legal Entity Identifier Foundation (GLEIF)
- **Source**: https://www.gleif.org
- **Use**: LEI format validation in `party` and `legal_entity` tables; resolution via GLEIF concatenated file

### 2.6 ISO 20275 Entity Legal Form (ELF) Code List

- **Copyright holder**: International Organization for Standardization
- **Target table**: `ref_entity_legal_form`

---

## 3. Public Regulatory Frameworks (Non-Copyrighted)

### 3.1 Basel Committee on Banking Supervision (BCBS) Publications

- **Issuing authority**: Bank for International Settlements (BIS)
- **Documents**: BCBS 239 (Risk Data Aggregation), BCBS 279 (SA-CCR), Basel III Capital Framework
- **Status**: Public regulatory documents
- **Use**: Referenced in regulatory model and risk-framework CHECK constraint

### 3.2 EU Markets in Financial Instruments Directive II / Regulation (MiFID II / MiFIR)

- **Issuing authority**: European Union
- **Status**: Public EU regulation

### 3.3 Dodd-Frank Wall Street Reform and Consumer Protection Act

- **Issuing authority**: United States Congress
- **Status**: Public US law

### 3.4 Global Master Repurchase Agreement (GMRA)

- **Copyright holder**: International Capital Market Association (ICMA)
- **Use**: Referenced conceptually in `agreement` table as a repo-framework identifier
- **Not redistributed**: No GMRA legal text

### 3.5 Global Master Securities Lending Agreement (GMSLA)

- **Copyright holder**: International Securities Lending Association (ISLA)
- **Use**: Referenced conceptually in `agreement` table as a securities-lending-framework identifier
- **Not redistributed**: No GMSLA legal text

---

## 4. Platform and Tooling

No platform source is redistributed by this repository. The following are noted as execution dependencies.

### 4.1 Apache Spark

- **Copyright holder**: The Apache Software Foundation
- **License**: Apache License 2.0
- **Role**: Execution engine for PySpark transformations in DLT pipelines

### 4.2 Delta Lake

- **Copyright holder**: The Delta Lake Project Authors
- **License**: Apache License 2.0
- **Role**: Storage format for Bronze / Silver / Gold tables

### 4.3 Databricks Delta Live Tables (DLT)

- **Copyright holder**: Databricks, Inc.
- **Role**: Declarative pipeline framework for `dlt-pipelines/` and `ref-data-pipelines/` notebooks
- **License basis**: Used as managed service; no Databricks code is redistributed

### 4.4 Microsoft Fabric (OneLake, Lakehouse, Warehouse)

- **Copyright holder**: Microsoft Corporation
- **Role**: Target platform for `fabric/` migration DDL
- **License basis**: Used as managed service; no Microsoft code is redistributed

---

## 5. Data Modeling Methodologies

### 5.1 Data Vault 2.0

- **Author**: Dan Linstedt
- **Copyright holder**: Genesee Academy
- **Use**: Silver layer hub / link / satellite modeling pattern in `data-vault/`

### 5.2 Dimensional Modeling

- **Author**: Ralph Kimball
- **Copyright holder**: The Kimball Group
- **Use**: Gold layer star-schema modeling pattern in `gold/`

### 5.3 DAMA-DMBOK (Data Management Body of Knowledge)

- **Copyright holder**: DAMA International
- **Use**: Governance principles referenced in operating documentation

### 5.4 BCBS 239 Principles for Effective Risk Data Aggregation and Risk Reporting

- **Copyright holder**: Bank for International Settlements
- **Principle implemented**: Principle 2 (Data Architecture and IT Infrastructure) - column-level lineage via `dv2_source_hk` on facts and `record_source` on reference tables

---

## 6. Status Summary

| Source | Direct redistribution of text/source | Compliance mechanism |
|--------|--------------------------------------|----------------------|
| FINRA CAT | No | Fair-use, taxonomy as factual identifier set |
| SEC Rule 613 | N/A | Public law |
| ISDA | No | Factual identifier set (day-count codes) |
| FIX | No | Factual identifier set (tags + enums) |
| FpML | No | Conceptual reference only |
| ISO 3166 | No | Runtime fetch |
| ISO 4217 | No | Runtime fetch |
| ISO 10383 | No | Runtime fetch |
| ISO 10962 | No | Runtime reference |
| ISO 17442 | No | Format validation only |
| ISO 20275 | No | Runtime fetch |
| BCBS / MiFID / Dodd-Frank | N/A | Public law / regulation |
| GMRA / GMSLA | No | Conceptual reference only |
| Apache Spark | No | Apache 2.0 compatible |
| Delta Lake | No | Apache 2.0 compatible |
| Databricks DLT | No | Managed service |
| Microsoft Fabric | No | Managed service |
| Data Vault 2.0 | No | Methodology reference |
| Kimball dimensional | No | Methodology reference |

Implementers assume responsibility for verifying that their use of this repository complies with the then-current terms of each third-party copyright holder.
