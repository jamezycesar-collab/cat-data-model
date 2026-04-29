# Entity Catalog - Expanded Enterprise Trade Lifecycle Model

**Platform:** Azure Databricks Delta Lake (primary) / Hive 3.x on HDInsight (compatibility variant)
**Total unique entities:** 83
**Generation date:** 2026-04-21

---

## Summary by Subject Area

| # | Subject Area | Entities | Source File |
|---|---|---:|---|
| 1 | Party Model | 14 | `01_party_model_delta.sql` |
| 2 | Instrument Model | 14 | `02_instrument_model_delta.sql` |
| 3 | Venue Model | 4 | `03_venue_model_delta.sql` |
| 4 | Agreement Model | 9 | `04_agreement_model_delta.sql` |
| 5 | Pre-Trade (Quote/RFE/IOI) | 5 | `05_pretrade_model_delta.sql` |
| 6 | Order Request | 1 | `06_order_request_model_delta.sql` |
| 7 | Order Stage | 4 | `07_order_stage_model_delta.sql` |
| 8 | Execution | 2 | `08_execution_model_delta.sql` |
| 9 | Allocation | 2 | `09_allocation_model_delta.sql` |
| 10 | Post-Trade (Confirmation/Clearing/Settlement) | 4 | `10_posttrade_model_delta.sql` |
| 11 | Position | 2 | `11_position_model_delta.sql` |
| 12 | Operations & Regulatory | 6 | `12_operations_regulatory_model_delta.sql` |
| 13 | Reference Tables | 16 | `13_reference_tables_delta.sql` |
| | **TOTAL** | **83** | |

---

## 1. Party Model (14 entities)

| Entity | Type | Status | Primary Key |
|---|---|---|---|
| `party` | Supertype | Enhanced | `party_id` |
| `legal_entity` | Subtype | Enhanced | `party_id` (FK) |
| `natural_person` | Subtype | Existing | `party_id` (FK) |
| `party_identifier` | Lookup | Existing | `identifier_id` |
| `party_role` | Role | Enhanced | `party_role_id` |
| `party_relationship` | Linkage | Existing | `relationship_id` |
| `party_address` | Detail | Existing | `address_id` |
| `party_contact` | Detail | Existing | `contact_id` |
| `account` | Subtype | Enhanced | `account_id` |
| `beneficial_owner` | New | New | `beneficial_owner_id` |
| `ownership_structure` | New | New | `ownership_id` |
| `control_relationship` | New | New | `control_rel_id` |
| `desk` | New | New | `desk_id` |
| `information_barrier` | New | New | `barrier_id` |

## 2. Instrument Model (14 entities)

| Entity | Type | Status | Primary Key |
|---|---|---|---|
| `instrument` | Supertype | Enhanced | `instrument_id` |
| `equity_instrument` | Subtype | Existing | `instrument_id` (FK) |
| `debt_instrument` | Subtype | Enhanced | `instrument_id` (FK) |
| `derivative_instrument` | Subtype | Enhanced | `instrument_id` (FK) |
| `fund_instrument` | Subtype | New | `instrument_id` (FK) |
| `instrument_identifier` | Lookup | Existing | `identifier_id` |
| `instrument_classification` | Mapping | Existing | `classification_id` |
| `instrument_leg` | Detail | New | `leg_id` |
| `instrument_relationship` | Linkage | New | `relationship_id` |
| `underlying_reference` | New | New | `underlying_ref_id` |
| `corporate_action` | New | New | `ca_id` |
| `option_contract_details` | Subtype | New | `option_id` |
| `future_contract_details` | Subtype | New | `future_id` |
| `swap_contract_details` | Subtype | New | `swap_id` |

## 3. Venue Model (4 entities)

| Entity | Type | Status | Primary Key |
|---|---|---|---|
| `venue` | Core | Enhanced | `venue_id` |
| `venue_asset_class` | Mapping | Existing | `venue_asset_class_id` |
| `venue_trading_session` | Detail | New | `session_id` |
| `venue_connectivity` | New | New | `connectivity_id` |

## 4. Agreement Model (9 entities)

| Entity | Type | Status | Primary Key |
|---|---|---|---|
| `agreement` | Master | New | `agreement_id` |
| `agreement_schedule` | Detail | New | `schedule_id` |
| `agreement_netting_set` | Detail | New | `netting_set_id` |
| `collateral_agreement` | Sub-agreement | New | `collateral_agreement_id` |
| `collateral_haircut` | Detail | New | `haircut_id` |
| `margin_call` | Transaction | New | `margin_call_id` |
| `repo_transaction` | Transaction | New | `repo_id` |
| `securities_loan` | Transaction | New | `loan_id` |
| `commission_schedule` | Detail | New | `schedule_id` |

## 5. Pre-Trade (Quote/RFE/IOI) (5 entities)

| Entity | CAT Event | FIX Msg | Primary Key |
|---|---|---|---|
| `quote_request` | MEQR | 35=R | `quote_request_id` |
| `quote_event` | MEQS | 35=S | `quote_event_id` |
| `quote_response` | - | - | `quote_response_id` |
| `request_for_execution` | - | - | `rfe_id` |
| `indication_of_interest` | - | 35=6 | `ioi_id` |

## 6. Order Request (1 entity)

| Entity | CAT Event | Primary Key |
|---|---|---|
| `order_request` | MEIR / MOIR | `order_request_id` |

## 7. Order Stage (4 entities)

| Entity | CAT Events | FIX Msg | Primary Key |
|---|---|---|---|
| `order_event` | MENO, MEOA, MEOJ, MECO, MECOC, MEOC, MEOCR, MENOS, MLNO, MLOA, MLOC, MLCO, MONO, MOOA, MOOC, MONOS, MOCO, MOCOC, EOA, EOC | 35=D, 35=G, 35=F | `order_event_id` |
| `order_route` | MEOR, MEIR, MEIC, MEIM, MLOR, MOOR, MOFA | - | `route_id` |
| `order_modification` | MEOM, MEOMR, MECOM, MLOM, MLICR, MOOM, MOCOM, EOM | 35=G | `modification_id` |
| `order_leg` | MLNO-linkage | 555-group | `order_leg_id` |

## 8. Execution (2 entities)

| Entity | CAT Events | FIX Msg | Primary Key |
|---|---|---|---|
| `execution` | MEOT, MEOTS, MEOF, MOOT (and Section 5.2 multi-leg events MLOR/MLOC/...), MOOF, | 35=8 | `execution_id` |
| `execution_leg` | Section 5.2 multi-leg events | 555-group | `execution_leg_id` |

## 9. Allocation (2 entities)

| Entity | CAT Events | FIX Msg | Primary Key |
|---|---|---|---|
| `allocation` | MEPA, MEAA, MOFA | 35=J, 35=P | `allocation_id` |
| `allocation_instruction` | - | - | `instruction_id` |

## 10. Post-Trade (4 entities)

| Entity | Description | FIX Msg | Primary Key |
|---|---|---|---|
| `trade_confirmation` | DTCC CTM / Omgeo affirmation | 35=AK | `confirmation_id` |
| `clearing_record` | CCP novation | - | `clearing_id` |
| `settlement_instruction` | CSD delivery | MT515/540-548 | `settlement_id` |
| `settlement_obligation` | CNS netting | - | `obligation_id` |

## 11. Position (2 entities)

| Entity | Description | Primary Key |
|---|---|---|
| `position` | Point-in-time snapshot (Gold-layer) | `position_id` |
| `position_break` | Reconciliation exceptions | `break_id` |

## 12. Operations & Regulatory (6 entities)

| Entity | Status | Primary Key |
|---|---|---|
| `error_correction` | Existing | `correction_id` |
| `reporting_submission` | Existing | `submission_id` |
| `clock_sync_audit` | Existing | `audit_id` |
| `regulatory_report` | New | `report_id` |
| `regulatory_hold` | New | `hold_id` |
| `audit_trail_event` | New | `audit_event_id` |

## 13. Reference Tables (16 entities)

| Entity | Source Authority | Rows (approx) | Primary Key |
|---|---|---:|---|
| `ref_cat_event_type` | FINRA CAT v4.1.0r9 | 50 | `(cat_event_type_code, effective_start_date)` |
| `ref_mic` | ISO 10383 | ~3,000 | `(mic_code, effective_start_date)` |
| `ref_country` | ISO 3166-1 | ~249 | `(country_code, effective_start_date)` |
| `ref_currency` | ISO 4217 | ~180 | `(currency_code, effective_start_date)` |
| `ref_cfi_category` | ISO 10962 | ~1,000 | `(cfi_code, effective_start_date)` |
| `ref_entity_legal_form` | ISO 20275 (GLEIF) | ~3,000 | `(elf_code, effective_start_date)` |
| `ref_order_type` | FIX tag 40 | ~30 | `(order_type_code, effective_start_date)` |
| `ref_time_in_force` | FIX tag 59 | ~12 | `(tif_code, effective_start_date)` |
| `ref_handling_instruction` | FIX tag 21 | 3 | `(handling_instruction_code, effective_start_date)` |
| `ref_party_role` | FIX tag 452 + internal | ~80 | `(party_role_code, effective_start_date)` |
| `ref_instrument_type` | Internal (ISO 10962 aligned) | ~40 | `(instrument_type_code, effective_start_date)` |
| `ref_day_count` | ISDA / ICMA | ~12 | `(day_count_code, effective_start_date)` |
| `ref_settlement_venue` | Internal | ~30 | `(settlement_venue_code, effective_start_date)` |
| `ref_regulator` | Internal | ~30 | `(regulator_code, effective_start_date)` |
| `ref_credit_rating_scale` | S&P, Moody's, Fitch, DBRS | ~100 | `(credit_rating_code, effective_start_date)` |
| `ref_tax_jurisdiction` | OECD / IRS | ~200 | `(tax_jurisdiction_code, effective_start_date)` |

---
