#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 cat-pretrade-data-model contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
_build_drawio.py - Emit 7 draw.io subject-area diagrams.

Layout: 250px horizontal, 220px vertical grid. Hub entity at center of each
cluster; relationships with orthogonal routing + cardinality labels. Entity
boxes are swimlane style (title = entity name, columns = top 5 attrs).

Palette per cat-pretrade-refactor section :
 Party Model #dae8fc / #6c8ebf (light blue)
 Instrument Model #d5e8d4 / #82b366 (light green)
 Venue Model #fff2cc / #d6b656 (light yellow)
 Agreement Model #f5f5f5 / #666666 (light gray)
 Trade Lifecycle #f8cecc / #b85450 (light red)
 Operations #e1d5e7 / #9673a6 (light purple)
 Regulatory #fce5cd / #d79b00 (light orange)
"""
from __future__ import annotations
from pathlib import Path
from xml.sax.saxutils import escape

OUT = Path(__file__).resolve.parent
ENTITY_W, ENTITY_H = 200, 160
COL_GAP, ROW_GAP = 250, 220

def entity_style(fill: str, stroke: str) -> str:
 return ("shape=table;startSize=30;container=1;collapsible=0;childLayout=tableLayout;"
 "fontSize=12;fillColor={f};strokeColor={s};html=1;".format(f=fill, s=stroke))

def col_style -> str:
 return ("shape=partialRectangle;html=1;whiteSpace=wrap;connectable=0;"
 "strokeColor=inherit;overflow=hidden;fillColor=none;top=0;left=0;bottom=0;right=0;"
 "pointerEvents=1;fontSize=11;")

def entity_xml(eid: int, x: int, y: int, name: str, cols: list[str],
 fill: str, stroke: str) -> list[str]:
 """Emit an entity swimlane with one row per column name string."""
 lines: list[str] = []
 table_id = f"entity_{eid}"
 lines.append(f'<mxCell id="{table_id}" value="{escape(name)}" '
 f'style="{entity_style(fill, stroke)}" vertex="1" parent="1">'
 f'<mxGeometry x="{x}" y="{y}" width="{ENTITY_W}" height="{ENTITY_H}" as="geometry"/>'
 f"</mxCell>")
 for i, c in enumerate(cols):
 cid = f"{table_id}_c{i}"
 lines.append(f'<mxCell id="{cid}" value="{escape(c)}" style="{col_style}" '
 f'vertex="1" parent="{table_id}">'
 f'<mxGeometry y="{30 + 22 * i}" width="{ENTITY_W}" height="22" as="geometry"/>'
 f"</mxCell>")
 return lines

def edge_xml(eid: int, src_id: str, dst_id: str, label: str) -> str:
 return (f'<mxCell id="edge_{eid}" value="{escape(label)}" '
 f'style="edgeStyle=orthogonalEdgeStyle;rounded=0;html=1;exitX=1;exitY=0.5;'
 f'exitDx=0;exitDy=0;entryX=0;entryY=0.5;entryDx=0;entryDy=0;fontSize=10;"'
 f' edge="1" parent="1" source="{src_id}" target="{dst_id}">'
 f'<mxGeometry relative="1" as="geometry"/>'
 f"</mxCell>")

def build_drawio(diagram_name: str, fill: str, stroke: str,
 entities: list[tuple[str, int, int, list[str]]],
 edges: list[tuple[int, int, str]]) -> str:
 """entities: (name, row, col, column_list)
 edges: (src_index, dst_index, label)
 """
 cells: list[str] = []
 cells.append('<mxCell id="0"/>')
 cells.append('<mxCell id="1" parent="0"/>')
 ent_ids: list[str] = []
 for i, (name, row, col, columns) in enumerate(entities):
 x = 60 + col * COL_GAP
 y = 40 + row * ROW_GAP
 cells.extend(entity_xml(i, x, y, name, columns, fill, stroke))
 ent_ids.append(f"entity_{i}")
 for j, (si, di, lbl) in enumerate(edges):
 cells.append(edge_xml(j, ent_ids[si], ent_ids[di], lbl))
 xml = ('<mxfile host="app.diagrams.net" version="24.7.17">'
 f'<diagram name="{escape(diagram_name)}" id="{diagram_name}">'
 '<mxGraphModel dx="1422" dy="757" grid="1" gridSize="10" guides="1" '
 'tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" '
 'pageWidth="1600" pageHeight="1200" math="0" shadow="0">'
 '<root>'
 + "".join(cells)
 + '</root></mxGraphModel></diagram></mxfile>')
 return xml

# ============================================================================
# 1. Party Model
# ============================================================================
party = build_drawio("party_model", "#dae8fc", "#6c8ebf",
 entities=[
 # (name, row, col, columns)
 ("party", 1, 2, ["party_id PK", "party_type", "party_status", "ultimate_parent_party_id FK", "domicile_country"]),
 ("legal_entity", 0, 1, ["party_id PK/FK", "lei", "legal_form", "incorporation_country", "g_sib_flag"]),
 ("natural_person", 0, 3, ["party_id PK/FK", "given_name_hash", "family_name_hash", "citizenship_country", "pep_flag"]),
 ("party_role", 1, 0, ["party_role_id PK", "party_id FK", "role_type", "desk_id FK", "start_date"]),
 ("party_relationship", 2, 0, ["relationship_id PK", "parent_party_id FK", "child_party_id FK", "relationship_type", "ownership_percentage"]),
 ("party_identifier", 1, 4, ["identifier_id PK", "party_id FK", "id_type", "id_value", "effective_start"]),
 ("party_address", 2, 4, ["address_id PK", "party_id FK", "address_type", "country_code", "postal_code"]),
 ("account", 2, 2, ["account_id PK", "party_id FK", "account_type", "custodian_party_id FK", "base_currency"]),
 ("beneficial_ownership", 3, 1, ["ownership_id PK", "owned_party_id FK", "owner_party_id FK", "ownership_percentage", "ultimate_beneficial_owner_flag"]),
 ("desk", 3, 3, ["desk_id PK", "desk_name", "desk_type", "parent_desk_id FK", "lei"]),
 ],
 edges=[
 (0, 1, "1..1"),
 (0, 2, "1..1"),
 (0, 3, "1..*"),
 (0, 4, "1..*"),
 (0, 5, "1..*"),
 (0, 6, "1..*"),
 (0, 7, "1..*"),
 (0, 8, "1..*"),
 (9, 3, "1..*"),
 ],)

# ============================================================================
# 2. Instrument Model
# ============================================================================
instrument = build_drawio("instrument_model", "#d5e8d4", "#82b366",
 entities=[
 ("instrument", 1, 2, ["instrument_id PK", "asset_class", "isin", "cusip", "figi"]),
 ("equity_attributes", 0, 0, ["instrument_id PK/FK", "listing_type", "shares_outstanding", "voting_rights", "adr_ratio"]),
 ("option_attributes", 0, 1, ["instrument_id PK/FK", "option_style", "strike_price", "expiration_date", "underlying_instrument_id FK"]),
 ("fixed_income_attributes", 0, 3, ["instrument_id PK/FK", "bond_type", "coupon_rate", "day_count FK", "call_date"]),
 ("derivative_contract", 0, 4, ["instrument_id PK/FK", "contract_type", "clearing_house_party_id FK", "isda_product_id", "notional_currency"]),
 ("instrument_listing", 2, 0, ["listing_id PK", "instrument_id FK", "venue_id FK", "primary_listing_flag", "first_trade_date"]),
 ("instrument_identifier", 2, 2, ["identifier_id PK", "instrument_id FK", "id_type", "id_value", "id_issuer"]),
 ("instrument_underlying", 2, 4, ["underlying_id PK", "instrument_id FK", "underlying_instrument_id FK", "underlying_type", "weight"]),
 ("corporate_action_linkage", 3, 2, ["linkage_id PK", "instrument_id FK", "event_type", "ex_date", "record_date"]),
 ],
 edges=[(0, i, "1..*") for i in range(1, 9)],)

# ============================================================================
# 3. Venue Model
# ============================================================================
venue = build_drawio("venue_model", "#fff2cc", "#d6b656",
 entities=[
 ("venue", 1, 1, ["venue_id PK", "mic ISO10383", "venue_type", "country_code", "operating_mic"]),
 ("venue_asset_class", 0, 0, ["venue_asset_class_id PK", "venue_id FK", "asset_class", "supported_flag", "effective_start"]),
 ("venue_trading_session", 0, 2, ["session_id PK", "venue_id FK", "session_type", "session_start_time", "timezone"]),
 ("venue_connectivity", 2, 0, ["connectivity_id PK", "venue_id FK", "connection_type", "protocol", "endpoint_url"]),
 ],
 edges=[(0, 1, "1..*"), (0, 2, "1..*"), (0, 3, "1..*")],)

# ============================================================================
# 4. Agreement Model
# ============================================================================
agreement = build_drawio("agreement_model", "#f5f5f5", "#666666",
 entities=[
 ("agreement", 1, 2, ["agreement_id PK", "agreement_type", "party_a_id FK", "party_b_id FK", "governing_law"]),
 ("agreement_schedule", 0, 1, ["schedule_id PK", "agreement_id FK", "schedule_section", "section_number", "elected_option"]),
 ("agreement_netting_set", 0, 3, ["netting_set_id PK", "agreement_id FK", "netting_set_type", "close_out_netting", "settlement_netting"]),
 ("collateral_agreement", 2, 1, ["collateral_agreement_id PK", "agreement_id FK", "collateral_category", "base_currency", "threshold_amount"]),
 ("collateral_haircut", 3, 0, ["haircut_id PK", "collateral_agreement_id FK", "collateral_type", "haircut_percentage", "rating_threshold"]),
 ("margin_call", 3, 2, ["margin_call_id PK", "collateral_agreement_id FK", "call_date", "exposure_amount", "call_status"]),
 ("repo_transaction", 2, 3, ["repo_id PK", "agreement_id FK", "start_date", "end_date", "repo_rate"]),
 ("securities_loan", 2, 4, ["loan_id PK", "agreement_id FK", "loaned_instrument_id FK", "borrow_fee_rate", "collateral_type"]),
 ("commission_schedule", 1, 0, ["commission_schedule_id PK", "agreement_id FK", "asset_class", "commission_rate_bps", "effective_start"]),
 ],
 edges=[
 (0, 1, "1..*"), (0, 2, "1..*"), (0, 3, "1..*"),
 (0, 6, "1..*"), (0, 7, "1..*"), (0, 8, "1..*"),
 (3, 4, "1..*"), (3, 5, "1..*"),
 ],)

# ============================================================================
# 5. Trade Lifecycle
# ============================================================================
trade = build_drawio("trade_lifecycle", "#f8cecc", "#b85450",
 entities=[
 ("quote_request", 0, 0, ["quote_request_id PK", "requesting_party_id FK", "instrument_id FK", "quantity", "side"]),
 ("quote_event", 0, 1, ["quote_id PK", "quote_request_id FK", "quoting_party_id FK", "bid_price", "ask_price"]),
 ("request_for_execution", 0, 2, ["rfe_id PK", "quote_id FK", "instrument_id FK", "quantity", "limit_price"]),
 ("order_request", 0, 3, ["order_request_id PK", "cat_order_id", "originator_party_id FK", "instrument_id FK", "quantity"]),
 ("order_event", 1, 3, ["order_id PK", "order_request_id FK", "cat_order_id", "parent_order_id FK", "order_status"]),
 ("order_route", 1, 2, ["route_id PK", "order_id FK", "destination_venue_id FK", "algo_strategy", "route_timestamp"]),
 ("order_modification", 1, 1, ["modification_id PK", "order_id FK", "modification_type", "old_quantity", "new_quantity"]),
 ("execution", 2, 3, ["execution_id PK", "order_id FK", "venue_id FK", "executed_quantity", "executed_price"]),
 ("allocation", 3, 3, ["allocation_id PK", "execution_id FK", "account_id FK", "allocated_quantity", "allocation_pct"]),
 ("trade_confirmation", 3, 2, ["confirmation_id PK", "execution_id FK", "allocation_id FK", "confirmation_status", "affirmation_status"]),
 ("clearing_record", 3, 1, ["clearing_id PK", "execution_id FK", "ccp_party_id FK", "clearing_status", "novation_timestamp"]),
 ("settlement_instruction", 4, 3, ["instruction_id PK", "allocation_id FK", "custodian_party_id FK", "contractual_settle_date", "delivery_type"]),
 ("settlement_obligation", 4, 2, ["obligation_id PK", "instruction_id FK", "actual_settle_date", "settlement_status", "fail_reason"]),
 ("position", 5, 3, ["position_id PK", "account_id FK", "instrument_id FK", "position_date", "market_value"]),
 ("position_break", 5, 2, ["break_id PK", "position_id FK", "break_detected_date", "break_type", "resolution_status"]),
 ],
 edges=[
 (0, 1, "1..*"), (1, 2, "1..*"), (2, 3, "1..*"),
 (3, 4, "1..*"), (4, 5, "1..*"), (4, 6, "1..*"),
 (4, 7, "1..*"), (7, 8, "1..*"),
 (7, 9, "1..*"), (8, 9, "1..*"),
 (7, 10, "1..*"), (8, 11, "1..*"), (11, 12, "1..1"),
 (8, 13, "1..*"), (13, 14, "1..*"),
 ],)

# ============================================================================
# 6. Operations
# ============================================================================
ops = build_drawio("operations", "#e1d5e7", "#9673a6",
 entities=[
 ("error_correction", 0, 0, ["correction_id PK", "cat_order_id", "correction_type", "corrected_value", "submission_id FK"]),
 ("clock_sync_audit", 0, 2, ["sync_event_id PK", "system_identifier", "nist_time_server", "drift_microseconds", "sync_date"]),
 ("audit_trail_event", 1, 1, ["audit_event_id PK", "entity_type", "entity_id", "event_type", "actor_party_id FK"]),
 ],
 edges=[(0, 2, "logs_via"), (1, 2, "logs_via")],)

# ============================================================================
# 7. Regulatory Reporting
# ============================================================================
reg = build_drawio("regulatory_reporting", "#fce5cd", "#d79b00",
 entities=[
 ("reporting_submission", 1, 0, ["submission_id PK", "submission_date", "regulator_code FK", "report_type", "submission_status"]),
 ("regulatory_report", 1, 2, ["report_id PK", "submission_id FK", "cat_event_type FK", "cat_order_id", "firm_roe_id"]),
 ("regulatory_hold", 0, 1, ["hold_id PK", "entity_type", "regulator_code FK", "hold_type", "hold_status"]),
 ("ref_cat_event_type", 2, 2, ["cat_event_code PK", "event_name", "event_phase", "submission_file_type", "active_flag"]),
 ("ref_regulator", 2, 0, ["regulator_code PK", "regulator_name", "jurisdiction", "submission_endpoint", "oversight_domain"]),
 ],
 edges=[
 (0, 1, "1..*"), (0, 4, "submitted_to"),
 (1, 3, "typed_by"), (2, 4, "imposed_by"),
 ],)

for name, xml in [
 ("party_model.drawio", party),
 ("instrument_model.drawio", instrument),
 ("venue_model.drawio", venue),
 ("agreement_model.drawio", agreement),
 ("trade_lifecycle.drawio", trade),
 ("operations.drawio", ops),
 ("regulatory_reporting.drawio", reg),
]:
 (OUT / name).write_text(xml)

print(f"[OK] Generated 7 draw.io files in {OUT}")
