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
- Microsoft Fabric DDL Migration
Converts Databricks Delta SQL files to:
 (a) Fabric Lakehouse (Spark SQL-compatible subset - USING DELTA kept)
 (b) Fabric Warehouse (T-SQL - VARCHAR(MAX)/DATETIME2(7)/BIT, DISTRIBUTION=HASH)

Source files:
 - output/expanded-model/*.sql -> expanded_ddl_fabric_*.sql
 - output/data-vault/*.sql -> dv2_ddl_fabric_*.sql
"""
from __future__ import annotations
import re
from pathlib import Path

ROOT = Path(__file__).resolve.parent.parent
EXPANDED = ROOT / "expanded-model"
DV2 = ROOT / "data-vault"
OUT = ROOT / "fabric"

EXCLUDE_EXPANDED = {
 # duplicates of the combined 01/02 files
 "01a_party_core_delta.sql",
 "01b_party_attributes_delta.sql",
 "01c_party_new_delta.sql",
 "02a_instrument_core_delta.sql",
 "02b_instrument_derivatives_delta.sql",
 "02c_instrument_classification_delta.sql",
 # python helper
 "_delta_to_hive.py",
 "_entity_tally.txt",
}

# =====================================================================
# Lakehouse conversion (minimal - mostly keeps Delta syntax)
# =====================================================================
def to_lakehouse(sql: str) -> str:
 s = sql
 # Remove ZORDER clauses (not supported - V-Order is automatic)
 s = re.sub(r"\n\s*ZORDER\s+BY\s*\([^)]*\)\s*;?", "", s, flags=re.IGNORECASE)
 # Remove autoOptimize TBLPROPERTIES entries
 s = re.sub(r"'delta\.autoOptimize\.[a-zA-Z_]+'\s*=\s*'[^']*'\s*,?\s*",
 "",
 s,
 flags=re.IGNORECASE,)
 # Remove CLUSTER BY (Liquid Clustering) if present
 s = re.sub(r"\n\s*CLUSTER\s+BY\s*\([^)]*\)", "", s, flags=re.IGNORECASE)
 # Clean up any trailing commas left inside TBLPROPERTIES parens
 s = re.sub(r",\s*\)", ")", s)
 # Normalize "compression.codec" -> Fabric uses auto but keep for parity
 return s

# =====================================================================
# Warehouse conversion (T-SQL)
# =====================================================================
def to_warehouse(sql: str) -> str:
 lines = sql.splitlines
 out: list[str] = []
 in_create = False
 in_tblprops = False
 pending_cols: list[str] = [] # column-list lines (includes closing ')')
 trailing_comments: list[str] = [] # TBLPROPERTIES-as-comments etc
 partition_cols: list[str] = [] # all columns from PARTITIONED BY (...)
 table_comment: str | None = None
 current_table: str | None = None

 # Columns we know are HASH-eligible (DATE / DATETIME2 - fixed-width types).
 # STRING -> NVARCHAR(MAX) columns (record_source, enum-style partitions like
 # contract_type, asset_class) are NOT allowed as HASH in Fabric Warehouse.
 HASH_ELIGIBLE = {
 # DV2 bridge / business-vault partitions
 "trade_date", "report_date", "activity_date", "fact_date", "exposure_date",
 "contractual_settle_date", "actual_settle_date", "snapshot_date",
 "cat_reportable_date", "hour_ts", "effective_start", "effective_end",
 # Expanded-model DATE partitions
 "created_date", "effective_start_date", "event_date", "position_date",
 "submission_date", "expiration_date", "break_detected_date",
 "call_date", "correction_date", "sync_date",
 }

 def flush:
 """Emit the accumulated CREATE TABLE body as T-SQL."""
 nonlocal pending_cols, trailing_comments, partition_cols, table_comment, current_table
 if not pending_cols:
 return
 # Pick a HASH-eligible partition column if available; else ROUND_ROBIN.
 hash_col = next((c for c in partition_cols if c in HASH_ELIGIBLE), None)
 distribution_clause = (f"DISTRIBUTION = HASH({hash_col})" if hash_col else "DISTRIBUTION = ROUND_ROBIN")
 post_table_comments: list[str] = []
 if not hash_col and partition_cols:
 # Note partition cols we skipped for HASH (e.g. NVARCHAR(MAX) record_source)
 post_table_comments.append(f"-- Consider HASH on a fixed-width column; original partition: {', '.join(partition_cols)}")
 if table_comment:
 out.append(f"-- {current_table}: {table_comment}")
 out.extend(pending_cols)
 out.append("WITH (")
 out.append(f" {distribution_clause}")
 out.append(");")
 out.extend(post_table_comments)
 if trailing_comments:
 out.extend(trailing_comments)
 pending_cols = []
 trailing_comments = []
 partition_cols = []
 table_comment = None
 current_table = None

 for raw in lines:
 line = raw

 # Detect table name to carry for comments
 m_create = re.match(r"CREATE TABLE(?: IF NOT EXISTS)?\s+([A-Za-z0-9_\.]+)\s*\(", line)
 if m_create:
 in_create = True
 current_table = m_create.group(1)
 pending_cols.append(line)
 continue

 if in_create:
 # --- capture PARTITIONED BY for DISTRIBUTION hint (all cols)
 m_part = re.match(r"\s*PARTITIONED\s+BY\s*\(([^)]+)\)", line, re.IGNORECASE)
 if m_part:
 partition_cols = [c.strip for c in m_part.group(1).split(",")]
 continue
 # --- capture COMMENT 'x' (table-level)
 m_cmt = re.match(r"\s*COMMENT\s+'([^']*)'\s*$", line, re.IGNORECASE)
 if m_cmt:
 table_comment = m_cmt.group(1)
 continue
 # --- capture TBLPROPERTIES - keep as trailing comment
 if re.match(r"\s*TBLPROPERTIES", line, re.IGNORECASE):
 in_tblprops = True
 trailing_comments.append(re.sub(r"TBLPROPERTIES", "-- TBLPROPERTIES (T-SQL noop):", line))
 if line.rstrip.endswith(";"):
 in_tblprops = False
 flush
 in_create = False
 continue
 if in_tblprops:
 trailing_comments.append("-- " + line.lstrip)
 if line.rstrip.endswith(";"):
 in_tblprops = False
 flush
 in_create = False
 continue
 # --- USING DELTA alone
 if re.match(r"\s*USING\s+DELTA\s*$", line, re.IGNORECASE):
 continue

 # If the column-list closing `)` appears without terminator,
 # it will be captured here. If it's followed immediately by `;`
 # (rare, some single-clause files) treat as end of statement.
 stripped = line.rstrip
 if stripped == ");":
 # self-contained closer - emit as plain `)` and flush
 pending_cols.append(")")
 flush
 in_create = False
 continue

 # --- transform column types inside CREATE TABLE body
 # column-level COMMENT 'x' -> inline /* x */
 line = re.sub(r"\s+COMMENT\s+'([^']*)'", r" /* \1 */", line)
 line = re.sub(r"\bTIMESTAMP\b", "DATETIME2(7)", line)
 line = re.sub(r"\bBOOLEAN\b", "BIT", line)
 line = re.sub(r"\bSTRING\b", "NVARCHAR(MAX)", line)
 line = re.sub(r"DATE'(\d{4}-\d{2}-\d{2})'", r"'\1'", line)
 line = re.sub(r"DEFAULT\s+current_timestamp\(\)",
 "DEFAULT SYSUTCDATETIME",
 line,
 flags=re.IGNORECASE,)
 line = re.sub(r"DEFAULT\s+current_date\(\)",
 "DEFAULT CAST(SYSUTCDATETIME AS DATE)",
 line,
 flags=re.IGNORECASE,)
 line = re.sub(r"GENERATED\s+ALWAYS\s+AS\s*\(\s*current_timestamp\(\)\s*\)",
 "DEFAULT SYSUTCDATETIME",
 line,
 flags=re.IGNORECASE,)
 line = re.sub(r"GENERATED\s+ALWAYS\s+AS\s*\([^)]*\)",
 "",
 line,
 flags=re.IGNORECASE,)

 pending_cols.append(line)
 continue

 # Outside a CREATE TABLE: pass through
 out.append(line)

 flush
 return "\n".join(out)

# =====================================================================
# File orchestration
# =====================================================================
def concat(folder: Path, exclude: set[str]) -> tuple[str, list[str]]:
 buf = []
 files = sorted(f for f in folder.glob("*.sql") if f.name not in exclude)
 for f in files:
 buf.append(f"\n-- @@@ Begin: {f.name} @@@")
 buf.append(f.read_text)
 buf.append(f"-- @@@ End: {f.name} @@@\n")
 return "\n".join(buf), [f.name for f in files]

def main:
 OUT.mkdir(parents=True, exist_ok=True)

 # --- Expanded model (13 files) ---
 src, files = concat(EXPANDED, EXCLUDE_EXPANDED)
 (OUT / "expanded_ddl_fabric_lakehouse.sql").write_text("-- Fabric Lakehouse DDL (Spark SQL-compatible) - generated by _delta_to_fabric.py\n"
 f"-- Source files: {', '.join(files)}\n\n"
 + to_lakehouse(src))
 (OUT / "expanded_ddl_fabric_warehouse.sql").write_text("-- Fabric Warehouse DDL (T-SQL) - generated by _delta_to_fabric.py\n"
 f"-- Source files: {', '.join(files)}\n\n"
 + to_warehouse(src))

 # --- DV2 model (7 files) ---
 src2, files2 = concat(DV2, exclude={"_delta_to_fabric.py"})
 # Skip the markdown mapping file (not.sql) automatically via glob
 (OUT / "dv2_ddl_fabric_lakehouse.sql").write_text("-- Fabric Lakehouse DDL for Data Vault 2.0 - generated by _delta_to_fabric.py\n"
 f"-- Source files: {', '.join(files2)}\n\n"
 + to_lakehouse(src2))
 (OUT / "dv2_ddl_fabric_warehouse.sql").write_text("-- Fabric Warehouse DDL for Data Vault 2.0 - generated by _delta_to_fabric.py\n"
 f"-- Source files: {', '.join(files2)}\n\n"
 + to_warehouse(src2))

 print("[OK] Generated 4 Fabric DDL artifacts in", OUT)

if __name__ == "__main__":
 main
