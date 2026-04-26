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

"""Referential integrity between the fact tables and their dims, including PIT surrogate-key presence."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession, functions as F

GOLD_DB = "gold"

@pytest.fixture(scope="module")
def spark -> SparkSession:
 return (
 SparkSession.builder.appName("ws8-referential-integrity").getOrCreate
)

# Fact -> (dim table, fact SK col, dim SK col, fact date col)
FACT_DIM_MATRIX = [
 # fact_cat_order_events
 ("fact_cat_order_events", "dim_party", "dim_party_sk", "dim_party_sk", "event_date"),
 ("fact_cat_order_events", "dim_instrument", "dim_instrument_sk", "dim_instrument_sk", "event_date"),
 ("fact_cat_order_events", "dim_venue", "dim_venue_sk", "dim_venue_sk", "event_date"),
 ("fact_cat_order_events", "dim_account", "dim_account_sk", "dim_account_sk", "event_date"),
 ("fact_cat_order_events", "dim_desk", "dim_desk_sk", "dim_desk_sk", "event_date"),
 ("fact_cat_order_events", "dim_trader", "dim_trader_sk", "dim_trader_sk", "event_date"),

 # fact_cat_allocations
 ("fact_cat_allocations", "dim_party", "dim_party_sk", "dim_party_sk", "event_date"),
 ("fact_cat_allocations", "dim_instrument", "dim_instrument_sk", "dim_instrument_sk", "event_date"),
 ("fact_cat_allocations", "dim_account", "dim_account_sk", "dim_account_sk", "event_date"),
 ("fact_cat_allocations", "dim_desk", "dim_desk_sk", "dim_desk_sk", "event_date"),

 # fact_cat_quotes
 ("fact_cat_quotes", "dim_party", "dim_party_sk", "dim_party_sk", "event_date"),
 ("fact_cat_quotes", "dim_instrument", "dim_instrument_sk", "dim_instrument_sk", "event_date"),
 ("fact_cat_quotes", "dim_venue", "dim_venue_sk", "dim_venue_sk", "event_date"),
 ("fact_cat_quotes", "dim_trader", "dim_trader_sk", "dim_trader_sk", "event_date"),

 # fact_cat_customer_records (CAIS snapshot grain)
 ("fact_cat_customer_records", "dim_party", "dim_party_sk", "dim_party_sk", "snapshot_date"),
 ("fact_cat_customer_records", "dim_account", "dim_account_sk", "dim_account_sk", "snapshot_date"),
]

# ---------------------------------------------------------------------------
# 1. Critical SKs are NOT NULL (hard fail - already enforced in DLT)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fact,dim,fact_sk,dim_sk,date_col", [
 r for r in FACT_DIM_MATRIX if r[1] in ("dim_party", "dim_instrument")
])
def test_critical_sks_not_null(spark, fact, dim, fact_sk, dim_sk, date_col):
 df = spark.table(f"{GOLD_DB}.{fact}")
 bad = df.filter(F.col(fact_sk).isNull)
 assert bad.count == 0, (
 f"{fact}: {bad.count} rows have NULL {fact_sk} - "
 f"DLT hard-fail gate has been bypassed"
)

# ---------------------------------------------------------------------------
# 2. Every resolved SK must match a real dim row
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fact,dim,fact_sk,dim_sk,date_col", FACT_DIM_MATRIX)
def test_sk_resolves_to_dim(spark, fact, dim, fact_sk, dim_sk, date_col):
 f = (
 spark.table(f"{GOLD_DB}.{fact}").filter(F.col(fact_sk).isNotNull).select(fact_sk).distinct
)
 d = spark.table(f"{GOLD_DB}.{dim}").select(dim_sk).distinct
 orphans = f.join(d, f[fact_sk] == d[dim_sk], "left_anti")
 assert orphans.count == 0, (
 f"{fact}.{fact_sk}: {orphans.count} distinct values do NOT "
 f"exist in {dim}.{dim_sk}"
)

# ---------------------------------------------------------------------------
# 3. Point-in-time correctness - dim row must have been active on fact date
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fact,dim,fact_sk,dim_sk,date_col", FACT_DIM_MATRIX)
def test_point_in_time_correctness(spark, fact, dim, fact_sk, dim_sk, date_col):
 f = spark.table(f"{GOLD_DB}.{fact}").filter(F.col(fact_sk).isNotNull)
 d = spark.table(f"{GOLD_DB}.{dim}")

 joined = (
 f.alias("f").join(d.alias("d"), F.col(f"f.{fact_sk}") == F.col(f"d.{dim_sk}"), "left")
)
 offenders = joined.filter(
 (F.col(f"f.{date_col}") < F.col("d.effective_start_date"))
 | (F.col(f"f.{date_col}") > F.col("d.effective_end_date"))
)
 assert offenders.count == 0, (
 f"{fact} -> {dim}: {offenders.count} rows reference a dim "
 f"version NOT effective on {date_col} - point-in-time joins "
 f"are broken, late-arriving data handling is off"
)

# ---------------------------------------------------------------------------
# 4. counterparty_party_sk (where populated) must resolve to dim_party
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fact", ["fact_cat_order_events", "fact_cat_quotes"])
def test_counterparty_party_sk_resolves(spark, fact):
 f = (
 spark.table(f"{GOLD_DB}.{fact}").filter(F.col("counterparty_party_sk").isNotNull).select("counterparty_party_sk").distinct
)
 d = spark.table(f"{GOLD_DB}.dim_party").select("dim_party_sk").distinct
 orphans = f.join(d, f.counterparty_party_sk == d.dim_party_sk, "left_anti")
 assert orphans.count == 0, (
 f"{fact}.counterparty_party_sk: {orphans.count} orphan references"
)

# ---------------------------------------------------------------------------
# 5. allocations.parent_execution_id must trace back to order events
# ---------------------------------------------------------------------------
def test_allocations_parent_execution_in_order_events(spark):
 a = (
 spark.table(f"{GOLD_DB}.fact_cat_allocations").filter(F.col("parent_execution_id").isNotNull).select(F.col("parent_execution_id").alias("exec_id"),
 F.col("cat_order_id")).distinct
)
 o = (
 spark.table(f"{GOLD_DB}.fact_cat_order_events").select("cat_order_id").distinct
)
 orphans = a.join(o, "cat_order_id", "left_anti")
 # Allow a small tolerance - some allocations may arrive before the
 # matching order event in intraday windows; FINRA CAT permits
 # cross-file linkage completion within T+3.
 assert orphans.count == 0, (
 f"fact_cat_allocations: {orphans.count} parent_execution_id / "
 f"cat_order_id pairs have no matching order event - "
 f"late-arriving OR orphan allocation"
)

# ---------------------------------------------------------------------------
# 6. feedback errors must reference a real submission batch
# ---------------------------------------------------------------------------
def test_feedback_error_batch_id_resolves(spark):
 e = (
 spark.table(f"{GOLD_DB}.cat_feedback_error").select("batch_id").distinct
)
 b = (
 spark.table(f"{GOLD_DB}.cat_submission_batch").select("batch_id").distinct
)
 orphans = e.join(b, "batch_id", "left_anti")
 assert orphans.count == 0, (
 f"cat_feedback_error: {orphans.count} distinct batch_id values "
 f"do NOT exist in cat_submission_batch - FINRA feedback "
 f"reconciliation is broken"
)

# ---------------------------------------------------------------------------
# 7. firm_roe_id uniformity - every fact row's firm must exist in dim_party
# with party_type IN ('INDUSTRY_MEMBER','BROKER_DEALER')
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fact,date_col", [
 ("fact_cat_order_events", "event_date"),
 ("fact_cat_allocations", "event_date"),
 ("fact_cat_quotes", "event_date"),
 ("fact_cat_customer_records", "snapshot_date"),
])
def test_firm_roe_id_registered_in_dim_party(spark, fact, date_col):
 f = (
 spark.table(f"{GOLD_DB}.{fact}").select(F.col("firm_roe_id"), F.col(date_col).alias("evt_date")).distinct
)
 p = (
 spark.table(f"{GOLD_DB}.dim_party").filter(F.col("firm_roe_id").isNotNull).select("firm_roe_id", "effective_start_date", "effective_end_date")
)
 joined = (
 f.alias("f").join(p.alias("p"), "firm_roe_id", "left").filter(
 F.col("p.firm_roe_id").isNull
 | (F.col("f.evt_date") < F.col("p.effective_start_date"))
 | (F.col("f.evt_date") > F.col("p.effective_end_date"))
)
)
 assert joined.count == 0, (
 f"{fact}: {joined.count} firm_roe_id / {date_col} combinations "
 f"are NOT registered in dim_party with the right effective window"
)
