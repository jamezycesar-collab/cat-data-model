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

"""Quality gates on the CAT facts: nullability on required fields, referential checks against dims, event-type cardinality."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession, functions as F

GOLD_DB = "gold"

EVENT_GRAIN_FACTS = [
 "fact_cat_order_events",
 "fact_cat_allocations",
 "fact_cat_quotes",
]

SNAPSHOT_GRAIN_FACTS = [
 "fact_cat_customer_records",
]

ALL_FACTS = EVENT_GRAIN_FACTS + SNAPSHOT_GRAIN_FACTS

@pytest.fixture(scope="module")
def spark() -> SparkSession:
 return (
 SparkSession.builder.appName("ws8-fact-quality-gates").getOrCreate()
)

# ---------------------------------------------------------------------------
# 1. firm_roe_id IS NOT NULL (every fact, every row)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fact_name", ALL_FACTS)
def test_firm_roe_id_not_null(spark, fact_name):
 df = spark.table(f"{GOLD_DB}.{fact_name}")
 bad = df.filter(F.col("firm_roe_id").isNull)
 assert bad.count == 0, (
 f"{fact_name}: {bad.count} rows have NULL firm_roe_id - "
 f"CAT submission will be rejected by FINRA"
)

# ---------------------------------------------------------------------------
# 2. dv2_source_hk IS NOT NULL (BCBS 239)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fact_name", ALL_FACTS)
def test_dv2_source_hk_not_null(spark, fact_name):
 df = spark.table(f"{GOLD_DB}.{fact_name}")
 bad = df.filter(F.col("dv2_source_hk").isNull)
 assert bad.count == 0, (
 f"{fact_name}: {bad.count} rows have NULL dv2_source_hk - "
 f"BCBS 239 column-level lineage is broken"
)

# ---------------------------------------------------------------------------
# 3. cat_event_code IS NOT NULL
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fact_name", ALL_FACTS)
def test_cat_event_code_not_null(spark, fact_name):
 df = spark.table(f"{GOLD_DB}.{fact_name}")
 bad = df.filter(F.col("cat_event_code").isNull)
 assert bad.count == 0, (
 f"{fact_name}: {bad.count} rows missing cat_event_code"
)

# ---------------------------------------------------------------------------
# 4. Event-grain facts: event_timestamp + event_date populated
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fact_name", EVENT_GRAIN_FACTS)
def test_event_grain_timestamps(spark, fact_name):
 df = spark.table(f"{GOLD_DB}.{fact_name}")

 # Every event-grain fact carries an event_date partition column
 no_date = df.filter(F.col("event_date").isNull)
 assert no_date.count == 0, (
 f"{fact_name}: {no_date.count} rows missing event_date"
)

 # Pick the right timestamp column based on the fact
 ts_col_map = {
 "fact_cat_order_events": "event_timestamp",
 "fact_cat_allocations": "allocation_timestamp",
 "fact_cat_quotes": "quote_timestamp",
 }
 ts_col = ts_col_map[fact_name]
 no_ts = df.filter(F.col(ts_col).isNull)
 assert no_ts.count == 0, (
 f"{fact_name}: {no_ts.count} rows missing {ts_col}"
)

# ---------------------------------------------------------------------------
# 5. CAIS snapshot grain: snapshot_date populated
# ---------------------------------------------------------------------------
def test_cais_snapshot_date_populated(spark):
 df = spark.table(f"{GOLD_DB}.fact_cat_customer_records")
 bad = df.filter(F.col("snapshot_date").isNull)
 assert bad.count == 0, (
 f"fact_cat_customer_records: {bad.count} CAIS rows missing "
 f"snapshot_date - daily snapshot grain is broken"
)

# ---------------------------------------------------------------------------
# 6. Every cat_event_code MUST exist in dim_event_type seed
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fact_name", ALL_FACTS)
def test_cat_event_code_in_dim_event_type(spark, fact_name):
 fact = spark.table(f"{GOLD_DB}.{fact_name}").select("cat_event_code").distinct
 dim = spark.table(f"{GOLD_DB}.dim_event_type").select("cat_event_code").distinct

 orphans = fact.join(dim, "cat_event_code", "left_anti")
 assert orphans.count == 0, (
 f"{fact_name}: {orphans.count} distinct cat_event_code values "
 f"are not registered in dim_event_type seed of 50"
)

# ---------------------------------------------------------------------------
# 7. Partition alignment - event_date matches DATE(event_timestamp)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fact_name,ts_col", [
 ("fact_cat_order_events", "event_timestamp"),
 ("fact_cat_allocations", "allocation_timestamp"),
 ("fact_cat_quotes", "quote_timestamp"),
])
def test_partition_date_alignment(spark, fact_name, ts_col):
 df = spark.table(f"{GOLD_DB}.{fact_name}")
 mis = df.filter(F.col("event_date") != F.to_date(F.col(ts_col)))
 assert mis.count == 0, (
 f"{fact_name}: {mis.count} rows have event_date ≠ DATE({ts_col}) - "
 f"partition pruning will be wrong for CAT submissions"
)

# ---------------------------------------------------------------------------
# 8. load_date populated (ETL audit)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fact_name", ALL_FACTS)
def test_load_date_populated(spark, fact_name):
 df = spark.table(f"{GOLD_DB}.{fact_name}")
 bad = df.filter(F.col("load_date").isNull)
 assert bad.count == 0, (
 f"{fact_name}: {bad.count} rows missing load_date"
)

# ---------------------------------------------------------------------------
# 9. CAT event taxonomy alignment - event phase matches file type
# ---------------------------------------------------------------------------
def test_order_events_file_type_alignment(spark):
 """
 OrderEvents file should never carry Allocation or Quote events.
 """
 fact = spark.table(f"{GOLD_DB}.fact_cat_order_events")
 dim = spark.table(f"{GOLD_DB}.dim_event_type")

 joined = (
 fact.alias("f").join(dim.alias("d"), "cat_event_code", "left").select(F.col("d.submission_file_type").alias("file_type")).distinct
)
 offenders = joined.filter(
 ~F.col("file_type").isin("OrderEvents", "OptionsExtensions")
)
 assert offenders.count == 0, (
 f"fact_cat_order_events contains events from non-OrderEvents "
 f"file types: {[r.file_type for r in offenders.collect]}"
)

def test_quotes_file_type_alignment(spark):
 fact = spark.table(f"{GOLD_DB}.fact_cat_quotes")
 dim = spark.table(f"{GOLD_DB}.dim_event_type")
 joined = (
 fact.alias("f").join(dim.alias("d"), "cat_event_code", "left").select(F.col("d.submission_file_type").alias("file_type")).distinct
)
 offenders = joined.filter(F.col("file_type") != F.lit("QuoteEvents"))
 assert offenders.count == 0, (
 f"fact_cat_quotes contains non-QuoteEvents file types: "
 f"{[r.file_type for r in offenders.collect]}"
)

def test_allocations_file_type_alignment(spark):
 fact = spark.table(f"{GOLD_DB}.fact_cat_allocations")
 dim = spark.table(f"{GOLD_DB}.dim_event_type")
 joined = (
 fact.alias("f").join(dim.alias("d"), "cat_event_code", "left").select(F.col("d.submission_file_type").alias("file_type")).distinct
)
 offenders = joined.filter(F.col("file_type") != F.lit("Allocations"))
 assert offenders.count == 0, (
 f"fact_cat_allocations contains non-Allocations file types: "
 f"{[r.file_type for r in offenders.collect]}"
)

def test_cais_file_type_alignment(spark):
 fact = spark.table(f"{GOLD_DB}.fact_cat_customer_records")
 dim = spark.table(f"{GOLD_DB}.dim_event_type")
 joined = (
 fact.alias("f").join(dim.alias("d"), "cat_event_code", "left").select(F.col("d.submission_file_type").alias("file_type")).distinct
)
 offenders = joined.filter(F.col("file_type") != F.lit("CAIS"))
 assert offenders.count == 0, (
 f"fact_cat_customer_records contains non-CAIS file types: "
 f"{[r.file_type for r in offenders.collect]}"
)
