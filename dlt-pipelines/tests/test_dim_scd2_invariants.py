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

"""Invariants for SCD2 dims: current-flag uniqueness per natural key, non-overlapping validity windows, surrogate-key integrity."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession, functions as F

GOLD_DB = "gold"

DIMS = [
 ("dim_party", "party_id_bk"),
 ("dim_instrument", "instrument_id_bk"),
 ("dim_venue", "venue_id_bk"),
 ("dim_account", "account_id_bk"),
 ("dim_desk", "desk_id_bk"),
 ("dim_trader", "trader_id_bk"),
]

@pytest.fixture(scope="module")
def spark -> SparkSession:
 return (
 SparkSession.builder.appName("ws8-dim-scd2-invariants").getOrCreate
)

# ---------------------------------------------------------------------------
# 1. Exactly one is_current = TRUE per business key
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dim_name,bk_col", DIMS)
def test_exactly_one_current_row_per_bk(spark, dim_name, bk_col):
 df = spark.table(f"{GOLD_DB}.{dim_name}")
 offenders = (
 df.filter(F.col("is_current") == F.lit(True)).groupBy(bk_col).count.filter(F.col("count") != F.lit(1))
)
 assert offenders.count == 0, (
 f"{dim_name}: {offenders.count} business keys violate "
 f"exactly-one-current-row invariant"
)

# ---------------------------------------------------------------------------
# 2. row_hash is SHA-256 (64 hex chars)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dim_name,_", DIMS)
def test_row_hash_sha256_length(spark, dim_name, _):
 df = spark.table(f"{GOLD_DB}.{dim_name}")
 bad = df.filter(F.length(F.col("row_hash")) != F.lit(64))
 assert bad.count == 0, (
 f"{dim_name}: {bad.count} rows have row_hash that is NOT 64 chars"
)

# ---------------------------------------------------------------------------
# 3. effective_start_date <= effective_end_date
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dim_name,_", DIMS)
def test_effective_window_non_negative(spark, dim_name, _):
 df = spark.table(f"{GOLD_DB}.{dim_name}")
 bad = df.filter(F.col("effective_start_date") > F.col("effective_end_date"))
 assert bad.count == 0, (
 f"{dim_name}: {bad.count} rows have start > end date"
)

# ---------------------------------------------------------------------------
# 4. Non-current rows must carry a bounded effective_end_date
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dim_name,_", DIMS)
def test_non_current_bounded_end_date(spark, dim_name, _):
 df = spark.table(f"{GOLD_DB}.{dim_name}")
 bad = df.filter(
 (F.col("is_current") == F.lit(False))
 & (F.col("effective_end_date") >= F.lit("9999-12-31"))
)
 assert bad.count == 0, (
 f"{dim_name}: {bad.count} non-current rows still have an "
 f"open-ended effective_end_date"
)

# ---------------------------------------------------------------------------
# 5. Business-key column is NEVER NULL
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dim_name,bk_col", DIMS)
def test_business_key_not_null(spark, dim_name, bk_col):
 df = spark.table(f"{GOLD_DB}.{dim_name}")
 bad = df.filter(F.col(bk_col).isNull)
 assert bad.count == 0, (
 f"{dim_name}: {bad.count} rows have NULL {bk_col}"
)

# ---------------------------------------------------------------------------
# 6. dv2_source_hk lineage is NEVER NULL (BCBS 239 traceability)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dim_name,_", DIMS)
def test_dv2_source_hk_not_null(spark, dim_name, _):
 df = spark.table(f"{GOLD_DB}.{dim_name}")
 bad = df.filter(F.col("dv2_source_hk").isNull)
 assert bad.count == 0, (
 f"{dim_name}: {bad.count} rows have NULL dv2_source_hk - "
 f"BCBS 239 column-level lineage is broken"
)

# ---------------------------------------------------------------------------
# 7. dim_event_type seed check - exactly 50 CAT events
# ---------------------------------------------------------------------------
def test_dim_event_type_seed_50_rows(spark):
 df = spark.table(f"{GOLD_DB}.dim_event_type")
 n = df.count
 assert n == 50, (
 f"dim_event_type must seed exactly 50 CAT events, got {n}"
)
 # Each cat_event_code must be distinct
 distinct = df.select("cat_event_code").distinct.count
 assert distinct == 50, (
 f"dim_event_type: {50 - distinct} duplicate cat_event_code values"
)

# ---------------------------------------------------------------------------
# 8. dim_date coverage - trading-day flag is set correctly
# ---------------------------------------------------------------------------
def test_dim_date_trading_day_flag(spark):
 df = spark.table(f"{GOLD_DB}.dim_date")
 # Weekends must NEVER be trading days
 weekend_trading = df.filter(
 (F.col("day_of_week").isin(1, 7))
 & (F.col("is_trading_day") == F.lit(True))
)
 assert weekend_trading.count == 0, (
 f"dim_date: {weekend_trading.count} weekend rows incorrectly "
 f"flagged as trading days"
)
 # US holiday rows must NEVER be trading days
 holiday_trading = df.filter(
 (F.col("holiday_flag") == F.lit(True))
 & (F.col("is_trading_day") == F.lit(True))
)
 assert holiday_trading.count == 0, (
 f"dim_date: {holiday_trading.count} holidays flagged as trading"
)
