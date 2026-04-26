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

"""Date spine generator. One row per calendar date with fiscal and business-day attributes."""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DateType

# ---------------------------------------------------------------------------
# NYSE holiday set (to refresh annually via reference-data pipeline)
# ---------------------------------------------------------------------------
NYSE_HOLIDAYS = {
 # 2024
 "2024-01-01": "New Year's Day",
 "2024-01-15": "Martin Luther King Jr. Day",
 "2024-02-19": "Presidents Day",
 "2024-03-29": "Good Friday",
 "2024-05-27": "Memorial Day",
 "2024-06-19": "Juneteenth",
 "2024-07-04": "Independence Day",
 "2024-09-02": "Labor Day",
 "2024-11-28": "Thanksgiving Day",
 "2024-12-25": "Christmas Day",
 # 2025
 "2025-01-01": "New Year's Day",
 "2025-01-20": "Martin Luther King Jr. Day",
 "2025-02-17": "Presidents Day",
 "2025-04-18": "Good Friday",
 "2025-05-26": "Memorial Day",
 "2025-06-19": "Juneteenth",
 "2025-07-04": "Independence Day",
 "2025-09-01": "Labor Day",
 "2025-11-27": "Thanksgiving Day",
 "2025-12-25": "Christmas Day",
 # 2026
 "2026-01-01": "New Year's Day",
 "2026-01-19": "Martin Luther King Jr. Day",
 "2026-02-16": "Presidents Day",
 "2026-04-03": "Good Friday",
 "2026-05-25": "Memorial Day",
 "2026-06-19": "Juneteenth",
 "2026-07-03": "Independence Day (observed)",
 "2026-09-07": "Labor Day",
 "2026-11-26": "Thanksgiving Day",
 "2026-12-25": "Christmas Day",
}

@dlt.table(
 name="dim_date",
 comment="Static calendar dimension with NYSE / DTCC / CAT deadline columns",
 table_properties={
 "delta.enableChangeDataFeed": "false",
 "scd_type": "0",
 "subject_area": "gold_dim",
 },
)
def dim_date:
 start_date = "2020-01-01"
 end_date = "2035-12-31"

 holidays_df = (
 spark.createDataFrame(
 [(k, v) for k, v in NYSE_HOLIDAYS.items],
 schema="calendar_date STRING, holiday_name STRING",
).withColumn("calendar_date", F.to_date("calendar_date"))
)

 dates_df = (
 spark.sql(
 f"SELECT explode(sequence(to_date('{start_date}'), "
 f"to_date('{end_date}'), interval 1 day)) AS calendar_date"
).join(holidays_df, "calendar_date", "left").withColumn("date_sk", F.date_format("calendar_date", "yyyyMMdd").cast("int")).withColumn("day_of_week", F.dayofweek("calendar_date")).withColumn("day_name", F.date_format("calendar_date", "EEEE")).withColumn("day_of_month", F.dayofmonth("calendar_date")).withColumn("day_of_year", F.dayofyear("calendar_date")).withColumn("week_of_year", F.weekofyear("calendar_date")).withColumn("month_number", F.month("calendar_date")).withColumn("month_name", F.date_format("calendar_date", "MMMM")).withColumn("quarter", F.quarter("calendar_date")).withColumn("year_number", F.year("calendar_date")).withColumn("fiscal_year", F.year("calendar_date")).withColumn("fiscal_quarter", F.quarter("calendar_date")).withColumn("holiday_flag", F.col("holiday_name").isNotNull).withColumn(
 "is_trading_day",
 (F.dayofweek("calendar_date").between(2, 6)) & (~F.col("holiday_flag")),
).withColumn("is_settlement_day", F.col("is_trading_day")).withColumn(
 "t_plus_1_settlement_date",
 F.expr("date_add(calendar_date, 1)"),
).withColumn(
 "t_plus_3_error_correction_deadline",
 F.expr("date_add(calendar_date, 3)"),
).withColumn(
 "cat_submission_deadline_timestamp",
 F.to_timestamp(
 F.concat(
 F.date_format(F.expr("date_add(calendar_date, 1)"), "yyyy-MM-dd"),
 F.lit(" 08:00:00"),
)
),
)
)

 return dates_df.select(
 "date_sk",
 "calendar_date",
 "day_of_week",
 "day_name",
 "day_of_month",
 "day_of_year",
 "week_of_year",
 "month_number",
 "month_name",
 "quarter",
 "year_number",
 "fiscal_year",
 "fiscal_quarter",
 "is_trading_day",
 "is_settlement_day",
 "t_plus_1_settlement_date",
 "t_plus_3_error_correction_deadline",
 "cat_submission_deadline_timestamp",
 "holiday_flag",
 "holiday_name",
)

@dlt.expect_all_or_fail(
 {
 "date_sk_format_valid": "date_sk BETWEEN 20200101 AND 20351231",
 "date_sk_matches_calendar": "date_sk = cast(date_format(calendar_date, 'yyyyMMdd') AS INT)",
 "trading_days_not_weekend": "NOT (is_trading_day = TRUE AND day_of_week IN (1, 7))",
 }
)
@dlt.table(
 name="dim_date_validated",
 comment="dim_date with hard-fail expectations - blocks the pipeline on violation",
)
def dim_date_validated:
 return dlt.read("dim_date")
