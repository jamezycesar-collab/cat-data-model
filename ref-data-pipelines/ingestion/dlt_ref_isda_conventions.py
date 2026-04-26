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

"""Ingest ISDA day-count conventions into ref_day_count_convention."""
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType

ISDA_VERSION = "ISDA 2021 Definitions"
ISDA_URL = "https://www.isda.org/book/2021-isda-interest-rate-derivatives-definitions/"

DAY_COUNTS = [
 # (code, name, description, category, isda_code, fpml_code, numerator, denominator, typical_usage)
 ("ACT_360", "Actual/360", "Actual days in accrual / 360", "ACTUAL", "ACT/360", "ACT/360", "ACTUAL", "360", "USD money market, USD LIBOR, EUR money market"),
 ("ACT_365_FIXED", "Actual/365 (Fixed)", "Actual days in accrual / 365 (fixed)", "ACTUAL", "ACT/365 FIXED","ACT/365.FIXED","ACTUAL", "365", "GBP money market, GBP LIBOR, JPY LIBOR"),
 ("ACT_365_25", "Actual/365.25", "Actual days in accrual / 365.25 (Julian year)", "ACTUAL", "ACT/365.25", "ACT/365.25", "ACTUAL", "365.25", "Legacy inflation-linked bonds"),
 ("ACT_ACT_ISDA", "Actual/Actual ISDA", "Actual days in accrual / actual days in year (365 or 366)", "ACTUAL", "ACT/ACT", "ACT/ACT.ISDA", "ACTUAL", "ACTUAL", "US Treasury bonds, USD swaps"),
 ("ACT_ACT_ICMA", "Actual/Actual ICMA", "ICMA rule 251 - actual / periodic frequency", "ACTUAL", "ACT/ACT.ICMA", "ACT/ACT.ICMA", "ACTUAL", "ACTUAL", "Eurobonds, Italian government bonds"),
 ("30_360", "30/360 Bond Basis", "30-day month / 360-day year (US bond basis)", "30_360", "30/360", "30/360", "30", "360", "US corporate bonds, US muni bonds"),
 ("30E_360", "30E/360 Eurobond", "European 30/360 (30E / 360)", "30_360", "30E/360", "30E/360", "30E", "360", "Eurobonds, German bunds"),
 ("30E_360_ISDA", "30E/360 ISDA", "ISDA 30E/360 (German method, EOM handling)", "30_360", "30E/360.ISDA", "30E/360.ISDA", "30E", "360", "EUR swaps, German corporate bonds"),
 ("30U_360", "30U/360 US", "US 30/360 with EOM rule", "30_360", "30U/360", "30U/360", "30U", "360", "US corporate bonds (SIA standard)"),
 ("BUS_252", "Business/252", "Business days / 252 (Brazilian convention)", "BUSINESS_252", "BUS/252", "BUS/252", "ACTUAL", "252", "Brazilian BRL markets, CDI rate"),
 ("1_1", "1/1", "One period per one period (zero-coupon)", "ACTUAL", "1/1", "1/1", "ACTUAL", "ACTUAL", "Zero-coupon notes"),
 ("ACT_365_L", "Actual/365 Long", "Actual / 365 unless period crosses leap year (then 366)", "ACTUAL", "ACT/365L", "ACT/365.L", "ACTUAL", "365", "UK gilts, some money-market"),
 ("ACT_ACT_AFB", "Actual/Actual AFB", "French AFB method (actual / 365 or 366 based on leap)", "ACTUAL", "ACT/ACT.AFB", "ACT/ACT.AFB", "ACTUAL", "ACTUAL", "French government bonds (OATs)"),
 ("NL_365", "NL/365", "Actual / 365 ignoring Feb 29", "ACTUAL", "NL/365", "NL/365", "ACTUAL", "365", "Select Asian money-markets"),
 ("SIMPLE", "Simple", "Simple arithmetic (deprecated, maintained for legacy CSAs)", "ACTUAL", "SIMPLE", "SIMPLE", "ACTUAL", "ACTUAL", "Legacy ISDA CSA collateral"),
]

SCHEMA = StructType([
 StructField("day_count_code", StringType, False),
 StructField("day_count_name", StringType, False),
 StructField("day_count_description", StringType, True),
 StructField("category", StringType, True),
 StructField("isda_code", StringType, True),
 StructField("fpml_code", StringType, True),
 StructField("numerator_method", StringType, True),
 StructField("denominator_method", StringType, True),
 StructField("typical_usage", StringType, True),
])

@dlt.view(name="v_day_count_seed")
def v_day_count_seed:
 df = spark.createDataFrame(DAY_COUNTS, schema=SCHEMA)
 return (
 df.withColumn("effective_start_date", F.current_date).withColumn("effective_end_date", F.lit("9999-12-31").cast(DateType)).withColumn("is_active", F.lit(True)).withColumn("source_authority", F.lit("ISDA")).withColumn("source_version", F.lit(ISDA_VERSION)).withColumn("last_updated_date", F.current_date).withColumn("record_source", F.lit(ISDA_URL)).withColumn("load_date", F.current_timestamp).withColumn("cdc_sequence", F.col("load_date").cast("long")).withColumn("cdc_operation", F.lit("UPSERT")).withColumn("row_hash",
 F.sha2(F.concat_ws("|",
 F.col("day_count_code"), F.col("day_count_name"),
 F.coalesce(F.col("category"), F.lit("")),
 F.coalesce(F.col("isda_code"), F.lit("")),
 F.coalesce(F.col("fpml_code"), F.lit("")),
 F.coalesce(F.col("numerator_method"), F.lit("")),
 F.coalesce(F.col("denominator_method"), F.lit("")),
), 256))
)

@dlt.expect_all_or_fail({
 "day_count_upper": "day_count_code RLIKE '^[A-Z0-9_]{2,40}$'",
 "category_enum": "category IN ('ACTUAL','30_360','BUSINESS_252')",
 "isda_not_null": "isda_code IS NOT NULL",
})
@dlt.view(name="v_day_count_validated")
def v_day_count_validated:
 return dlt.read("v_day_count_seed")

dlt.create_streaming_table(
 name="ref_day_count",
 comment="ISDA 2021 day-count conventions - 15 canonical codes",
 table_properties={"delta.enableChangeDataFeed": "true", "subject_area": "reference", "scd_type": "2"},
 partition_cols=["is_active"],
)

dlt.apply_changes(
 target="ref_day_count",
 source="v_day_count_validated",
 keys=["day_count_code"],
 sequence_by=F.col("cdc_sequence"),
 stored_as_scd_type=2,
 apply_as_deletes=F.expr("cdc_operation = 'DELETE'"),
 except_column_list=["cdc_operation", "cdc_sequence"],
)
