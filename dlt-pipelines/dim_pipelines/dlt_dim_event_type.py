"""Type-0 dim_event_type pipeline. Loads the conformed event taxonomy from the
verified CSV at primary-sources/cat_im_event_types.csv (FINRA CAT IM Tech Specs
v4.1.0r15 Tables 15, 60, 61). The CSV is the source of truth; do not hand-edit
this file with inline event lists.
"""
import csv
from pathlib import Path

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType,
)


SCHEMA = StructType([
    StructField("dim_event_type_sk",     IntegerType(), False),
    StructField("cat_event_code",        StringType(),  False),
    StructField("event_name",            StringType(),  False),
    StructField("event_description",     StringType(),  True),
    StructField("event_phase",           StringType(),  False),
    StructField("submission_file_type",  StringType(),  False),
    StructField("section_number",        StringType(),  False),
    StructField("category",              StringType(),  False),
    StructField("is_manual",             BooleanType(), False),
    StructField("is_electronic",         BooleanType(), False),
    StructField("is_options",            BooleanType(), False),
    StructField("is_multileg",           BooleanType(), False),
    StructField("is_reportable",         BooleanType(), False),
    StructField("source_spec_version",   StringType(),  False),
])

CAT_SPEC_VERSION = "v4.1.0r15"
SOURCE_CSV = "primary-sources/cat_im_event_types.csv"

# Map CSV category to dim_event_type compatibility columns.
def _category_to_flags(category: str) -> tuple[bool, bool, bool, bool]:
    is_options  = "OPTION" in category
    is_multileg = "MULTILEG" in category
    is_manual   = False  # spec uses manualFlag field on the event, not a separate code
    is_electronic = True
    return is_manual, is_electronic, is_options, is_multileg


def _csv_to_rows():
    csv_path = Path(__file__).resolve().parents[2] / SOURCE_CSV
    if not csv_path.exists():
        raise FileNotFoundError(
            f"{csv_path} not found. Run guardrails/validate_event_taxonomy.py "
            f"to verify primary sources are present."
        )

    rows = []
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for sk, r in enumerate(reader, start=1):
            is_manual, is_electronic, is_options, is_multileg = _category_to_flags(r["category"])
            file_type = "OptionEvents" if is_options else "OrderEvents"
            rows.append((
                sk,
                r["message_type"],
                r["event_name"],
                r["description"],
                r["phase"],
                file_type,
                r["section"],
                r["category"],
                is_manual,
                is_electronic,
                is_options,
                is_multileg,
                True,           # is_reportable
                CAT_SPEC_VERSION,
            ))

    if len(rows) != 99:
        raise ValueError(
            f"Expected 99 CAT event types in v4.1.0r15 spec, got {len(rows)}. "
            f"Update spec_pins.json and the validator before changing this expectation."
        )
    return rows


@dlt.expect_all_or_fail({
    "exactly_99_events": "(SELECT COUNT(*) FROM dim_event_type) = 99",
    "cat_event_code_unique": "cat_event_code IS NOT NULL",
    "valid_section": "section_number IS NOT NULL",
    "submission_file_type_enum":
        "submission_file_type IN ('OrderEvents','OptionEvents','QuoteEvents','Allocations','CAIS')",
})
@dlt.table(
    name="dim_event_type",
    comment=f"Conformed CAT event-type dimension. Loaded from primary-sources CSV "
            f"matched to FINRA CAT IM Tech Specs {CAT_SPEC_VERSION}. 99 rows.",
    table_properties={
        "delta.enableChangeDataFeed": "false",
        "scd_type": "0",
        "subject_area": "gold_dim",
        "cat_spec_version": CAT_SPEC_VERSION,
        "row_count_invariant": "99",
    },
)
def dim_event_type():
    rows = _csv_to_rows()
    return (
        spark.createDataFrame(rows, schema=SCHEMA)
             .withColumn("load_date", F.current_timestamp())
             .withColumn("record_source", F.lit(f"primary-sources/cat_im_event_types.csv@{CAT_SPEC_VERSION}"))
    )
