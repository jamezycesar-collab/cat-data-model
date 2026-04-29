"""DLT pipeline that loads ref_cat_event_type from primary-sources/cat_im_event_types.csv.

The CSV is the verified extract of CAT Industry Member Tech Specs v4.1.0r15
Tables 15, 60, and 61. Loading via CSV (rather than an inline Python list)
prevents drift between the seed data and the spec.

The validate_event_taxonomy.py guardrail compares the CSV against the table
contents in the spec PDF before each release.
"""
import csv
from pathlib import Path

import dlt
from pyspark.sql.types import StructType, StructField, StringType


SCHEMA = StructType([
    StructField("event_code", StringType(), False),
    StructField("event_name", StringType(), False),
    StructField("section", StringType(), False),
    StructField("category", StringType(), False),
    StructField("phase", StringType(), False),
    StructField("description", StringType(), True),
    StructField("source_spec", StringType(), False),
    StructField("source_spec_version", StringType(), False),
])


CAT_SPEC_VERSION = "v4.1.0r15"
SOURCE_CSV = "primary-sources/cat_im_event_types.csv"


def _load_rows():
    csv_path = Path(__file__).resolve().parents[1] / SOURCE_CSV
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Required source file not found: {csv_path}. "
            f"Run validate_event_taxonomy.py to verify primary sources are present."
        )
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            rows.append((
                r["message_type"],
                r["event_name"],
                r["section"],
                r["category"],
                r["phase"],
                r["description"],
                "FINRA CAT IM Tech Specs",
                CAT_SPEC_VERSION,
            ))
    if len(rows) != 99:
        raise ValueError(
            f"Expected 99 CAT event types in v4.1.0r15 spec, got {len(rows)}. "
            f"Update the row count expectation here when a new spec version changes the count."
        )
    return rows


@dlt.table(
    name="ref_cat_event_type",
    comment="Reference table of CAT reportable event types. Sourced from "
            "FINRA CAT IM Technical Specifications {} Tables 15, 60, 61.".format(
                CAT_SPEC_VERSION),
    table_properties={
        "quality": "gold",
        "delta.appendOnly": "false",
    },
)
def ref_cat_event_type():
    rows = _load_rows()
    return spark.createDataFrame(rows, schema=SCHEMA)
