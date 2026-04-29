"""DLT pipeline that loads CAIS reference enumerations.

CAIS (Customer & Account Information System) is file-based, not event-based.
The reportable units are CAIS Data Files and Transformed Identifiers Files
containing FDID Records, Customer Records, Address Records, and Large Trader
Records. The taxonomic structure is captured here as a set of enumerations
keyed on the CAIS field that constrains them.

Source: Full CAIS Technical Specifications v2.2.0r4. The CSV
primary-sources/cais_enumerations.csv is the verified extract from
Sections 4.1 and 5.1.3.
"""
import csv
from pathlib import Path

import dlt
from pyspark.sql.types import StructType, StructField, StringType


SCHEMA = StructType([
    StructField("enum_field", StringType(), False),
    StructField("enum_value", StringType(), False),
    StructField("description", StringType(), True),
    StructField("source_spec", StringType(), False),
    StructField("source_spec_version", StringType(), False),
])


CAIS_SPEC_VERSION = "v2.2.0r4"
SOURCE_CSV = "primary-sources/cais_enumerations.csv"


def _load_rows():
    csv_path = Path(__file__).resolve().parents[1] / SOURCE_CSV
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Required source file not found: {csv_path}."
        )
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        rows = [
            (r["enum_field"], r["enum_value"], r["description"],
             "FINRA CAT CAIS Tech Specs", CAIS_SPEC_VERSION)
            for r in reader
        ]
    return rows


@dlt.table(
    name="ref_cais_enumeration",
    comment="Reference table of CAIS submission enumerations. Sourced from "
            "Full CAIS Technical Specifications {}.".format(CAIS_SPEC_VERSION),
    table_properties={
        "quality": "gold",
        "delta.appendOnly": "false",
    },
)
def ref_cais_enumeration():
    rows = _load_rows()
    return spark.createDataFrame(rows, schema=SCHEMA)
