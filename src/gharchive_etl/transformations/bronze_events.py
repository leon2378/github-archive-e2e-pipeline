"""Bronze: GH Archive events exactly as landed, loaded incrementally with Auto Loader."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

LANDING_PATH = spark.conf.get("gharchive.landing_path")

IDENTITY = StructType([StructField("id", LongType()), StructField("login", StringType())])

# An explicit schema keeps bronze stable when GitHub adds fields; anything that doesn't fit is
# kept in _rescued_data instead of failing the stream. `payload` has a different shape for each
# event type, so it is stored as raw JSON text and parsed in silver.
EVENT_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("created_at", StringType()),
        StructField("public", BooleanType()),
        StructField("actor", IDENTITY),
        StructField(
            "repo", StructType([StructField("id", LongType()), StructField("name", StringType())])
        ),
        StructField("org", IDENTITY),
        StructField("payload", StringType()),
    ]
)


@dp.table(
    comment="Raw GH Archive events as landed by the ingestion job. Append-only, one row per event.",
    table_properties={"quality": "bronze"},
)
def bronze_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(EVENT_SCHEMA)
        .load(LANDING_PATH)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
