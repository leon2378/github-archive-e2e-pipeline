"""Bronze: GitHub API fetch records as landed by ingestion/github_repos.py, via Auto Loader."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

REPOS_LANDING_PATH = spark.conf.get("gharchive.repos_landing_path")

# Mirrors slim_repo() in the ingestion job. Timestamps stay as ISO strings in bronze.
REPO_SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("full_name", StringType()),
        StructField("owner_login", StringType()),
        StructField("owner_type", StringType()),
        StructField("description", StringType()),
        StructField("homepage", StringType()),
        StructField("language", StringType()),
        StructField("topics", ArrayType(StringType())),
        StructField("license_spdx_id", StringType()),
        StructField("is_fork", BooleanType()),
        StructField("parent_full_name", StringType()),
        StructField("is_archived", BooleanType()),
        StructField("is_disabled", BooleanType()),
        StructField("is_template", BooleanType()),
        StructField("visibility", StringType()),
        StructField("default_branch", StringType()),
        StructField("size_kb", LongType()),
        StructField("stargazers_count", LongType()),
        StructField("forks_count", LongType()),
        StructField("open_issues_count", LongType()),
        StructField("subscribers_count", LongType()),
        StructField("created_at", StringType()),
        StructField("updated_at", StringType()),
        StructField("pushed_at", StringType()),
    ]
)

# One record per fetch attempt; `repo` is null unless the API answered 200.
FETCH_SCHEMA = StructType(
    [
        StructField("repo_id", LongType()),
        StructField("requested_name", StringType()),
        StructField("fetched_at", StringType()),
        StructField("http_status", IntegerType()),
        StructField("etag", StringType()),
        StructField("repo", REPO_SCHEMA),
    ]
)


@dp.table(
    comment="Raw GitHub API fetch records, one row per attempt (200, 304, 404, ...). Append-only.",
    table_properties={"quality": "bronze"},
)
def bronze_repos():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(FETCH_SCHEMA)
        .load(REPOS_LANDING_PATH)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
