"""Silver: the repo fetch log, and repo attribute history as a Type 2 slowly changing dimension."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Attributes whose changes open a new version in silver_repo_history. Other attributes (such as
# the description or homepage) are overwritten on the current version, and fast-moving metrics
# (stars, forks) live in silver_repo_fetches instead, so they don't create a version every day.
TRACKED_ATTRIBUTES = [
    "full_name",
    "owner_login",
    "language",
    "topics",
    "license_spdx_id",
    "is_archived",
    "is_disabled",
    "visibility",
]


@dp.table(
    comment=(
        "One row per GitHub API fetch attempt: outcome, ETag and repo metrics. Source for "
        "availability, ETags and daily metric snapshots."
    ),
    cluster_by=["repo_id"],
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(
    {
        "valid_repo_id": "repo_id IS NOT NULL",
        "valid_fetched_at": "fetched_at IS NOT NULL",
    }
)
@dp.expect("known_http_status", "http_status IN (200, 304, 403, 404, 451)")
def silver_repo_fetches():
    status = F.col("http_status")
    return spark.readStream.table("bronze_repos").select(
        # Key on the id GitHub returned when there is one; the queued id is kept for auditing.
        F.coalesce(F.col("repo.id"), F.col("repo_id")).alias("repo_id"),
        F.col("repo_id").alias("queued_repo_id"),
        "requested_name",
        F.col("fetched_at").cast("timestamp").alias("fetched_at"),
        "http_status",
        F.when(status == 200, "ok")
        .when(status == 304, "not_modified")
        .when(status == 404, "not_found")
        .when(status.isin(403, 451), "blocked")
        .otherwise("other")
        .alias("fetch_outcome"),
        "etag",
        F.col("repo.full_name").alias("full_name"),
        F.col("repo.stargazers_count").alias("stargazers_count"),
        F.col("repo.forks_count").alias("forks_count"),
        F.col("repo.open_issues_count").alias("open_issues_count"),
        F.col("repo.subscribers_count").alias("subscribers_count"),
        F.col("repo.size_kb").alias("size_kb"),
        F.col("repo.pushed_at").cast("timestamp").alias("pushed_at"),
        "_ingested_at",
    )


@dp.temporary_view(comment="Successful fetches only: the snapshots that feed repo history.")
@dp.expect_all_or_drop(
    {
        "valid_repo_id": "repo_id IS NOT NULL",
        "valid_full_name": "full_name IS NOT NULL",
        "valid_fetched_at": "fetched_at IS NOT NULL",
    }
)
def repo_snapshots():
    # Sorted so a reordering of the same topics isn't mistaken for a change.
    topics = F.array_sort(F.coalesce(F.col("repo.topics"), F.array().cast("array<string>")))
    return (
        spark.readStream.table("bronze_repos")
        .where("http_status = 200 AND repo IS NOT NULL")
        .select(
            F.col("repo.id").alias("repo_id"),
            F.col("repo.full_name").alias("full_name"),
            F.col("repo.owner_login").alias("owner_login"),
            F.col("repo.owner_type").alias("owner_type"),
            F.col("repo.description").alias("description"),
            F.col("repo.homepage").alias("homepage"),
            F.col("repo.language").alias("language"),
            topics.alias("topics"),
            F.col("repo.license_spdx_id").alias("license_spdx_id"),
            F.col("repo.is_fork").alias("is_fork"),
            F.col("repo.parent_full_name").alias("parent_full_name"),
            F.col("repo.is_archived").alias("is_archived"),
            F.col("repo.is_disabled").alias("is_disabled"),
            F.col("repo.is_template").alias("is_template"),
            F.col("repo.visibility").alias("visibility"),
            F.col("repo.default_branch").alias("default_branch"),
            F.col("repo.created_at").cast("timestamp").alias("repo_created_at"),
            F.col("fetched_at").cast("timestamp").alias("fetched_at"),
        )
    )


dp.create_streaming_table(
    name="silver_repo_history",
    comment=(
        "Repo attributes over time (SCD Type 2): one row per version, valid from __START_AT "
        "until __END_AT (null = current version)."
    ),
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="silver_repo_history",
    source="repo_snapshots",
    keys=["repo_id"],
    sequence_by=F.col("fetched_at"),
    stored_as_scd_type="2",
    track_history_column_list=TRACKED_ATTRIBUTES,
)
