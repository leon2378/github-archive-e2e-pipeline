"""Silver: typed, flattened and validated events. Every dbt model reads from this table."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Rows failing any of these are dropped; the counts show up in the pipeline's data quality tab.
REQUIRED = {
    "valid_event_id": "event_id IS NOT NULL",
    "valid_event_type": "event_type IS NOT NULL",
    "valid_created_at": "created_at IS NOT NULL",
    "valid_repo": "repo_id IS NOT NULL AND repo_name IS NOT NULL",
}

# Unknown types are kept but flagged, so a new GitHub event type is a warning, not lost data.
KNOWN_EVENT_TYPES = [
    "CommitCommentEvent",
    "CreateEvent",
    "DeleteEvent",
    "DiscussionEvent",
    "ForkEvent",
    "GollumEvent",
    "IssueCommentEvent",
    "IssuesEvent",
    "MemberEvent",
    "PublicEvent",
    "PullRequestEvent",
    "PullRequestReviewCommentEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewThreadEvent",
    "PushEvent",
    "ReleaseEvent",
    "SponsorshipEvent",
    "WatchEvent",
]
KNOWN_TYPES_SQL = ", ".join(f"'{t}'" for t in KNOWN_EVENT_TYPES)


@dp.table(
    comment="One row per GitHub event with typed columns. Source for all gold models.",
    cluster_by=["event_date", "event_type"],
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(REQUIRED)
@dp.expect("known_event_type", f"event_type IN ({KNOWN_TYPES_SQL})")
def silver_events():
    # created_at looks like 2026-09-01T05:03:07Z. Date and hour are cut from the string so they
    # are always UTC, whatever the session time zone is.
    return spark.readStream.table("bronze_events").select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.get_json_object("payload", "$.action").alias("action"),
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.to_date(F.substring("created_at", 1, 10)).alias("event_date"),
        F.substring("created_at", 12, 2).cast("int").alias("event_hour"),
        F.col("actor.id").alias("actor_id"),
        F.col("actor.login").alias("actor_login"),
        F.coalesce(F.col("actor.login").endswith("[bot]"), F.lit(False)).alias("is_bot"),
        F.col("repo.id").alias("repo_id"),
        F.col("repo.name").alias("repo_name"),
        F.split("repo.name", "/")[0].alias("repo_owner"),
        F.col("org.login").alias("org_login"),
        F.col("payload").alias("payload_json"),
        "_source_file",
        "_ingested_at",
    )
