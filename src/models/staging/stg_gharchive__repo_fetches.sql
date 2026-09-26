-- One row per GitHub API fetch attempt.
select
    repo_id,
    queued_repo_id,
    requested_name,
    full_name,
    fetched_at,
    http_status,
    fetch_outcome,
    etag,
    stargazers_count,
    forks_count,
    open_issues_count,
    subscribers_count,
    pushed_at,
    _ingested_at
from {{ source('gharchive', 'silver_repo_fetches') }}
