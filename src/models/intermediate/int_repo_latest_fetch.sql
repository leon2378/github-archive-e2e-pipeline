-- Latest fetch state per repo: when it was last checked and with what outcome, the ETag to send
-- on the next request, and the metrics from the latest successful (200) fetch.
with fetches as (
    select * from {{ ref('stg_gharchive__repo_fetches') }}
),

latest_attempt as (
    select
        repo_id,
        max(fetched_at) as last_checked_at,
        max_by(fetch_outcome, fetched_at) as latest_outcome
    from fetches
    group by repo_id
),

latest_etag as (
    select
        repo_id,
        max_by(etag, fetched_at) as last_etag
    from fetches
    where fetch_outcome in ('ok', 'not_modified') and etag is not null
    group by repo_id
),

latest_ok as (
    select
        repo_id,
        max(fetched_at) as metrics_as_of,
        max_by(stargazers_count, fetched_at) as stargazers_count,
        max_by(forks_count, fetched_at) as forks_count,
        max_by(open_issues_count, fetched_at) as open_issues_count,
        max_by(subscribers_count, fetched_at) as subscribers_count,
        max_by(pushed_at, fetched_at) as pushed_at
    from fetches
    where fetch_outcome = 'ok'
    group by repo_id
)

select
    latest_attempt.repo_id,
    latest_attempt.last_checked_at,
    latest_attempt.latest_outcome,
    latest_attempt.latest_outcome in ('ok', 'not_modified') as is_available,
    latest_etag.last_etag,
    latest_ok.metrics_as_of,
    latest_ok.stargazers_count,
    latest_ok.forks_count,
    latest_ok.open_issues_count,
    latest_ok.subscribers_count,
    latest_ok.pushed_at
from latest_attempt
left join latest_etag on latest_attempt.repo_id = latest_etag.repo_id
left join latest_ok on latest_attempt.repo_id = latest_ok.repo_id
