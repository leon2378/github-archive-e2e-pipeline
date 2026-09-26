{{ config(materialized='table', liquid_clustered_by=['snapshot_date']) }}

-- Daily metrics per enriched repo (a periodic snapshot fact): one row per repo per day it was
-- checked. A 304 check means nothing changed, so it carries forward the previous metrics.
-- Rebuilt in full each run: the fetch log grows by only ~1,000 rows a day.
with checks as (
    select
        repo_id,
        fetched_at,
        fetch_outcome,
        stargazers_count,
        forks_count,
        open_issues_count,
        subscribers_count
    from {{ ref('stg_gharchive__repo_fetches') }}
    where fetch_outcome in ('ok', 'not_modified')
),

filled as (
    select
        repo_id,
        fetched_at,
        to_date(fetched_at) as snapshot_date,
        fetch_outcome,
        last_value(stargazers_count, true) over running as stargazers_count,
        last_value(forks_count, true) over running as forks_count,
        last_value(open_issues_count, true) over running as open_issues_count,
        last_value(subscribers_count, true) over running as subscribers_count
    from checks
    window running as (
        partition by repo_id order by fetched_at
        rows between unbounded preceding and current row
    )
),

last_check_of_day as (
    select
        *,
        row_number() over (partition by repo_id, snapshot_date order by fetched_at desc) as recency
    from filled
)

select
    snapshot_date,
    repo_id,
    stargazers_count,
    forks_count,
    open_issues_count,
    subscribers_count,
    fetch_outcome = 'not_modified' as is_carried_forward,
    stargazers_count
        - lag(stargazers_count) over (partition by repo_id order by snapshot_date) as stars_change,
    fetched_at as snapshot_at
from last_check_of_day
where recency = 1 and stargazers_count is not null
