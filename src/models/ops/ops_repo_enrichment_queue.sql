{{ config(materialized='table') }}

-- Repos to fetch on the next GitHub API enrichment run, highest priority first.
-- ingestion/github_repos.py reads the top of this table, so the rules for "which repos matter"
-- live here, versioned and tested with the other models:
--   1. every repo in the trending list
--   2. the most active repos by distinct human actors on the latest day of data
--   3. already-enriched repos not checked for `enrichment_refresh_after_days`, oldest first
-- Repos found missing or blocked are skipped until `enrichment_recheck_unavailable_days` have
-- passed. Known repos carry their saved ETag, so an unchanged repo costs no API quota, and are
-- requested by their current name to avoid rename redirects.
-- Time is measured against the latest day of data rather than the clock, like the reports.

with latest_day as (
    select cast(max(event_date) as timestamp) as as_of
    from {{ ref('fct_repo_daily_activity') }}
),

fetch_state as (
    select * from {{ ref('int_repo_latest_fetch') }}
),

known_repos as (
    select repo_id, full_name from {{ ref('dim_repo') }}
),

trending as (
    select
        repo_id,
        repo_name,
        'trending' as reason,
        1 as priority_group,
        trending_rank as rank_in_group
    from {{ ref('rpt_trending_repos') }}
),

most_active as (
    select
        daily.repo_id,
        daily.repo_name,
        'most_active' as reason,
        2 as priority_group,
        row_number() over (
            order by daily.unique_human_actors desc, daily.total_events desc, daily.repo_id
        ) as rank_in_group
    from {{ ref('fct_repo_daily_activity') }} as daily
    inner join latest_day on daily.event_date = to_date(latest_day.as_of)
    where daily.unique_human_actors > 0
),

refresh as (
    select
        fetch_state.repo_id,
        known_repos.full_name as repo_name,
        'refresh' as reason,
        3 as priority_group,
        row_number() over (
            order by fetch_state.last_checked_at, fetch_state.repo_id
        ) as rank_in_group
    from fetch_state
    inner join known_repos on fetch_state.repo_id = known_repos.repo_id
    cross join latest_day
    where
        fetch_state.is_available
        and fetch_state.last_checked_at
            < latest_day.as_of - interval {{ var('enrichment_refresh_after_days') }} days
),

candidates as (
    select * from trending
    union all
    select * from most_active
    where rank_in_group <= {{ var('enrichment_active_top_n') }}
    union all
    select * from refresh
    where rank_in_group <= {{ var('enrichment_refresh_top_n') }}
),

eligible as (
    select
        candidates.*,
        coalesce(known_repos.full_name, candidates.repo_name) as request_name,
        fetch_state.last_etag,
        fetch_state.last_checked_at,
        row_number() over (
            partition by candidates.repo_id
            order by candidates.priority_group, candidates.rank_in_group
        ) as occurrence
    from candidates
    cross join latest_day
    left join fetch_state on candidates.repo_id = fetch_state.repo_id
    left join known_repos on candidates.repo_id = known_repos.repo_id
    where not coalesce(
        fetch_state.latest_outcome in ('not_found', 'blocked')
        and fetch_state.last_checked_at
            >= latest_day.as_of - interval {{ var('enrichment_recheck_unavailable_days') }} days,
        false
    )
)

select
    row_number() over (order by priority_group, rank_in_group, repo_id) as priority,
    repo_id,
    request_name as repo_name,
    reason,
    last_etag as etag,
    last_checked_at,
    current_timestamp() as queued_at
from eligible
where occurrence = 1
