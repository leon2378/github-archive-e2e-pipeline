{{ config(materialized='table') }}

-- Repos to fetch on the next GitHub API enrichment run, highest priority first.
-- ingestion/github_repos.py reads the top of this table, so the rules for "which repos matter"
-- live here, versioned and tested with the other models:
--   1. every repo in the trending list
--   2. the most active repos by distinct human actors on the latest day of data
-- ETags are filled in once repo history exists, so unchanged repos cost no API quota.

with latest_day as (
    select max(event_date) as event_date
    from {{ ref('fct_repo_daily_activity') }}
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
    inner join latest_day on daily.event_date = latest_day.event_date
    where daily.unique_human_actors > 0
),

candidates as (
    select * from trending
    union all
    select * from most_active
    where rank_in_group <= {{ var('enrichment_active_top_n') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by repo_id order by priority_group, rank_in_group
        ) as occurrence
    from candidates
)

select
    row_number() over (order by priority_group, rank_in_group, repo_id) as priority,
    repo_id,
    repo_name,
    reason,
    cast(null as string) as etag,
    current_timestamp() as queued_at
from deduplicated
where occurrence = 1
