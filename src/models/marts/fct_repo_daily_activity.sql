{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['event_date', 'repo_id'],
        liquid_clustered_by=['event_date'],
    )
}}

-- One row per repository per day with its headline activity metrics.
with events as (
    select *
    from {{ ref('stg_gharchive__events') }}
    where {{ changed_dates_filter(ref('stg_gharchive__events')) }}
)

select
    event_date,
    repo_id,
    max_by(repo_name, created_at) as repo_name,
    max_by(org_login, created_at) as org_login,
    count_if(event_type = 'WatchEvent') as stars,
    count_if(event_type = 'ForkEvent') as forks,
    count_if(event_type = 'PushEvent') as pushes,
    count_if(event_type = 'PullRequestEvent' and action = 'opened') as pull_requests_opened,
    count_if(event_type = 'PullRequestEvent' and action = 'merged') as pull_requests_merged,
    count_if(event_type = 'IssuesEvent' and action = 'opened') as issues_opened,
    count_if(event_type = 'ReleaseEvent' and action = 'published') as releases,
    count(distinct case when not is_bot then actor_id end) as unique_human_actors,
    count(*) as total_events,
    count_if(is_bot) as bot_events,
    max(_ingested_at) as _loaded_through
from events
group by event_date, repo_id
