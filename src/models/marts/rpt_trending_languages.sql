{{ config(materialized='table') }}

-- Which languages are gaining stars: stars in the latest trending window per primary language,
-- across every enriched repo (the trending list plus the most active repos). Repos that haven't
-- been enriched yet have no known language and are left out.
{%- set window_days = var('trending_window_days') %}

with daily as (
    select * from {{ ref('fct_repo_daily_activity') }}
),

as_of as (
    select max(event_date) as as_of_date from daily
),

window_stars as (
    select
        daily.repo_id,
        sum(daily.stars) as stars_in_window
    from daily
    cross join as_of
    where daily.event_date > date_sub(as_of.as_of_date, {{ window_days }})
    group by daily.repo_id
    having sum(daily.stars) > 0
),

enriched as (
    select
        coalesce(repo.language, 'No language') as language,
        repo.full_name,
        window_stars.stars_in_window,
        trending.repo_id is not null as is_trending
    from window_stars
    inner join {{ ref('dim_repo') }} as repo on window_stars.repo_id = repo.repo_id
    left join {{ ref('rpt_trending_repos') }} as trending
        on window_stars.repo_id = trending.repo_id
),

by_language as (
    select
        language,
        count(*) as repos_with_stars,
        sum(stars_in_window) as stars_in_window,
        count_if(is_trending) as trending_repos,
        max_by(full_name, stars_in_window) as top_repo,
        max(stars_in_window) as top_repo_stars
    from enriched
    group by language
)

select
    row_number() over (order by stars_in_window desc, language) as language_rank,
    language,
    repos_with_stars,
    stars_in_window,
    round(100 * stars_in_window / sum(stars_in_window) over (), 1) as stars_share_pct,
    trending_repos,
    top_repo,
    top_repo_stars,
    (select as_of_date from as_of) as as_of_date
from by_language
