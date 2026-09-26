{{ config(materialized='table') }}

-- Top repositories by stars in the most recent window, compared with the window before it.
-- The window ends at the latest date in the data rather than today, so backfilled history works.
{%- set window_days = var('trending_window_days') %}

with daily as (
    select * from {{ ref('fct_repo_daily_activity') }}
),

as_of as (
    select max(event_date) as as_of_date from daily
),

windowed as (
    select
        daily.repo_id,
        max_by(daily.repo_name, daily.event_date) as repo_name,
        as_of.as_of_date,
        sum(case when daily.event_date > date_sub(as_of.as_of_date, {{ window_days }})
            then daily.stars else 0 end) as stars_in_window,
        sum(case when daily.event_date <= date_sub(as_of.as_of_date, {{ window_days }})
            then daily.stars else 0 end) as stars_prior_window,
        sum(case when daily.event_date > date_sub(as_of.as_of_date, {{ window_days }})
            then daily.forks else 0 end) as forks_in_window
    from daily
    cross join as_of
    where daily.event_date > date_sub(as_of.as_of_date, {{ 2 * window_days }})
    group by daily.repo_id, as_of.as_of_date
),

ranked as (
    select
        -- Stars are sparse in GH Archive, so ties are common: break them by forks, then name.
        row_number() over (
            order by stars_in_window desc, forks_in_window desc, repo_name
        ) as trending_rank,
        repo_id,
        repo_name,
        as_of_date,
        stars_in_window,
        stars_prior_window,
        stars_in_window - stars_prior_window as star_delta,
        -- null when the repo had no stars in the prior window (growth is undefined)
        round(try_divide(stars_in_window - stars_prior_window, stars_prior_window) * 100, 1)
            as star_growth_pct,
        forks_in_window
    from windowed
    where stars_in_window > 0
)

select *
from ranked
where trending_rank <= {{ var('trending_top_n') }}
