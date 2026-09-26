{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['event_date', 'event_hour', 'event_type'],
        liquid_clustered_by=['event_date'],
    )
}}

-- Event volume per UTC hour and event type: powers the "when does GitHub work" heatmap
-- and the bot-share trend.
with events as (
    select *
    from {{ ref('stg_gharchive__events') }}
    where {{ changed_dates_filter(ref('stg_gharchive__events')) }}
)

select
    event_date,
    event_hour,
    event_type,
    count(*) as events,
    count_if(is_bot) as bot_events,
    count(distinct actor_id) as unique_actors,
    max(_ingested_at) as _loaded_through
from events
group by event_date, event_hour, event_type
