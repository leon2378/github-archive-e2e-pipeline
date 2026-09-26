-- The columns the marts need from silver, in one place.
select
    event_id,
    event_type,
    action,
    created_at,
    event_date,
    event_hour,
    actor_id,
    actor_login,
    is_bot,
    repo_id,
    repo_name,
    repo_owner,
    org_login,
    _ingested_at
from {{ source('gharchive', 'silver_events') }}
