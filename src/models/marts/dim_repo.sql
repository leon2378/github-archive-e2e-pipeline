{{ config(materialized='table') }}

-- One row per enriched repo: its current attributes (the latest SCD2 version), whether it is
-- still visible on GitHub, and its latest metrics. Join facts to it on repo_id, which survives
-- renames and ownership transfers.
with history as (
    select * from {{ ref('stg_gharchive__repo_history') }}
),

current_version as (
    select * from history where is_current
),

versions as (
    select
        repo_id,
        count(*) as version_count,
        min(valid_from) as first_seen_at
    from history
    group by repo_id
),

fetch_state as (
    select * from {{ ref('int_repo_latest_fetch') }}
)

select
    current_version.repo_id,
    current_version.full_name,
    current_version.owner_login,
    current_version.owner_type,
    current_version.description,
    current_version.homepage,
    current_version.language,
    current_version.topics,
    current_version.license_spdx_id,
    current_version.is_fork,
    current_version.parent_full_name,
    current_version.is_archived,
    current_version.is_disabled,
    current_version.is_template,
    current_version.visibility,
    current_version.default_branch,
    current_version.repo_created_at,
    datediff(current_date(), to_date(current_version.repo_created_at)) as repo_age_days,
    current_version.valid_from as current_version_since,
    versions.version_count,
    versions.first_seen_at,
    fetch_state.is_available,
    fetch_state.latest_outcome,
    fetch_state.last_checked_at,
    fetch_state.stargazers_count,
    fetch_state.forks_count,
    fetch_state.open_issues_count,
    fetch_state.subscribers_count,
    fetch_state.pushed_at,
    fetch_state.metrics_as_of
from current_version
inner join versions on current_version.repo_id = versions.repo_id
left join fetch_state on current_version.repo_id = fetch_state.repo_id
