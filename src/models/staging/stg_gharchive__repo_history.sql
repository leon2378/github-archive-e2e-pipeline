-- Repo attribute versions (SCD Type 2), with readable names for the validity columns.
select
    repo_id,
    full_name,
    owner_login,
    owner_type,
    description,
    homepage,
    language,
    topics,
    license_spdx_id,
    is_fork,
    parent_full_name,
    is_archived,
    is_disabled,
    is_template,
    visibility,
    default_branch,
    repo_created_at,
    __START_AT as valid_from,
    __END_AT as valid_to,
    __END_AT is null as is_current
from {{ source('gharchive', 'silver_repo_history') }}
