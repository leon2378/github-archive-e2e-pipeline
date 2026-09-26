-- Every repo's history must be a clean chain of versions: each version ends exactly when the next
-- one starts (no gaps, no overlaps) and exactly one version is current. Returns offending repos;
-- the test passes when this returns no rows.
with versions as (
    select
        repo_id,
        valid_from,
        valid_to,
        lead(valid_from) over (partition by repo_id order by valid_from) as next_valid_from
    from {{ ref('stg_gharchive__repo_history') }}
)

select repo_id, 'gap_or_overlap' as problem
from versions
where next_valid_from is not null and (valid_to is null or valid_to <> next_valid_from)

union all

select repo_id, 'not_exactly_one_current_version' as problem
from {{ ref('stg_gharchive__repo_history') }}
group by repo_id
having count_if(is_current) <> 1
