{#
    Incremental helper for date-grained models.

    On incremental runs, keeps only the event dates that received new rows since the model last
    ran. Those dates are then recomputed in full and merged, so late-arriving or backfilled data
    corrects existing aggregates instead of being skipped. On full refreshes it keeps everything.

    Models using it must output `max(_ingested_at) as _loaded_through`.
#}
{% macro changed_dates_filter(relation) -%}
    {%- if is_incremental() -%}
    event_date in (
        select distinct event_date
        from {{ relation }}
        where _ingested_at > (
            select coalesce(max(_loaded_through), timestamp '1970-01-01') from {{ this }}
        )
    )
    {%- else -%}
    true
    {%- endif -%}
{%- endmacro %}
