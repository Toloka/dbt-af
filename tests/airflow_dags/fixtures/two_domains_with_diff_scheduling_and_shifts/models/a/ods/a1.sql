{{
    config(
        materialized="table",
        schedule="@hourly",
        schedule_shift=10,
        schedule_shift_unit="minute",
    )
}}


select 1 as id, 'a' as val
union all
select 2 as id, 'b' as val
union all
select 3 as id, 'c' as val
