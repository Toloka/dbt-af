{{
    config(
        materialized="table",
        schedule="@every15minutes",
        schedule_shift=3,
        schedule_shift_unit="minute",
    )
}}


select 1 as id, 'a' as val
union all
select 2 as id, 'b' as val
union all
select 3 as id, 'c' as val
