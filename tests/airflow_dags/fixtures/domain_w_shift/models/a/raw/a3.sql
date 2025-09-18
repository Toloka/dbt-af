{{
    config(
        materialized="table",
        schedule="@daily",
        schedule_shift=5,
        schedule_shift_unit="hour",
    )
}}


select 1 as id, 'a' as val
union all
select 2 as id, 'b' as val
union all
select 3 as id, 'c' as val
