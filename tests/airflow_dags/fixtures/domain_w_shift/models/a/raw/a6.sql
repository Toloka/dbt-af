{{
    config(
        materialized="table",
        schedule="@monthly",
        schedule_shift=30,
        schedule_shift_unit="hour",
    )
}}


select 1 as id, 'a' as val
union all
select 2 as id, 'b' as val
union all
select 3 as id, 'c' as val
