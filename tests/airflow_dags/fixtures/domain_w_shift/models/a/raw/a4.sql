{{ config(
    materialized='table',
    file_format='delta',
    unique_key='id',
    schedule='@weekly',
    schedule_shift=2,
    schedule_shift_unit='day'
) }}


select  1 as id, 'a' as val
union all
select 2 as id, 'b' as val
union all
select 3 as id, 'c' as val
