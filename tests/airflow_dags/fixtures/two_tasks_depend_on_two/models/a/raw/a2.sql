{{ config(
    materialized='table',
    file_format='delta',
    unique_key='id',
) }}


select  4 as id, 'a' as val
union all
select 5 as id, 'b' as val
union all
select 6 as id, 'c' as val
