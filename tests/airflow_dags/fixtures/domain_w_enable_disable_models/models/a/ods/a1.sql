{{ config(
    materialized='table',
    file_format='delta',
    unique_key='id',
    enable_from_dttm='2023-01-01',
) }}


select  1 as id, 'a' as val
union all
select 2 as id, 'b' as val
union all
select 3 as id, 'c' as val
