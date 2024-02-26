{{ config(
    materialized='table',
    file_format='delta',
    unique_key='id',
) }}


select  11 as id, 'a' as val, timestamp'2020-01-01 00:00:00' as created_at
union all
select 22 as id, 'b' as val, timestamp'2020-01-02 00:00:00' as created_at
union all
select 33 as id, 'c' as val, timestamp'2020-01-03 00:00:00' as created_at
