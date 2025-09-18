{{
    config(
        materialized="table",
    )
}}


select 1 as id, 'a' as val, timestamp '2020-01-01 00:00:00' as created_at
union all
select 2 as id, 'b' as val, timestamp '2020-01-02 00:00:00' as created_at
union all
select 3 as id, 'c' as val, timestamp '2020-01-03 00:00:00' as created_at
