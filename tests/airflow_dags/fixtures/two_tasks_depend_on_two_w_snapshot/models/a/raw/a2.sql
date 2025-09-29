{{
    config(
        materialized="table",
    )
}}


select 4 as id, 'a' as val
union all
select 5 as id, 'b' as val
union all
select 6 as id, 'c' as val
