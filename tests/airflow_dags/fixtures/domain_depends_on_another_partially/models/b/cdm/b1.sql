{{
    config(
        materialized="table",
    )
}}


select 1 as id, 'a' as data
union all
select 2 as id, 'b' as data
union all
select 3 as id, 'c' as data
