{{
    config(
        materialized="table",
    )
}}


select *
from {{ ref("a1") }}
where val <> 'a'
union all
select *
from {{ ref("b1") }}
where val <> 'a'
