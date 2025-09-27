{{
    config(
        materialized="table",
    )
}}


select *
from {{ ref("a1") }}
where val = 'a'
union all
select *
from {{ ref("a2") }}
where val = 'a'
