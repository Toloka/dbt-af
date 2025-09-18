{{
    config(
        materialized="table",
    )
}}


select *
from {{ ref("a1") }}
union all
select *
from {{ ref("a2") }}
