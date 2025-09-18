{{
    config(
        materialized="table",
    )
}}


select *
from {{ ref("a2") }}
