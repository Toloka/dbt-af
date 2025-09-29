{{
    config(
        materialized="table",
    )
}}


select *
from {{ ref("b2") }}
