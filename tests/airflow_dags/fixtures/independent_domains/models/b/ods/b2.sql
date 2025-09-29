{{
    config(
        materialized="table",
    )
}}


select *
from {{ ref("b1") }}
