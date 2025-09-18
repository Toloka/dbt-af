{{
    config(
        materialized="table",
        schedule="@hourly",
    )
}}


select *
from {{ ref("a2") }}
