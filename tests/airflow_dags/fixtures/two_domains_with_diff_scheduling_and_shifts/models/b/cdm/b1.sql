{{
    config(
        materialized="table",
        schedule="@daily",
        schedule_shift=2,
        schedule_shift_unit="hour",
    )
}}


select *
from {{ ref("a1") }}
