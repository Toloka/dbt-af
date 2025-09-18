{{
    config(
        materialized="table",
    )
}}


select *
from {{ ref("a1") }}
where val = 'b'
