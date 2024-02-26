{{ config(
    materialized='table',
    file_format='delta',
    unique_key='id',
) }}


select a.id, b.val
from {{ ref('b1') }} as a
join {{ ref('a2') }} as b
on a.id = b.id
