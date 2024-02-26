{{ config(
    materialized='table',
    file_format='delta',
    unique_key='id',
) }}


select *
from {{ ref('a1') }}
where val = 'b'
union all
select *
from {{ ref('a2') }}
where val = 'b'
