{{ config(
    materialized='table',
    file_format='delta',
    unique_key='id',
    schedule='@hourly',
) }}


select *
from {{ ref('a2') }}
