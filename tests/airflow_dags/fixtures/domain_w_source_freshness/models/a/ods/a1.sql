{{ config(
    materialized='table',
    file_format='delta',
) }}


select *
from {{ source("ext_tables", "_external_table_to_use_in_source") }}
