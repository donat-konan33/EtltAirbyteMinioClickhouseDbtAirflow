{{
  config(
    materialized='view'
  )
}}


select
  municipality,
  LOWER(TRIM((REGEXP_REPLACE(municipality, r'[^A-Za-z0-9]', '')))) AS municipality_lower,
  dep_name,
  dep_code

from {{ref('stg_municipality')}}
