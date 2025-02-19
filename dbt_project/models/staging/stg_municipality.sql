{{
  config(
    materialized='view',
  )
}}


select mu.municipality,
       mu.dep_name_upper as dep_name,
       mu.dep_code
from {{source('raw_old_data', 'municipality')}} mu
