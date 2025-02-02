{{
  config(
    materialized='view'
  )
}}

select *
from {{ref('stg_depcode')}}
