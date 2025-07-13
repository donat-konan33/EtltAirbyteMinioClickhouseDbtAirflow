{{
  config(
    materialized='view'
  )
}}

with int_table AS (
  select geo_point_2d,
         geo_shape,
         reg_name,
         reg_code,
         dep_name,
         CASE
          WHEN dep_name='CORSE DU SUD' THEN 'corse'
          WHEN dep_name='HAUTE CORSE' THEN 'corse0'
          ELSE dep_name
         END AS dep_name_modif,
         dep_code,
         dep_status
  from {{ ref('stg_depcode') }}
 )

select
  geo_point_2d,
  geo_shape,
  reg_name,
  reg_code,
  dep_name,
  LOWER(TRIM((REGEXP_REPLACE(dep_name_modif, '[^A-Za-z0-9]', '')))) AS department_lower,
  dep_code,
  dep_status
from int_table
