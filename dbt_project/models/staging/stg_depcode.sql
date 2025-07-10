--keep all columns and replace dep_status when null
{{
  config(
    materialized='view',
  )
}}

with depcode as (
      select *
      from {{ source('clickhouse', 'raw_depcode') }}
)

select geometryFromWKB(geo_point_2d) as geo_point_2d,
       geometryFromWKB(geo_shape) as geo_shape,
       reg_name,
       reg_code,
       dep_name_upper as dep_name,
       dep_current_code as dep_code,
       IF(dep_status='', 'undefined', dep_status) as dep_status
from depcode
