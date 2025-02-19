{{
  config(
    materialized='table'
  )
}}

with int_most_recent_weather as (
  select *
  from {{ref('int_most_recent_weather')}}
),
  int_depcode as (
    select *
    from {{ref('int_depcode')}}
)

select
  iw.record_id,
  iw.dates,
  iw.datetimeEpoch,
  iw.weekday_name,
  iw.department,
  id.dep_name,
  id.dep_code,
  id.dep_status,
  id.reg_name,
  id.reg_code,
  id.geo_point_2d,
  id.geo_shape,
  ST_AsGeoJSON(id.geo_shape) AS geojson,
  iw.locations,
  iw.latitude,
  iw.longitude,
  iw.solarenergy_kwhpm2,
  iw.solarradiation,
  iw.uvindex,
  iw.temp,
  iw.tempmax,
  iw.tempmin,
  iw.feelslike,
  iw.feelslikemax,
  iw.feelslikemin,
  iw.precip,
  iw.precipprob,
  iw.precipcover,
  iw.snow,
  iw.snowdepth_filled,
  iw.dew,
  iw.humidity,
  iw.winddir,
  iw.windspeed,
  iw.windgust,
  iw.pressure,
  iw.severerisk,
  iw.icon,
  iw.cloudcover,
  iw.conditions,
  iw.moonphase,
  iw.moonphase_label,
  iw.descriptions,
  iw.sunrise_time,
  iw.sunset_time,
  iw.source,
  iw.sunriseEpoch,
  iw.sunsetEpoch,
  EDIT_DISTANCE(iw.department_lower, id.department_lower) AS dep_distance,
  ROUND(AVG(solarenergy_kwhpm2) OVER(
    PARTITION BY id.reg_name
  ), 3) AS avg_solarenergy_kwhpm2,
  ROUND(AVG(solarradiation) OVER(
    PARTITION BY id.reg_name
  ), 3) AS avg_solarradiation

from int_most_recent_weather iw
inner join int_depcode id on EDIT_DISTANCE(iw.department_lower, id.department_lower) <= 1
