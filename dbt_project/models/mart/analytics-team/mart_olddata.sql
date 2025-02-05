{{
  config(
    materialized='table'
  )
}}

with int_weather as (
  select *
  from {{ref('int_weather')}}
),
  int_depcode as (
    select *
    from {{ref('int_depcode')}}
),

  int_municipality as (
    select *
    from {{ref('int_municipality')}}
  )

select
    id.geo_point_2d as geo_point_2d,
    id.geo_shape as geo_shape,
    id.reg_name as reg_name,
    id.dep_code as dep_code,
    id.dep_status as dep_status,
    iw.regions as regions,
    iw.municipality as municipality,
    iw.dates as dates,
    iw.tempmax_c as tempmax,
    iw.tempmin_c AS tempmin_c,
    iw.temp_c AS temp_c,
    iw.feelslikemax_c AS feelslikemax_c,
    iw.feelslikemin_c AS feelslikemin_c,
    iw.feelslike_c AS feelslike_c,
    iw.dew_c AS dew_c,
    iw.humidity as humidity,
    iw.precip_mm AS precip_mm,
    iw.precipprob as precipprob,
    iw.precipcover as precipcover,
    iw.preciptype as preciptype,
    iw.snow_cm AS snow_cm,
    iw.snowdepth_cm AS snowdepth_cm,
    iw.windgust_kph AS windgust_kph,
    iw.windspeed_kph AS windspeed_kph,
    iw.winddir as winddir,
    iw.pressure as pressure,
    iw.cloudcover as cloudcover,
    iw.visibility as visibility,
    iw.solarradiation as solarradiation,
    iw.solarenergy_kwhpm2 AS solarenergy_kwhpm2,
    iw.uvindex as uvindex,
    iw.severerisk as severerisk,
    iw.sunrise AS sunrise,
    iw.sunset AS sunset,
    iw.timeofday as timeofday,
    iw.moonphase as moonphase,
    iw.conditions as conditions,
    iw.descriptions as descriptions

from int_weather iw
Left Join int_municipality im using (municipality_lower)
Left Join int_depcode id using (dep_code)
