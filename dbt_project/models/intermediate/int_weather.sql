{{
  config(
    materialized='view',
  )
}}


select
    municipality,
    regions,
    dates,
    {{convert_f_to_c('tempmax')}}       AS tempmax_c,
    {{convert_f_to_c('tempmin')}}       AS tempmin_c,
    {{convert_f_to_c('temp')}}          AS temp_c,
    {{convert_f_to_c('feelslikemax')}}  AS feelslikemax_c,
    {{convert_f_to_c('feelslikemin')}}  AS feelslikemin_c,
    {{convert_f_to_c('feelslike')}}     AS feelslike_c,
    {{convert_f_to_c('dew')}}           AS dew_c,
    humidity,
    {{convert_inch_to_mm('precip')}}    AS precip_mm,
    precipprob,
    precipcover,
    preciptype,
    {{convert_inch_to_cm('snow')}}      AS snow_cm,
    {{convert_inch_to_cm('snowdepth')}} AS snowdepth_cm,
    {{convert_mph_to_kph('windgust')}}  AS windgust_kph,
    {{convert_mph_to_kph('windspeed')}} AS windspeed_kph,
    winddir,
    pressure,
    cloudcover,
    visibility,
    solarradiation,
    {{convert_mj_per_m2_to_kwh_per_m2('solarenergy')}} AS solarenergy_kwhpm2,
    uvindex,
    severerisk,
    FORMAT_TIMESTAMP('%H:%M:%S', sunrise) AS sunrise,
    FORMAT_TIMESTAMP('%H:%M:%S', sunset)  AS sunset,
    timeofday,
    moonphase,
    conditions,
    descriptions

from {{ref('stg_weather')}}
