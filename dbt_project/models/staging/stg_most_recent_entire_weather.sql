{{
  config(
    materialized="view",
  )
}}

with average_snowdepth as (
  select AVG(snowdepth) AS avg_snowdepth
  from {{source("raw_entire_data", "latest_entire_weather")}}
)

select
  _airbyte_ab_id as record_id,
  DATE(datetime) as dates,
  datetimeEpoch,
  department,
  resolvedAddress as locations,
  latitude,
  longitude,
  solarenergy,
  solarradiation,
  uvindex,
  temp,
  tempmax,
  tempmin,
  feelslike,
  feelslikemax,
  feelslikemin,
  precip,
  precipprob,
  precipcover,
  snow,
  COALESCE(snowdepth, avg_snowdepth) AS snowdepth_filled,
  dew,
  humidity,
  winddir,
  windspeed,
  windgust,
  pressure,
  severerisk,
  icon,
  cloudcover,
  conditions,
  moonphase,
  description as descriptions,
  PARSE_TIME('%H:%M:%S', sunrise) AS sunrise_time,
  PARSE_TIME('%H:%M:%S', sunset) AS sunset_time,
  source,
  sunriseEpoch,
  sunsetEpoch,

from {{source("raw_entire_data", "latest_entire_weather")}},
    average_snowdepth
