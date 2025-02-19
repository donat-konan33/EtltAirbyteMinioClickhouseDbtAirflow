{{
  config(
    materialized='view',
  )
}}


with weather as (
  select
    communes as municipality,
    regions,
    DATE(datetime) as dates,
    tempmax,
    tempmin,
    temp,
    feelslikemax,
    feelslikemin,
    feelslike,
    dew,
    humidity,
    precip,
    precipprob,
    precipcover,
    preciptype,
    snow,
    snowdepth,
    windgust,
    windspeed,
    winddir,
    sealevelpressure as pressure,
    cloudcover,
    visibility,
    solarradiation,
    solarenergy,
    uvindex,
    severerisk,
    TIMESTAMP(sunrise) as sunrise,
    TIMESTAMP(sunset) as sunset,
    timeofday,
    moonphase,
    conditions,
    description as descriptions

from {{source('raw_old_data', 'weather')}}
)

select distinct *
from weather
