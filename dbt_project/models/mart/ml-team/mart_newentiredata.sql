{{
  config(
    materialized='table'
  )
}}

with int_most_recent_entire_weather as (
  select *
  from {{ref('int_most_recent_entire_weather')}}
),
  int_depcode as (
    select *
    from {{ref('int_depcode')}}
)

select dates,
      datetimeEpoch,
      latitude,
      longitude,
      department,
      reg_name,
      AVG(solarenergy_kwhpm2) AS solarenergy_kwhpm2,
      AVG(solarradiation) AS solarradiation,
      AVG(uvindex) AS uvindex,
      AVG(temp) AS temp,
      AVG(precip) AS precip

from (select
    iw.dates,
    iw.datetimeEpoch,
    iw.department,
    id.reg_name,
    iw.latitude,
    iw.longitude,
    iw.solarenergy_kwhpm2,
    iw.solarradiation,
    iw.uvindex,
    iw.temp,
    iw.precip,
    EDIT_DISTANCE(iw.department_lower, id.department_lower) AS dep_distance

  from int_most_recent_entire_weather iw
  inner join int_depcode id on EDIT_DISTANCE(iw.department_lower, id.department_lower) <= 1
  ) as subquery
group by dates,
         datetimeEpoch,
         department,
         reg_name,
         latitude,
         longitude

order by dates,
         department,
         reg_name
