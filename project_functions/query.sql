--CTE
with weather_data as (
	select "_airbyte_raw_id" as record_id,
			"resolvedAddress" as address,
			longitude,
			latitude,
			"_airbyte_extracted_at" as extracted_date,
			jsonb_array_elements(days) as data
	from weather_of_city
),
--PostgresSQL Syntax for handling json data
expand_data as (
 select record_id,
		split_part(address,',', 2) as region,
		split_part(address, ',', 1) as department,
		longitude,
		latitude,
		extracted_date,
		(data ->> 'datetime')::date as datetime,
		(data ->> 'datetimeEpoch')::numeric as datetimeEpoch,
		(data ->> 'tempmax')::numeric as tempmax,
	    (data ->> 'tempmin')::numeric as tempmin,
	    (data ->> 'temp')::numeric as temp,
	    (data ->> 'feelslikemax')::numeric as feelslikemax,
	    (data ->> 'feelslikemin')::numeric as feelslikemin,
	    (data ->> 'feelslike')::numeric as feelslike,
	    (data ->> 'dew')::numeric as dew,
	    (data ->> 'humidity')::numeric as humidity,
	    (data ->> 'precip')::numeric as precip,
	    (data ->> 'precipprob')::numeric as precipprob,
	    (data ->> 'precipcover')::numeric as precipcover,
	    (data ->> 'preciptype')::varchar as preciptype,
	    (data ->> 'snow')::numeric as snow,
	    (data ->> 'snowdepth')::numeric as snowdepth,
	    (data ->> 'windgust')::numeric as windgust,
	    (data ->> 'windspeed')::numeric as windspeed,
	    (data ->> 'winddir'):: numeric as winddir,
	    (data ->> 'pressure')::numeric as pressure,
	    (data ->> 'cloudcover')::numeric as cloudcover,
	    (data ->> 'visibility')::numeric as visibility,
	    (data ->> 'solarradiation')::numeric as solarradiation,
	    (data ->> 'solarenergy')::numeric as solarenergy,
	    (data ->> 'uvindex')::numeric as uvindex,
	    (data ->> 'severerisk')::numeric as severerisk,
	    (data ->> 'sunrise')::varchar as sunrise,
	    (data ->> 'sunriseEpoch')::numeric as sunriseEpoch,
	    (data ->> 'sunset')::varchar as sunset,
	    (data ->> 'sunsetEpoch')::numeric as sunsetEpoch,
	    (data ->> 'moonphase')::numeric as moonphase,
	    (data ->> 'conditions')::text as conditions,
	    (data ->> 'description')::text as description,
	    (data ->> 'icon')::text as icon,
	    (data ->> 'source')::varchar as source
	from weather_data
)


select *
from expand_data expd;
