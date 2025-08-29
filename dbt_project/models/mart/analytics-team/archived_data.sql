{{
  config(
    materialized='table'
  )
}}

SELECT * FROM {{ source('clickhouse', 'archived_data') }}
UNION ALL
SELECT * FROM {{ ref('mart_newdata') }}
