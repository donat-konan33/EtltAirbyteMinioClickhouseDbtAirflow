{{
  config(
    materialized='table',
    engine='ReplacingMergeTree',
    order_by='(dates, department)',
    partition_by='toYYYYMM(dates)',  -- Partitioning by month for better performance on date queries
    version='timestamp'  -- to explore for more understanding
  )
}}

SELECT * FROM {{ source('clickhouse', 'archived_data') }}
UNION ALL
SELECT * FROM {{ ref('mart_newdata') }}
