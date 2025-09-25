#!/bin/bash
# create external volumes
docker create external volume minio-data
docker create external volume clickhouse-altinity-data
docker create external volume airflow-metadatabase
# ...
