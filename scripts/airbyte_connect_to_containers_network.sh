#!/bin/bash

# Target network name (the one used by your other services)
TARGET_NETWORK="minio-clickhouse-airflow"

# Check if the network exists
if ! docker network ls --format "{{.Name}}" | grep -q "^${TARGET_NETWORK}$"; then
  echo "❌ The network '${TARGET_NETWORK}' does not exist."
  exit 1
fi

echo "🔍 Searching for Airbyte containers..."
AIRBYTE_CONTAINERS=$(docker ps --format "{{.Names}}" | grep "airbyte")

if [ -z "$AIRBYTE_CONTAINERS" ]; then
  echo "❌ No Airbyte container found."
  exit 1
fi

for container in $AIRBYTE_CONTAINERS; do
  echo "➡️ Connecting $container to the $TARGET_NETWORK network ..."
  docker network connect $TARGET_NETWORK $container 2>/dev/null \
    && echo "✅ $container added to $TARGET_NETWORK" \
    || echo "⚠️ $container is already in $TARGET_NETWORK"
done

echo "🎉 All Airbyte containers have been connected to $TARGET_NETWORK."

# checkout whether minio and clickhouse are connected
if ! docker network inspect kind minio >/dev/null 2>&1; then
  echo "❌ MinIO is not connected to the network."
else
  echo "✅ MinIO is connected to the network."
fi

if ! docker network inspect kind clickhousedb >/dev/null 2>&1; then
  echo "❌ ClickHouse is not connected to the network."
else
  echo "✅ ClickHouse is connected to the network."
fi

# get all airflow containers
AIRFLOW_CONTAINERS=$(docker ps --format "{{.Names}}" | grep "airflow")

if [ -z "$AIRFLOW_CONTAINERS" ]; then
  echo "❌ No Airflow container found."
  exit 1
fi

for container in $AIRFLOW_CONTAINERS; do
  echo "➡️ Connecting $container to the $TARGET_NETWORK network ..."
  docker network connect $TARGET_NETWORK $container 2>/dev/null \
    && echo "✅ $container added to $TARGET_NETWORK" \
    || echo "⚠️ $container is already in $TARGET_NETWORK"
done

echo "🎉 All Airflow containers have been connected to $TARGET_NETWORK."

#  add all containers different to containers contained airbyte in its name
ALL_CONTAINERS=$(docker ps --format "{{.Names}}")
AIRBYTE_NETWORK=$
for container in $ALL_CONTAINERS; do
  if [[ ! "$container" =~ "airbyte" ]]; then
    echo "➡️ Connecting $container to the $TARGET_NETWORK network ..."
    docker network connect $TARGET_NETWORK $container 2>/dev/null \
      && echo "✅ $container added to $TARGET_NETWORK" \
      || echo "⚠️ $container is already in $TARGET_NETWORK"
  fi
done
