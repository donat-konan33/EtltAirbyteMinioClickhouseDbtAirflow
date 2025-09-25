#!/bin/bash

# Target network name (the one used by your other services)
TARGET_NETWORK="minio-clickhouse-airflow"
AIRBYTE_NETWORK="kind"

# Check if the network exists
if ! docker network ls --format "{{.Name}}" | grep -q "^${TARGET_NETWORK}$"; then
  echo "❌ The network '${TARGET_NETWORK}' does not exist."
  exit 1
fi

echo "🔍 Searching for Airbyte containers..."
AIRBYTE_CONTAINERS=$(docker ps --format "{{.Names}}" | grep "airbyte-")

if [ -z "$AIRBYTE_CONTAINERS" ]; then
  echo "❌ No Airbyte container found."
  exit 1
else
  for container in $AIRBYTE_CONTAINERS; do
    echo "➡️ Connecting $container to the $TARGET_NETWORK network ..."
    docker network connect $TARGET_NETWORK $container 2>/dev/null \
      && echo "✅ $container added to $TARGET_NETWORK" \
      || echo "⚠️ $container is already in $TARGET_NETWORK"
  done
fi

echo "🎉 All Airbyte containers have been connected to $TARGET_NETWORK."


if ! docker network ls --format "{{.Name}}" | grep -q "^${AIRBYTE_NETWORK}$"; then
  echo "❌ The network '${AIRBYTE_NETWORK}' does not exist."
  exit 1
fi

# add minio to kind network of airbyte
echo "🔍 Searching for Minio container..."
MINIO_CONTAINER=$(docker ps --format "{{.Names}}" | grep "^minio$") # get exactly minio container name

if [ -z "$MINIO_CONTAINER" ]; then
  echo "❌ No Minio container found."
  exit 1
else
  for container in $MINIO_CONTAINER; do
    echo "➡️ Connecting $container to the $AIRBYTE_NETWORK network ..."
    docker network connect $AIRBYTE_NETWORK $container 2>/dev/null \
      && echo "✅ $container added to $AIRBYTE_NETWORK" \
      || echo "⚠️ $container is already in $AIRBYTE_NETWORK"
  done
fi
