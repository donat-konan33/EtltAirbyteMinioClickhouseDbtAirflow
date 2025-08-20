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
