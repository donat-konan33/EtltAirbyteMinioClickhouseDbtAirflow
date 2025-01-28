#!/usr/bin/env bash
set -x

echo "Add connection"
airflow connections add 'postgres_connection' \
                    --conn-type postgres \
                    --conn-host "$HOST" \
                    --conn-schema "$POSTGRES_DB" \
                    --conn-login "$POSTGRES_USER" \
                    --conn-password "$POSTGRES_PASSWORD" \
                    --conn-port "5432"

airflow connections add 'google_cloud_default' \
    --conn-type 'google_cloud_platform' \
    --conn-extra "{
        \"extra__google_cloud_platform__keyfile_dict\": $(echo $GCP_SERVICE_ACCOUNT_KEY | base64 -d),
        \"extra__google_cloud_platform__project\": \"${PROJECT_ID}\"
    }"
