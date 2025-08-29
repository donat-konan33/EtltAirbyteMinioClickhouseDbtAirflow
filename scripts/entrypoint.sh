#!/usr/bin/env bash

# airflow 3.0.1
airflow db migrate

airflow users create -r Admin -u $AIRFLOW_ADMIN_USERNAME -p $AIRFLOW_ADMIN_PASSWORD -e $AIRFLOW_ADMIN_EMAIL -f $AIRFLOW_ADMIN_FIRST_NAME -l $AIRFLOW_ADMIN_LAST_NAME

# no longer needed : ref : https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/simple/index.html

# adjust value according to your needs | replace the default ./airflow/config/simple_auth_passwords.json
#cat <<EOF > ./airflow_auth_config/simple_auth_passwords.json
#{
#  "${AIRFLOW_ADMIN_USERNAME}": "${AIRFLOW_ADMIN_PASSWORD}"
#}
#EOF
# relevant for airflow 3.x

scripts/init_connections.sh

airflow variables set LAKE_BUCKET $LAKE_BUCKET
airflow variables set LAKE_BUCKET_2 $LAKE_BUCKET_2
# airflow api-server # instead of the ancient webserver command "airflow webserver" --- IGNORE ---

airflow webserver
