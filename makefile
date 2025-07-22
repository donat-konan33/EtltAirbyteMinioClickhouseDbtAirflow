.PHONY: test_python_functions test_dags_config test_tasks_configs test_trigger_dagrun_dag_config test

# Our tests will re-create a SQLite copy of your postgres db inside the "temp" folder, and then test against this sqlite db
AIRFLOW_HOME=${PWD}/tests/temp

test_python_functions:
	python -m pytest -v --disable-warnings tests/test_python_functions.py

test_trigger_dagrun_dag_config:
	export AIRFLOW_HOME=${AIRFLOW_HOME}; \
	airflow db init; \
	chmod +x tests/scripts/init_connections.sh; tests/scripts/init_connections.sh; echo 'Succesfully created db and connection'; \
	pytest -v --disable-warnings tests/test_trigger_dagrun_dag_config.py

test_dags_config:
	export AIRFLOW_HOME=${AIRFLOW_HOME}; \
	airflow db init; \
	chmod +x tests/scripts/init_connections.sh; tests/scripts/init_connections.sh; echo 'Succesfully created db and connection'; \
	pytest -v --disable-warnings tests/test_dags_config.py

test_tasks_config:
	export AIRFLOW_HOME=${AIRFLOW_HOME}; \
	airflow db init; \
	chmod +x tests/scripts/init_connections.sh; tests/scripts/init_connections.sh; echo 'Succesfully created db and connection'; \
	pytest -v --disable-warnings tests/test_tasks_config.py
