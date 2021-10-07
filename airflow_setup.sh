#!/usr/bin/env bash

# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)
export AIRFLOW_HOME=$(pwd)

AIRFLOW_VERSION=2.1.4
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# initialize the database
airflow db init

airflow users create \
    --username <username> \
    --firstname <first_name> \
    --lastname <last_name> \
    --role Admin \
    --email <email>

# start the scheduler
# open a new terminal or else run webserver with ``-D`` option to run it as a daemon
airflow scheduler -D

# start the web server, default port is 8080
# using port 8088 here since port 8080 was already being used
airflow webserver --port 8088

# visit localhost:8088 in the browser and use the admin account you just
# created to login. Enable the example_bash_operator dag in the home page
