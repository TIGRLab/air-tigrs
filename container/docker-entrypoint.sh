#!/bin/bash

## Entrypoint script to spin up and connect various airflow services
# Will fail if it cannot connect after CONNECTION_ATTEMPTS_MAX attempts
# Commands:
#	webserver - start Airflow webserver
#	scheduler [SERVICE_COMMAND] - start Airflow scheduler
# If scheduler is provided, an additional SERVICE_COMMAND can be specified
# to run checks with retries on. (i.e sacct for SLURM services)
# This is not necessary for "LocalExecutor" and "SequentialExecutor" as they
# do not require additional services be available

COMMAND=${1:-}
SERVICE_COMMAND=${2:-}

CONNECTION_ATTEMPTS_MAX=${CONNECTION_ATTEMPTS_MAX:-8}
readonly CONNECTION_ATTEMPTS_MAX

CONNECTION_INTERVAL=${CONNECTION_INTERVAL:-5}
readonly CONNECTION_INTERVAL

function log(){
	echo "---- $1 ----"
}

function log_failed(){
	echo "FAILED: $1"
}

function run_with_retries(){

	local cmd
	cmd=${1}

	echo "Attempting to run $cmd"
	set +e
	eval "$cmd 2>&1"
	status=$?

	while [ ${status} -ne 0 ]; do
		[[ counter -eq $CONNECTION_ATTEMPTS_MAX ]] && \
			log_failed "Reached maximum number of attempts ${CONNECTION_ATTEMPTS_MAX}" && \
			log_failed "Failed to run command $cmd! Ensure DB service is available" && \
			exit 1

		echo "Waiting ${CONNECTION_INTERVAL} seconds before re-attempting connection"
		sleep $CONNECTION_INTERVAL

		echo "Starting attempt #$((counter + 1))"
		eval "$cmd 2>&1"
		status=$?
		log_failed "Command exited with code ${status}"
		((counter++))
	done
	set -e
}

function check_additional_pip(){
	if [[ -n "${_PIP_ADDITIONAL_REQUIREMENTS=}" ]]; then
		log "_PIP_ADDITIONAL_REQUIREMENTS defined, installing"
		echo "Installing: ${_PIP_ADDITIONAL_REQUIREMENTS}"
		echo "This is not suitable for production and/or staging environments!"
		echo "Build a custom image instead if additional requirements needed"
		pip install --no-cache-dir --user ${_PIP_ADDITIONAL_REQUIREMENTS}
	fi
}

function initialize_webserver() {
	airflow db init
	airflow users create \
		--username ${AIRFLOW_WEB_USER:-airflow} \
		--password ${AIRFLOW_WEB_PASSWORD:-airflow} \
		--firstname Datman \
		--lastname Clevis \
		--role Admin \
		--email example@example.com

	if [[ -n "${AIRTIGRS_DEFAULT_CONNECTIONS=}" ]]; then
		log "AIRTIGRS_DEFAULT_CONNECTIONS set, importing"
		airflow connections import $AIRTIGRS_DEFAULT_CONNECTIONS
	fi
}

function start_scheduler(){
	run_with_retries "airflow scheduler"
}

function start_webserver(){
	run_with_retries "airflow webserver"
}


if [[ "${CONNECTION_ATTEMPTS_MAX}" -gt "0" ]]; then
	log "Checking Airflow MetaDB status"
	run_with_retries "airflow db check"
fi

if [[ ${COMMAND} =~ ^(scheduler)$ ]] \
	&& [[ "${CONNECTION_ATTEMPTS_MAX}" -gt "0" ]]; then

	CONFIGURED_EXEC=$(airflow config get-value core executor)
	readonly CONFIGURED_EXEC

	check_additional_pip

	if [[ ! ${CONFIGURED_EXEC} ]]; then
		log "No Executor configured, using SequentialExecutor"
		export AIRFLOW__CORE__EXECUTOR="SequentialExecutor"
	elif [[ ${CONFIGURED_EXEC} =~ ^(LocalExecutor|SequentialExecutor)$ ]]; then
		# Local/Sequential Executor has no additional requirements
		log "Requested ${CONFIGURED_EXEC}, starting..."
	elif [[ ! ${SERVICE_COMMAND} ]]; then
		# All other executors require external services
		# If a SERVICE_COMMAND is not given, then we cannot
		# check if the scheduler will be able to run jobs
		log_failed "Requested ${CONFIGURED_EXEC} but SERVICE_COMMAND not specified"
		log_failed "Provide a SERVICE_COMMAND so that service available for scheduler on start "
		exit 1
	else
		log "Requested ${CONFIGURED_EXEC}"
		log "Ensuring service is available with ${SERVICE_COMMAND}"
		run_with_retries $SERVICE_COMMAND
	fi

	log "Starting Scheduler"
	start_scheduler
fi


if [[ ${COMMAND} =~ ^(webserver)$ ]]; then
	check_additional_pip

	log "Initializing DB"
	initialize_webserver

	log "Starting Webserver"
	start_webserver
fi

