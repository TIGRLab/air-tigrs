#!/bin/bash

## Entrypoint script to spin up and connect various airflow services
# Will fail if it cannot connect after CONNECTION_ATTEMPTS_MAX attempts

COMMAND=${1:-}

CONNECTION_ATTEMPTS_MAX=${CONNECTION_ATTEMPTS_MAX:-8}
readonly CONNECTION_ATTEMPTS_MAX

CONNECTION_INTERVAL=${CONNECTION_INTERVAL:-5}
readonly CONNECTION_INTERVAL

function run_with_retries(){

	local cmd
	cmd=${1}

	echo "Attempting to run $cmd"
	set +e
	eval "$cmd 2>&1"
	status=$?

	while [ ${status} -ne 0 ]; do
		[[ counter -eq $CONNECTION_ATTEMPTS_MAX ]] && \
			echo "Reached maximum number of attempts ${CONNECTION_ATTEMPTS_MAX}" && \
			echo "Failed to run command $cmd! Ensure DB service is available" && \
			exit 1

		echo "Waiting ${CONNECTION_INTERVAL} seconds before re-attempting connection"
		sleep $CONNECTION_INTERVAL

		echo "Starting attempt #$((counter + 1))"
		eval "$cmd 2>&1"
		status=$?
		echo "FAILED: Command exited with code ${status}"
		((counter++))
	done
	set -e
}

function check_additional_pip(){
	if [[ -n "${_PIP_ADDITIONAL_REQUIREMENTS=}" ]]; then
		echo "---- _PIP_ADDITIONAL_REQUIREMENTS defined, installing ----"
		echo "Installing: ${_PIP_ADDITIONAL_REQUIREMENTS}"
		echo "This is not suitable for production and/or staging environments!"
		echo "Build a custom image instead if additional requirements needed"
		pip install --no-cache-dir --user ${_PIP_ADDITIONAL_REQUIREMENTS}
	fi
}


function setup_development_environment() {
# Initialize shared archive directory

	echo "---- Creating Archive source directory ----"
	mkdir -p /sources/archive
	chown -R "${AIRFLOW_UID}:${AIRFLOW_GID}" /sources/archive ${AIRFLOW_HOME}

	if [[ -n "${AIRTIGRS_DEVELOPMENT_SETUP=}" ]]; then
		echo "---- AIRTIGRS_DEVELOPMENT_SETUP set, constructing archive ----"
		/sources/dev/archive-setup.sh /sources/archive /sources/dev
	fi
}

function start_webserver() {
	echo "---- Instantiating Airflow Webserver ----"
	airflow db init
	airflow users create \
		--username ${AIRFLOW_WEB_USER} \
		--password ${AIRFLOW_WEB_PASSWORD} \
		--firstname Datman \
		--lastname Clevis \
		--role Admin \
		--email example@example.com

	if [[ -n "${AIRTIGRS_DEFAULT_CONNECTIONS=}" ]]; then
		echo "---- AIRTIGRS_DEFAULT_CONNECTIONS set, importing ----"
		airflow connections import $AIRTIGRS_DEFAULT_CONNECTIONS
	fi

}

function start_scheduler(){
	run_with_retries "airflow scheduler"
}


function wait_for_slurm_connect(){
	echo "---- Connecting to SLURM controller ----"
	# Command to try is sacct
	# Check if command is available, if not then bad
	run_with_retries "sacct"

}

if [[ "${CONNECTION_ATTEMPTS_MAX}" -gt "0" ]]; then
	echo "---- Checking Airflow MetaDB status ----"
	run_with_retries "airflow db check"
fi

if [[ ${COMMAND} =~ ^(init)$ ]]; then
	setup_development_environment
	airflow version
fi

if [[ ${COMMAND} =~ ^(scheduler|drmaa)$ ]] \
	&& [[ "${CONNECTION_ATTEMPTS_MAX}" -gt "0" ]]; then
	check_additional_pip

	if [[ ${COMMAND} =~ ^(drmaa)$ ]]; then

		# Check to make sure we're configured for DRMAA
		CONFIGURED_EXEC=$(airflow config get-value core executor)
		if [[ ${CONFIGURED_EXEC} =~ ".*DRMAA.*" ]]; then
			echo -n "ERROR: Requested DRMAA executor but "
			echo "AIRFLOW__CORE__EXECUTOR=${AIRFLOW__CORE__EXECUTOR} "
			echo "Set AIRFLOW__CORE__EXECUTOR to use a DRMAA-type executor"
			echo "Exiting with error..."
			exit 1
		fi

		# Use DRMAA-specific checks instead?
		wait_for_slurm_connect
	fi
	start_scheduler
fi


if [[ ${COMMAND} =~ ^(webserver)$ ]]; then
	check_additional_pip
	start_webserver
	airflow webserver
fi

