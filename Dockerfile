ARG AIRFLOW_BASE_IMAGE="2.2.1-python3.8"

FROM apache/airflow:${AIRFLOW_BASE_IMAGE}

USER root
RUN apt-get update && apt-get install --no-install-recommends -y \
	wget git gettext

## Taken from the Datman Dockerfile
RUN mkdir /.ssh && \
	ln -s /.ssh /home/airflow/.ssh && \
	chmod 777 /.ssh && \
	ssh-keyscan github.com >> /.ssh/known_hosts && \
	chmod 666 /.ssh/known_hosts

## Set up volumes
RUN mkdir -p /sources/{airflow,config,archive,dev} && \
	chown -R airflow:0 /sources

USER airflow
WORKDIR /home/airflow
RUN cd $HOME && \
	git clone https://github.com/TIGRLAB/datman.git && \
	cd datman && pip install --user .

ENV PATH="${PATH}:${HOME}/datman/bin"
ENV DM_CONFIG=/config/main_config.yml
ENV DM_SYSTEM=docker

COPY . ./air-tigrs
RUN	cd air-tigrs \
	&& pip install --use-deprecated=legacy-resolver --user .[buildtest]

ENTRYPOINT ["/bin/bash"]
