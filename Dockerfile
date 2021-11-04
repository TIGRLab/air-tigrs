FROM tigrlab/datman:0.1

RUN cd / && \
	git clone https://github.com/tigrlab/air-tigrs.git && \
	cd air-tigrs && \
	pip install -e .[all] --use-deprecated=legacy-resolver

WORKDIR /
ENTRYPOINT ["/bin/bash"]
