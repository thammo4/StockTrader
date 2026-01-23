#
# FILE: `StockTrader/airflow/Dockerfile`
#

FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
	# git \
	build-essential \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/*


USER airflow
RUN pip install --no-cache-dir \
	# [pyproject.dependencies]
	python-dotenv>=1.0.1 \
	pandas>=2.0.0 \
	numpy>=1.24.4 \
	pyarrow>=15.0.0 \
	requests>=2.31.0 \

	# [pyproject.optional-dependencies.airflow]
	holidays>=0.40 \
	fredapi>=0.5.1 \
	minio>=7.2.0 \
	redis>=5.0.0 \

	# [pyproject.optional-dependencies.pricing]
	QuantLib>=1.34 \
	scipy>=1.10.0 \

	# uvatradier dependencies
	matplotlib>=3.8.3 \
	websockets>=8.1
