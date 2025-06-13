#
# FILE: `StockTrader/dbt/Dockerfile`
#

FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
	git \
	curl \
	&& rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
	dbt-core==1.7.* \
	dbt-duckdb==1.7.* \
	pandas \
	pyarrow

WORKDIR /opt/stocktrader/dbt

CMD ["tail", "-f", "/dev/null"]