#
# FILE: `StockTrader/dbt/Dockerfile`
#


#
# Install Minimal Python Environment
#

FROM python:3.11-slim


#
# Install Necessary Dependencies for Debian-based Linux Dist (e.g. ubuntu)
#

RUN apt-get update && apt-get install -y \
	git \
	curl \
	&& rm -rf /var/lib/apt/lists/*


#
# Install Relevant dbt Dependencies
#

RUN pip install --no-cache-dir \
	dbt-core==1.7.* \
	dbt-duckdb==1.7.* \
	pandas \
	pyarrow

#
# Set Working Directory to dbt Project Root
#

WORKDIR /opt/stocktrader/dbt


#
# Define Default Container Command
# Note: `tail -f /dev/null`
# 	• inf loop that does not do anything
# 	• keeps container running indefinitely
# 	• avoids consuming excessive resources
#

CMD ["tail", "-f", "/dev/null"]