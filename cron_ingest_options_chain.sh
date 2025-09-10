#!/bin/bash

if [ "$(date '+%a %b %d %Y')" = "Mon May 26 2025" ]; then
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] SKIP ingest_options_chain, memorial day" >> /Users/rudi/Desktop/StockTrader/logs/cron_ingest_options_chain.log
	exit 0
fi

export STOCK_TRADER_HOME="/Users/rudi/Desktop/StockTrader";
export STOCK_TRADER_MARKET_DATA="/Users/rudi/Desktop/StockTrader/data";
export STOCK_TRADER_LOG="/Users/rudi/Desktop/StockTrader/logs";
export STOCK_TRADER_DWH="/Users/rudi/Desktop/StockTrader/data/warehouse";
export PYTHONPATH="/Users/rudi/Desktop/StockTrader";

{
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] BEGIN ingest_options_chain" >> /Users/rudi/Desktop/StockTrader/logs/cron_ingest_options_chain.log
	source /Users/rudi/Desktop/StockTrader/venv12/bin/activate
	pip3.12 install -e /Users/rudi/Desktop/uvatradier
	python3.12 /Users/rudi/Desktop/StockTrader/scripts/ingest_options_chain.py >> /Users/rudi/Desktop/StockTrader/logs/cron_ingest_options_chain.log 2>&1
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] DONE ingest_options_chain" >> /Users/rudi/Desktop/StockTrader/logs/cron_ingest_options_chain.log
}
