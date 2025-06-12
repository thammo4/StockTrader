[![CI Status](https://github.com/thammo4/StockTrader/actions/workflows/ci.yml/badge.svg)](https://github.com/thammo4/StockTrader/actions)
# StockTrader

## Overview

StockTrader is a data pipeline for retrieving, processing, and analyzing stock market data with a focus on options pricing. The system fetches historical market data including options chains, dividend records, and risk-free rate benchmarks, then processes this data to create inputs for options pricing models.

## Directory Structure
```
StockTrader/
├── Dockerfile
├── docker-compose.yml
├── airflow/
│   └── dags/
│       └── ingest_fred_rates_dag.py
│       └── ingest_tradier_dividends_dag.py
│       └── ingest_tradier_options_dag.py
│       └── ingest_tradier_quotes_dag.py
├── data/
│   └── warehouse/
│       └── fred/
│           └── <landing directory for FRED interest rate data>
│           └── <SERIESID.parquet>
│       └── dividends/
│           └── <landing directory for dividend data>
│           └── <SYMBOL.parquet>
│       └── options/
│           └── <landing directory for options chain data>
│           └── <SYMBOL.parquet>
│       └── quotes/
│           └── <landing directory for quote data>
│           └── <SYMBOL.parquet>
├── logs/
├── scripts/
│   └── create_dividend_parquet.py
│   └── create_fred_parquet.py
│   └── create_ohlcv_parquet.py
│   └── fetch_active_options.py
│   └── fetch_active_params.py
│   └── ingest_fred_rates.py
│   └── ingest_tradier_dividends.py
│   └── ingest_tradier_options.py
│   └── ingest_tradier_quotes.py
│   └── prep_bopm_data.py
│   └── price_bopm_data.py
├── src/
│   └── StockTrader
│       └── settings.py
│       └── tradier.py
│       └── freddy.py
└── tests/
│   └── config.py
│   └── test_freddy.py
│   └── test_settings.py
│   └── test_tradier.py
│   └── scripts/
│       └── test_create_dividend_parquet.py
│       └── test_create_fred_parquet.py
│       └── test_create_ohlcv_parquet.py
│       └── test_dolt_historical_ticker.py
└── utils/
│   └── dividend_table.py
|   └── parse_occ.py
|   └── vol_estimate.py
```
