[![CI Status](https://github.com/thammo4/StockTrader/actions/workflows/ci.yml/badge.svg)](https://github.com/thammo4/StockTrader/actions)
# StockTrader

## Overview

StockTrader is a data pipeline for retrieving, processing, and analyzing stock market data with a focus on options pricing. The system fetches historical market data including options chains, dividend records, and risk-free rate benchmarks, then processes this data to create inputs for options pricing models.

## Directory Structure
```
StockTrader/
├── airflow/
│   └── dags/
│       └── ingest_options_chain_dag.py
├── data/
│   └── warehouse/
├── logs/
├── scripts/
├── src/
│   └── StockTrader
│       └── settings.py
│       └── tradier.py
│       └── freddy.py
└── tests/
└── utils/
│   └── dividend_table.py
|   └── parse_occ.py
```
