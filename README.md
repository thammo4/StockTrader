[![CI Status](https://github.com/thammo4/StockTrader/actions/workflows/ci.yml/badge.svg)](https://github.com/thammo4/StockTrader/actions)
[![dbt CI](https://github.com/thammo4/StockTrader/actions/workflows/dbt-ci.yml/badge.svg)](https://github.com/thammo4/StockTrader/actions/workflows/dbt-ci.yml)
# StockTrader

## Overview

StockTrader is a data pipeline for retrieving, processing, and analyzing stock market data with a focus on options pricing. The system fetches historical market data including options chains, dividend records, and risk-free rate benchmarks, then processes this data to create inputs for options pricing models.

## Directory Structure
```
StockTrader/
├── Dockerfile
├── docker-compose.yml
├── .github/
│   └── scripts/
│       └── dbt-generate-test-data.py
│   └── workflows/
│       └── ci.yml
│       └── dbt-ci.yml
├── airflow/
│   └── dags/
│       └── ingest_fred_rates_dag.py
│       └── ingest_tradier_dividends_dag.py
│       └── ingest_tradier_options_dag.py
│       └── ingest_tradier_quotes_dag.py
├── dbt/
│   └── Dockerfile
│   └── dbt_project.yml
│   └── profiles.yml
│   └── package-lock.yml
│   └── packages.yml
│   └── macros/
│       └── occ_utils.sql
│   └── models/
│       └── intermediate/
│       	  └── int_symbols__active_status.sql
│       	  └── data_quality/
│           	  └── int_options__data_quality_metrics.sql
│       	  └── dimensions/
│           	  └── int_dim__daily_dividends.sql
│           	  └── int_dim__daily_risk_free.sql
│           	  └── int_dim__market_dates.sql
│           	  └── int_dim__symbols_market_dates.sql
│       	  └── options_pricing/
│           	  └── int_ohlcv__rolling_vol.sql
│           	  └── int_options__calcs_moneyness.sql
│           	  └── int_options__filters_bad_prices.sql
│           	  └── int_options__joins_dividends.sql
│           	  └── int_options__joins_spots_and_vols.sql
│       	  └── reference/
│           	  └── int_quotes__root_symbols_map.sql
│       └── marts/
│       	  └── options_pricing/
│                  └── bopm/
│                       └── mart_bopm_pays_dividends.sql
│       └── staging/
│       	  └── sources.yml
│       	  └── fred/
│                  └── schema.yml
│                  └── stg_fred__rates.sql
│       	  └── tradier/
│               └── schema.yml
│           	  └── stg_tradier__dividends.sql
│           	  └── stg_tradier__ohlcv_bars.sql
│           	  └── stg_tradier__options.sql
│           	  └── stg_tradier__quotes.sql
│       	  └── qlib_priced/
│           	  └── schema.yml
│           	  └── sources.yml
│           	  └── stg_qlib_priced__outputs.sql
│   └── tests/
│       └── assert_options_market_prices_consistent.sql
│       └── assert_options_market_prices_sufficient.sql
│       └── assert_options_occ_id_valid.sql
│       └── assert_options_pricing_inputs_valid.sql
│       └── assert_options_underlying_map_valid.sql
├── data/
│   └── warehouse/
│       └── stocktrader_analytics_dev.duckdb
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
├── infrastructure/
│   └── redis/
│       └── job_producer.py
│       └── job_schema.py
│       └── job_worker.py
│   └── terraform/
│       └── main.tf
│       └── outputs.tf
│       └── variables.tf
│       └── versions.tf
│       └── scripts/
│           └── gcp_vm_debian.sh
├── logs/
├── scripts/
│   └── create_dividend_parquet.py
│   └── create_fred_parquet.py
│   └── create_ohlcv_parquet.py
│   └── ddb_minio_batch_export.sh
│   └── fetch_active_options.py
│   └── fetch_active_params.py
│   └── generate_dbt_universe_seed.sh
│   └── ingest_fred_rates.py
│   └── ingest_tradier_dividends.py
│   └── ingest_tradier_options.py
│   └── ingest_tradier_quotes.py
│   └── prep_bopm_data.py
│   └── price_bopm_data.py
├── src/
│   └── StockTrader/
│       └── settings.py
│       └── tradier.py
│       └── freddy.py
│       └── pricing/
│           └── base.py
│           └── batch.py
│           └── errors.py
│           └── registry.py
│           └── types.py
│           └── qlib/
│               └── builders.py
│               └── context.py
│               └── implied_vol.py
│               └── models/
│               	└── crr_bopm_amr_divs.py
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
