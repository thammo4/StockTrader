[![CI Status](https://github.com/thammo4/StockTrader/actions/workflows/ci.yml/badge.svg)](https://github.com/thammo4/StockTrader/actions)
[![dbt CI](https://github.com/thammo4/StockTrader/actions/workflows/dbt-ci.yml/badge.svg)](https://github.com/thammo4/StockTrader/actions/workflows/dbt-ci.yml)
# StockTrader

## Overview

StockTrader is a data pipeline for retrieving, processing, and analyzing stock market data with a focus on options pricing. The system fetches historical market data including options chains, dividend records, and risk-free rate benchmarks, then processes this data to create inputs for options pricing models.

## Directory Structure

```
StockTrader/
├── .dockerignore
├── .gitattributes
├── .gitignore
├── .gitmodules
├── Dockerfile
├── docker-compose.yml
├── LICENSE
├── pyproject.toml
├── README.md
├── cron_ingest_options_chain.sh
├── largecap_all.txt
├── .github/
│   ├── scripts/
│   │   └── dbt-generate-test-data.py
│   └── workflows/
│       ├── ci.yml
│       └── dbt-ci.yml
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       ├── _bootstrap.py
│       ├── ingest_fred_rates_dag.py
│       ├── ingest_tradier_dividends_dag.py
│       ├── ingest_tradier_ohlcv_bars_dag.py
│       ├── ingest_tradier_options_dag.py
│       └── ingest_tradier_quotes_dag.py
├── data/
│   ├── MMM_bopm_data.parquet/
│   ├── MMM_date_range.txt
│   ├── MMM_dividend_data.parquet
│   ├── MMM_ohlcv_bar_data.parquet
│   ├── MMM_options_data.parquet
│   ├── TEST_bopm_data.parquet/
│   ├── TEST_date_range.txt
│   ├── TEST_dividend_data.parquet
│   ├── TEST_ohlcv_bar_data.parquet
│   ├── TEST_options_data.parquet
│   └── warehouse/
│       ├── stocktrader_analytics_dev.duckdb
│       ├── stocktrader_analytics_prod.duckdb
│       ├── account_af/
│       │   └── <ACCOUNTID>.parquet
│       ├── fred_af/
│       │   └── <SERIESID>.parquet
│       ├── dividends_af/
│       │   └── <SYMBOL>.parquet
│       ├── options_af/
│       │   └── <SYMBOL>.parquet
│       ├── quotes_af/
│       │   └── <SYMBOL>.parquet
│       └── ohlcv_bars/
│           └── <SYMBOL>.parquet
├── dbt/
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── package-lock.yml
│   ├── packages.yml
│   ├── macros/
│   │   └── occ_utils.sql
│   ├── models/
│   │   ├── intermediate/
│   │   │   ├── data_quality/
│   │   │   │   ├── int_options__calcs_pricing_metrics.sql
│   │   │   │   └── int_options__data_quality_metrics.sql
│   │   │   ├── mappings/
│   │   │   │   ├── int_dividends__maps_to_daily.sql
│   │   │   │   └── int_risk_free_rates__maps_to_daily.sql
│   │   │   ├── options_pricing/
│   │   │   │   ├── int_ohlcv__calcs_rolling_vols.sql
│   │   │   │   ├── int_options__calcs_moneyness.sql
│   │   │   │   ├── int_options__creates_base_dset.sql
│   │   │   │   ├── int_options__joins_dividends.sql
│   │   │   │   ├── int_options__joins_risk_free_rates.sql
│   │   │   │   └── int_options__joins_spots_and_vols.sql
│   │   │   ├── priced_options/
│   │   │   │   ├── int_options__calcs_ivp.sql
│   │   │   │   ├── int_options__calcs_vrp.sql
│   │   │   │   └── int_options__joins_qlib_priced.sql
│   │   │   └── reference/
│   │   │       └── int_quotes__root_symbols_map.sql
│   │   ├── marts/
│   │   │   ├── options_pricing/
│   │   │   │   └── bopm/
│   │   │   │       └── mart_bopm__pays_dividends.sql
│   │   │   └── priced_options/
│   │   │       └── mart_vrp__high_ivp_candidates.sql
│   │   └── staging/
│   │       ├── sources.yml
│   │       ├── fred/
│   │       │   ├── schema.yml
│   │       │   └── stg_fred__rates.sql
│   │       ├── tradier/
│   │       │   ├── schema.yml
│   │       │   ├── stg_tradier__dividends.sql
│   │       │   ├── stg_tradier__ohlcv_bars.sql
│   │       │   ├── stg_tradier__options.sql
│   │       │   └── stg_tradier__quotes.sql
│   │       └── qlib_priced/
│   │           ├── schema.yml
│   │           ├── sources.yml
│   │           └── stg_qlib_priced__outputs.sql
│   ├── seeds/
│   │   └── universe/
│   │       └── largecap_all.csv
│   └── tests/
│       ├── assert_options_market_prices_consistent.sql
│       ├── assert_options_market_prices_sufficient.sql
│       ├── assert_options_occ_id_valid.sql
│       ├── assert_options_pricing_inputs_valid.sql
│       └── assert_options_underlying_map_valid.sql
├── dev/
│   ├── env_setup/
│   │   └── gcp_vm_debian.sh
│   └── pull_changes.sh
├── infrastructure/
│   ├── __init__.py
│   ├── redis/
│   │   ├── __init__.py
│   │   ├── job_producer.py
│   │   ├── job_schema.py
│   │   └── job_worker.py
│   └── terraform/
│       ├── main.tf
│       ├── outputs.tf
│       ├── variables.tf
│       ├── versions.tf
│       └── scripts/
│           └── gcp_vm_debian.sh
├── scripts/
│   ├── __init__.py
│   ├── create_dividend_parquet.py
│   ├── create_fred_parquet.py
│   ├── create_ohlcv_parquet.py
│   ├── ddb_minio_batch_export.sh
│   ├── ddb_minio_candidates_export.sh
│   ├── generate_dbt_universe_seed.sh
│   ├── ingest_fred_rates.py
│   ├── ingest_options_chain.py
│   ├── ingest_tradier_dividends.py
│   ├── ingest_tradier_ohlcv_bars.py
│   ├── ingest_tradier_options.py
│   ├── ingest_tradier_quotes.py
│   ├── launch_pricing_worker.sh
│   ├── minio_ddb_batch_import.sh
│   ├── skip_us_holidays.py
│   ├── dbt_bstrap/
│   │   └── int_options__joins_qlib_priced.sh
│   └── dbt_refresh/
│       ├── dbt_batch_load.sh
│       ├── groups/
│       │   ├── options_pricing.txt
│       │   └── priced_options.txt
│       └── models/
│           ├── int_options__calcs_moneyness.sh
│           ├── int_options__calcs_pricing_metrics.sh
│           ├── int_options__calcs_vrp.sh
│           ├── int_options__creates_base_dset.sh
│           ├── int_options__data_quality_metrics.sh
│           ├── int_options__joins_dividends.sh
│           ├── int_options__joins_qlib_priced.sh
│           ├── int_options__joins_risk_free_rates.sh
│           └── int_options__joins_spots_and_vols.sh
├── src/
│   ├── __init__.py
│   └── StockTrader/
│       ├── __init__.py
│       ├── settings.py
│       ├── tradier.py
│       ├── freddy.py
│       ├── execution/
│       │   ├── __init__.py
│       │   ├── dto.py
│       │   ├── executor.py
│       │   ├── main2.py
│       │   ├── orchestrator.py
│       │   ├── order_iface.py
│       │   ├── builders/
│       │   │   ├── __init__.py
│       │   │   └── template_builder.py
│       │   ├── filters/
│       │   │   ├── __init__.py
│       │   │   ├── constraints.py
│       │   │   ├── filtered_loader.py
│       │   │   ├── ksack.py
│       │   │   ├── margin.py
│       │   │   └── portfolio_state.py
│       │   ├── loaders/
│       │   │   ├── __init__.py
│       │   │   └── candidate_loader.py
│       │   ├── persisters/
│       │   │   ├── __init__.py
│       │   │   └── result_persister.py
│       │   └── templates/
│       │       ├── single_leg_option.yml
│       │       └── vrp_short.yml
│       ├── portfolio/
│       │   ├── __init__.py
│       │   ├── m2m.py
│       │   ├── monitor.py
│       │   ├── position_loader.py
│       │   └── position_quotes.py
│       └── pricing/
│           ├── __init__.py
│           ├── base.py
│           ├── batch.py
│           ├── errors.py
│           ├── registry.py
│           ├── types.py
│           └── qlib/
│               ├── __init__.py
│               ├── builders.py
│               ├── context.py
│               ├── implied_vol.py
│               └── models/
│                   ├── __init__.py
│                   └── crr_bopm_amr_divs.py
├── tests/
│   ├── __init__.py
│   ├── config.py
│   ├── test_freddy.py
│   ├── test_settings.py
│   ├── test_tradier.py
│   └── scripts/
│       ├── __init__.py
│       ├── test_create_dividend_parquet.py
│       ├── test_create_fred_parquet.py
│       └── test_create_ohlcv_parquet.py
├── utils/
│   ├── __init__.py
│   ├── dividend_table.py
│   ├── get_symbols.py
│   ├── minio_store.py
│   ├── parse_occ.py
│   └── write_atomic.py
└── vendors/
    └── US-Stock-Symbols/   # git submodule (rreichel3/US-Stock-Symbols)
```
