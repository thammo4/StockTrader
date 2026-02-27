--
-- FILE: `StockTrader/dbt/models/intermediate/priced_options/int_priced__materialized.sql`
--

{{ config(
	materialized='incremental',
	incremental_strategy='append',
	description='Materialized, metadata-stripped pricing outputs for bopm_dividends population. Filters to analytically viable records (ok, iv_fail). Eliminates staging view resolution overhead for downstream joins.'
) }}

with source as (
	select
		market_date,
		occ,
		npv,
		delta,
		gamma,
		theta,
		iv,
		pricing_status
	from {{ ref('stg_qlib_priced__outputs') }}
	where pricing_status in ('ok', 'iv_fail')
	{% if is_incremental() %}
		and market_date > (select max(market_date) from {{ this }})
	{% endif %}
)

select * from source


-- (venv12) rudi@rudi StockTrader % colima list
-- PROFILE    STATUS     ARCH       CPUS    MEMORY    DISK      RUNTIME    ADDRESS
-- default    Running    aarch64    6       6GiB      100GiB    docker     
-- (venv12) rudi@rudi StockTrader % docker exec -it stocktrader-dbt-1 bash
-- root@1d609c407568:/opt/stocktrader/dbt# dbt run --select int_priced__materialized
-- 03:18:40  Running with dbt=1.7.19
-- 03:18:40  Registered adapter: duckdb=1.7.5
-- 03:18:41  Found 23 models, 98 tests, 1 seed, 6 sources, 0 exposures, 0 metrics, 787 macros, 0 groups, 0 semantic models
-- 03:18:41  
-- 03:18:41  Concurrency: 6 threads (target='dev')
-- 03:18:41  
-- 03:18:41  1 of 1 START sql incremental model main_intermediate.int_priced__materialized .. [RUN]
-- 03:18:55  1 of 1 OK created sql incremental model main_intermediate.int_priced__materialized  [OK in 13.89s]
-- 03:18:55  
-- 03:18:55  Finished running 1 incremental model in 0 hours 0 minutes and 14.22 seconds (14.22s).
-- 03:18:55  
-- 03:18:55  Completed successfully
-- 03:18:55  
-- 03:18:55  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1

-- root@1d609c407568:/opt/stocktrader/dbt# dbt run --select int_options__joins_qlib_priced
-- 03:25:51  Running with dbt=1.7.19
-- 03:25:51  Registered adapter: duckdb=1.7.5
-- 03:25:52  Found 23 models, 98 tests, 1 seed, 6 sources, 0 exposures, 0 metrics, 787 macros, 0 groups, 0 semantic models
-- 03:25:52  
-- 03:25:53  Concurrency: 6 threads (target='dev')
-- 03:25:53  
-- 03:25:53  1 of 1 START sql incremental model main_intermediate.int_options__joins_qlib_priced  [RUN]
-- Killed