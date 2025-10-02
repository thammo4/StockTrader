--
-- FILE: `StockTrader/dbt/models/staging/fred/stg_fred__rates.sql`
--

with source as (
	select
		*
	from read_parquet('../data/warehouse/fred_af/*.parquet', filename=true)
)

select
	regexp_extract(filename, '([^/]+)\.parquet$', 1) as series_id,
	fred_date::date as rate_date,
	round(fred_rate, 2) as rate_pct,
	created_date::date as created_date
from source