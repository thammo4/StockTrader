--
-- FILE: `StockTrader/dbt/models/intermediate/dimensions/int_dim__market_dates.sql`
--

{{ config(materialized='incremental', incremental_strategy='append') }}

with source as (
	select
		distinct market_date
	from {{ ref('int_options__data_quality_metrics') }}
	{% if is_incremental() %}
	where market_date > (select max(market_date) from {{ this }})
	{% endif %}
)

select
	market_date
from source
