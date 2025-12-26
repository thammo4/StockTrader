--
-- FILE: `StockTrader/dbt/models/intermediate/dimensions/int_dim__symbols_market_dates.sql`
--

{{ config(
	materialized='incremental',
	incremental_strategy='append'
) }}

with symbol_dates as (
	select
		distinct market_date,
		symbol
	from {{ ref('int_options__joins_risk_free_rates') }}
	{% if is_incremental() %}
	where market_date > (select max(market_date) from {{ this }})
	{% endif %}
)

select
	market_date,
	symbol
from symbol_dates