--
-- FILE: `StockTrader/dbt/models/intermediate/options_pricing/int_options__filters_bad_prices.sql
--

{{ config(
	materialized='incremental',
	incremental_strategy='append'
) }}

with source as (
	select
		created_date as market_date,
		symbol,
		occ,
		option_type,
		expiry_date,
		ttm_days,
		strike_price,
		bid_price,
		ask_price,
		mid_price,
		bid_ask_spread,
		volume,
		open_interest,
		bid_size,
		ask_size
	from {{ ref('stg_tradier__options') }}
	where is_valid_price = true
	{% if is_incremental() %}
	and created_date > (select max(market_date) from {{ this }})
	{% endif %}
)

select
	market_date,
	symbol,
	occ,
	option_type,
	expiry_date,
	ttm_days,
	strike_price,
	bid_price,
	ask_price,
	mid_price,
	bid_ask_spread,
	volume,
	open_interest,
	bid_size,
	ask_size
from source
