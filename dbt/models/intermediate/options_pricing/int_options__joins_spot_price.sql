--
-- FILE: `StockTrader/dbt/models/intermediate/options_pricing/int_options__joins_spot_price.sql
--

-- Constructing Options Pricing Model Inputs
-- Early Intermediate Layer Join
-- (Options Data) <- (OHLCV Data: close_price)

{{ config(
	materialized="incremental",
	incremental_strategy="append"
) }}

with base_options as (
	select
		symbol,
		occ,
		option_type,
		expiry_date,
		ttm_days,
		strike_price,
		bid_price,
		ask_price,
		mid_price,
		volume,
		open_interest,
		bid_size,
		ask_size,
		created_date as market_date
	from {{ ref('stg_tradier__options') }}
	{% if is_incremental() %}
	where created_date > (select max(market_date) from {{ this }})
	{% endif %}
),

underlying_prices as (
	select
		symbol,
		close_price as spot_price,
		market_date
	from {{ ref('stg_tradier__ohlcv_bars') }}
)

select
	o.market_date,
	o.symbol,
	o.occ,
	o.option_type,
	o.expiry_date,
	o.ttm_days,
	o.strike_price,
	o.bid_price,
	o.ask_price,
	o.mid_price,
	o.volume,
	o.open_interest,
	o.bid_size,
	o.ask_size,
	u.spot_price
from base_options o
left join underlying_prices u
	on o.symbol = u.symbol
	and o.market_date = u.market_date
