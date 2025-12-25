--
-- FILE: `StockTrader/dbt/models/intermediate/options_pricing/int_options__joins_spots_and_vols.sql`
--

{{ config(
	materialized='incremental',
	incremental_strategy='append'
) }}

with options_base as (
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
	from {{ ref('int_options__filters_bad_prices') }}
	{% if is_incremental() %}
	where market_date > (select max(market_date) from {{ this }})
	{% endif %}
),

underlying_price_and_rolling_vol as (
	select
		market_date,
		symbol,
		close_price as spot_price,
		vol_rolling_day_annualized as sigma,
		vol_is_valid_window as sigma_is_valid
	from {{ ref('int_ohlcv__rolling_vol') }}
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
	o.bid_ask_spread,
	o.volume,
	o.open_interest,
	o.bid_size,
	o.ask_size,

	u.spot_price,
	u.sigma,
	u.sigma_is_valid
from options_base o
left join underlying_price_and_rolling_vol u
on o.symbol = u.symbol
and o.market_date = u.market_date
