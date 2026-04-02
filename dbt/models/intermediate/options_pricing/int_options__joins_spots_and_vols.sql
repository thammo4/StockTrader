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
		mid_price,
		bid_price,
		ask_price,
		bid_ask_spread,
		volume::int as volume,
		open_interest::int as open_interest,
		bid_size,
		ask_size
	from {{ ref('int_options__creates_base_dset') }}
	{% if is_incremental() %}
	where market_date > (select max(market_date) from {{ this }})
	{% endif %}
),

underlying_vols as (
	select
		market_date,
		symbol,
		sigma_5d,
		sigma_21d,
		sigma_42d,
		sigma_63d,
		sigma_252d
	from {{ ref('int_ohlcv__calcs_rolling_vols') }}
),

underlying_spots as (
	select
		market_date,
		symbol,
		close_price as spot_price
	from {{ ref('stg_tradier__ohlcv_bars') }}
),

joined_options_underlyings as (
	select
		o.market_date,
		o.symbol,
		o.occ,
		o.option_type,
		o.expiry_date,
		o.ttm_days,
		o.strike_price,
		o.mid_price,
		o.bid_price,
		o.ask_price,
		o.bid_ask_spread,
		o.volume,
		o.open_interest,
		o.bid_size,
		o.ask_size,
		us.spot_price,
		case
			when o.ttm_days between 1 and 7 then uv.sigma_5d
			when o.ttm_days between 8 and 30 then uv.sigma_21d
			when o.ttm_days between 31 and 60 then uv.sigma_42d
			when o.ttm_days between 61 and 120 then uv.sigma_63d
			else uv.sigma_252d
		end as sigma,
		uv.sigma_5d,
		uv.sigma_21d,
		uv.sigma_42d,
		uv.sigma_63d,
		uv.sigma_252d
	from options_base o
	join underlying_spots us using (market_date, symbol)
	join underlying_vols uv using (market_date, symbol)
)

select
	market_date,
	symbol,
	occ,
	option_type,
	expiry_date,
	ttm_days,
	strike_price,
	mid_price,
	bid_price,
	ask_price,
	bid_ask_spread,
	volume,
	open_interest,
	bid_size,
	ask_size,
	spot_price,
	sigma,
	sigma_5d,
	sigma_21d,
	sigma_42d,
	sigma_63d,
	sigma_252d
from joined_options_underlyings
where sigma is not null
