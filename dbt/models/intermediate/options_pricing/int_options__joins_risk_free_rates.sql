--
-- FILE: `StockTrader/dbt/models/intermediate/options_pricing/int_options__joins_risk_free_rates.sql`
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
		ask_size,
		spot_price,
		sigma
	from {{ ref('int_options__joins_spots_and_vols') }}
	where sigma is not null
	{% if is_incremental() %}
	and market_date > (select max(market_date) from {{ this }})
	{% endif %}
),

market_date_rates as (
	select
		market_date,
		risk_free_rate,
		rate_date_ref
	from {{ ref('int_dim__daily_risk_free') }}
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
	o.spot_price,
	o.sigma,
	m.risk_free_rate,
	m.rate_date_ref
from options_base o
left join market_date_rates m using (market_date)
