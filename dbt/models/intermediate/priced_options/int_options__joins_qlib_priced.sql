--
-- FILE: `StockTrader/dbt/models/intermediate/priced_options/int_options__joins_qlib_priced.sql`
--

{{ config(
	materialized='incremental',
	incremental_strategy='append',
	description='Filtered QuantLib pricing outputs (pricing_status=ok), keyed by (market_date, occ).'
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
		spot_price,
		mid_price,
		bid_price,
		ask_price,
		intrinsic_price,
		time_value_mid_price,
		time_value_bid_price,
		time_value_ask_price,
		volume::INT as volume,
		open_interest::INT as open_interest,
		bid_size,
		ask_size,
		sigma,
		risk_free_rate,
		dividend_yield_annualized,
		moneyness_ratio,
		moneyness_ratio_log,
		moneyness_standardized,
		moneyness_category
	from {{ ref('int_options__calcs_moneyness') }}
	where dividend_status = 'yes_dividend'
	  and is_negative_ask_time_value = false
	  and ttm_days >= 1
	{% if is_incremental() %}
		and market_date > (select max(market_date) from {{ this }})
	{% endif %}
),

qlib_priced_data as (
	select
		market_date,
		occ,
		npv,
		delta,
		gamma,
		theta,
		iv
	from {{ ref('int_priced__materialized') }}
	where pricing_status = 'ok'
	{% if is_incremental() %}
		and market_date > (select max(market_date) from {{ this }})
	{% endif %}
)


select
	o.market_date,
	o.symbol,
	o.occ,
	o.option_type,
	o.expiry_date,
	o.ttm_days,
	o.strike_price,
	o.spot_price,
	o.mid_price,
	o.bid_price,
	o.ask_price,
	o.intrinsic_price,
	o.time_value_mid_price,
	o.time_value_bid_price,
	o.time_value_ask_price,
	o.volume,
	o.open_interest,
	o.bid_size,
	o.ask_size,
	o.sigma,
	o.risk_free_rate,
	o.dividend_yield_annualized,
	o.moneyness_ratio,
	o.moneyness_ratio_log,
	o.moneyness_standardized,
	o.moneyness_category,
	q.npv,
	q.delta,
	q.gamma,
	q.theta,
	q.iv
from options_base o
join qlib_priced_data q using (market_date, occ)
