--
-- FILE: `StockTrader/dbt/models/intermediate/n_leg/int_options__pairs_vertical.sql
--

{{ config(
	materialized='incremental',
	incremental_strategy='append'
) }}

{% set max_ttm_days = 120 %}
{% set min_abs_delta = 0.03 %}
{% set max_abs_delta = 0.60 %}
{% set min_bid_price = 0.05 %}

{% set max_width_pct_spot = 0.10 %}


with legs as (
	select
		market_date,
		symbol,
		occ,
		option_type,
		expiry_date,
		ttm_days,
		dte_bucket,
		strike_price,
		spot_price,
		mid_price,
		bid_price,
		ask_price,
		time_value_bid_price,
		volume,
		open_interest,
		bid_size,
		ask_size,
		sigma,
		risk_free_rate,
		moneyness_category,
		moneyness_ratio,
		npv,
		delta,
		gamma,
		theta,
		iv,
		vrp_spread,
		vrp_ratio
	from {{ ref('int_options__calcs_vrp') }}
	where ttm_days <= {{ max_ttm_days }}
	and abs(delta) between {{ min_abs_delta }} and {{ max_abs_delta }}
	and bid_price >= {{ min_bid_price }}
	{% if is_incremental() %}
	and market_date > (select max(market_date) from {{ this }})
	{% endif %}
)


select
	-- common keys to both legs
	l1.market_date,
	l1.symbol,
	l1.option_type,
	l1.expiry_date,
	l1.ttm_days,
	l1.dte_bucket,
	l1.spot_price,
	l1.sigma,
	l1.risk_free_rate,

	l1.occ || '|' || l2.occ as pair_key,

	-- leg1 = low strike
	l1.occ as occ1,
	l1.strike_price as strike_price1,
	l1.mid_price as mid_price1,
	l1.bid_price as bid_price1,
	l1.ask_price as ask_price1,
	l1.time_value_bid_price as time_value_bid_price1,
	l1.volume as volume1,
	l1.open_interest as open_interest1,
	l1.bid_size as bid_size1,
	l1.ask_size as ask_size1,
	l1.moneyness_category as moneyness_category1,
	l1.moneyness_ratio as moneyness_ratio1,
	l1.npv as npv1,
	l1.delta as delta1,
	l1.gamma as gamma1,
	l1.theta as theta1,
	l1.iv as iv1,
	l1.vrp_spread as vrp_spread1,
	l1.vrp_ratio as vrp_ratio1,

	-- leg2 = high strike
	l2.occ as occ2,
	l2.strike_price as strike_price2,
	l2.mid_price as mid_price2,
	l2.bid_price as bid_price2,
	l2.ask_price as ask_price2,
	l2.time_value_bid_price as time_value_bid_price2,
	l2.volume as volume2,
	l2.open_interest as open_interest2,
	l2.bid_size as bid_size2,
	l2.ask_size as ask_size2,
	l2.moneyness_category as moneyness_category2,
	l2.moneyness_ratio as moneyness_ratio2,
	l2.npv as npv2,
	l2.delta as delta2,
	l2.gamma as gamma2,
	l2.theta as theta2,
	l2.iv as iv2,
	l2.vrp_spread as vrp_spread2,
	l2.vrp_ratio as vrp_ratio2,

	l2.strike_price - l1.strike_price as strike_width,
	(l2.strike_price - l1.strike_price) / l1.spot_price as strike_width_pct_spot,
	l2.mid_price - l1.mid_price as mid_price_diff,
	l2.iv - l1.iv as iv_diff,

from legs l1
join legs l2
	on l1.market_date = l2.market_date
	and l1.symbol = l2.symbol
	and l1.option_type = l2.option_type
	and l1.expiry_date = l2.expiry_date
	and l1.strike_price < l2.strike_price
	and (l2.strike_price - l1.strike_price) / l1.spot_price <= {{ max_width_pct_spot }}









