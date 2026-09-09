--
-- FILE: `StockTrader/dbt/models/marts/mart_vrp__calcs_vrp_ezread`
--

{{ config(materialized='view') }}

select
	market_date,
	symbol,
	occ,
	option_type,
	expiry_date,
	ttm_days,

	round(strike_price,2) as K,
	round(spot_price,2) as S,
	round(mid_price,2) as pm,
	round(bid_price,2) as pb,
	round(ask_price,2) as pa,
	round(intrinsic_price,2) as pi,
	round(time_value_mid_price,2) as tv_mid,
	round(time_value_bid_price,2) as tv_bid,
	round(time_value_ask_price,2) as tv_ask,

	volume,
	open_interest as oi,
	bid_size::integer as bid_size,
	ask_size::integer as ask_size,

	round(sigma,4) as sigma,
	round(risk_free_rate,4) as r,
	round(dividend_yield_annualized,4) as q,

	round(moneyness_ratio,4) as mnys_ratio,
	round(moneyness_ratio_log, 4) mnys_log,
	round(moneyness_standardized,4) as mnys_std,
	moneyness_category as mnys,

	round(npv,2) as npv,
	round(delta,4) as delta,
	round(gamma,4) as gamma,
	round(theta,4) as theta,
	round(iv,4) as iv,

	dte_bucket,

	round(vrp_spread,4) as vrps,
	round(vrp_ratio,4) as vrpr,

	round(spread_price,2) as ps,
	round(xaction_price,2) as xs,
	round(credit_price,2) as pc

from {{ ref('int_options__calcs_vrp') }}
