--
-- FILE: `StockTrader/dbt/intermediate/data_quality/int_options__calcs_pricing_metrics.sql
--

--
-- Assess QLib pricing outputs relative to market data
--

{{ config(
	materialized='incremental',
	incremental_strategy='append'
) }}

with source as (
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
		volume,
		open_interest,
		bid_size,
		ask_size,
		sigma,
		risk_free_rate,
		dividend_yield_annualized,
		moneyness_ratio,
		moneyness_ratio_log,
		moneyness_standardized,
		moneyness_category,
		npv,
		delta,
		gamma,
		theta,
		iv
	from {{ ref('int_options__joins_qlib_priced') }}
	{% if is_incremental() %}
	where market_date > (select max(market_date) from {{ this }})
	{% endif %}
),

--
-- NPV Calculation Accuracy
--

npv_quality_metrics as (
	select
		*,
		round(npv-mid_price,4) as npv_err,
		round((npv-mid_price)/mid_price,4) as npv_err_pct,
		bid_price <= npv and npv <= ask_price as npv_in_spread
	from source
),

--
-- Standardized Theta Computations
--

theta_normalized as (
	select
		*,
		round(theta*365, 4) as theta_annualized,
		round(theta / nullif(mid_price,0), 6) as theta_pct_mid,
		round(theta / nullif(spot_price,0), 6) as theta_pct_spot
	from npv_quality_metrics
),

--
-- Standardized Gamma Computations
--

gamma_normalized as (
	select
		*,
		round(gamma*spot_price / 100.0, 6) as gamma_pct
	from theta_normalized
),

--
-- Liquidity
--

liquidity_filtered as (
	select
		*,
		round((ask_price-bid_price)/nullif(mid_price,0),6) as bid_ask_spread_pct
	from gamma_normalized
)

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
	volume,
	open_interest,
	bid_size,
	ask_size,
	sigma,
	risk_free_rate,
	dividend_yield_annualized,
	moneyness_ratio,
	moneyness_ratio_log,
	moneyness_standardized,
	moneyness_category,
	npv,
	delta,
	gamma,
	theta,
	iv,
	npv_err,
	npv_err_pct,
	npv_in_spread,
	theta_annualized,
	theta_pct_mid,
	theta_pct_spot,
	gamma_pct,
	bid_ask_spread_pct
from liquidity_filtered
