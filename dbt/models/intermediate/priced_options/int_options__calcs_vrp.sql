--
-- FILE: `StockTrader/dbt/models/intermediate/priced_options/int_options__calcs_vrp.sql`
--

{{ config(
	materialized='incremental',
	incremental_strategy='append'
) }}

--
-- Assign DTE Bucket Classification + Compute Raw VRP Signals + Xaction Costs
--

-- DTE Buckets match sigma horizons defined in int_ohlcv__calcs_rolling_vols
-- weekly: 1-7 DTE -> sigma_5d
-- monthly: 8-30 DTE -> sigma_21d
-- intermediate: 31-60 DTE -> sigma_42d
-- quarterly: 61-120 DTE -> sigma_63d
-- leaps: 121+ DTE -> sigma_252d

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

dte_buckets as (
	select
		*,
		case
			when ttm_days between 1 and 7 then 'weekly'
			when ttm_days between 8 and 30 then 'monthly'
			when ttm_days between 31 and 60 then 'intermediate'
			when ttm_days between 61 and 120 then 'quarterly'
			else 'leaps'
		end as dte_bucket
	from options_base
),

vrp_metrics as (
	select
		*,
		iv-sigma as vrp_spread,
		iv/nullif(sigma,0) as vrp_ratio
	from dte_buckets
),

xaction_prices as (
	select
		*,
		100*(ask_price-bid_price) as spread_price,
		100*(ask_price-bid_price) + .70 as xaction_price,
		100*bid_price as credit_price
	from vrp_metrics
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
	dte_bucket,
	vrp_spread,
	vrp_ratio,
	spread_price,
	xaction_price,
	credit_price
from xaction_prices