--
-- FILE: `StockTrader/dbt/models/marts/mart_vrp__high_ivp_candidates`
--

{{ config(materialized='view') }}


--
-- Trading Activity Cutoffs
--

{% set min_volume = 12 %}
{% set min_open_interest = 105 %}
{% set min_bid_size = 12 %}
{% set min_ask_size = 12 %}
{% set min_time_value_bid_price = 0 %}
{% set max_bid_ask_spread_pct = 0.09 %}


--
-- IV Cutoffs
--

{% set min_vrp_ratio = 1.095 %}
{% set min_vrp_spread = 0.095 %}
{% set min_iv_cdf = 0.8925 %}
{% set min_iv_partition_n = 30 %}


--
-- Moneyness Cutoffs
--

{% set min_abs_delta = 0.455 %}
{% set max_abs_delta = 0.545 %}


--
-- Symbol Count Cutoffs
--

{% set max_symbol_rank = 2 %}


with ivp_population as (
	select
		*
	from {{ ref('int_options__calcs_vrp') }}
	where dte_bucket != 'leaps'
	and moneyness_category not in ('deep_itm', 'deep_otm')
),

ivp as (
	select
		*,
		cume_dist() over (partition by symbol, option_type, moneyness_category, dte_bucket order by iv) as iv_cdf,
		count(*) over (partition by symbol, option_type, moneyness_category, dte_bucket) as iv_partition_n
	from ivp_population
),

todays_contracts as (
	select
		*
	from ivp
	where market_date = (select max(market_date) from ivp)
	and expiry_date > current_date
),

tradable_cutoffs as (
	select
		*
	from todays_contracts
	where volume > {{ min_volume }}
	and open_interest > {{ min_open_interest }}
	and bid_size > {{ min_bid_size }}
	and ask_size > {{ min_ask_size }}
	and (ask_price-bid_price)/nullif(bid_price,0) < {{ max_bid_ask_spread_pct }}
	and time_value_bid_price > 0
),

iv_cutoffs as (
	select
		*
	from tradable_cutoffs
	where vrp_ratio > {{ min_vrp_ratio }}
	and vrp_spread > {{ min_vrp_spread }}
	and iv_cdf > {{ min_iv_cdf }}
	and iv_partition_n > {{ min_iv_partition_n }}
),

delta_cutoffs as (
	select
		*
	from iv_cutoffs
	where abs(delta) between {{ min_abs_delta }} and {{ max_abs_delta }}
),

symbol_count_cutoffs as (
	select
		*,
		row_number() over (partition by symbol order by iv_cdf desc, vrp_spread desc) as symbol_rank
	from delta_cutoffs
	qualify symbol_rank <= {{ max_symbol_rank }}
)

select
	market_date,
	symbol,
	option_type,
	moneyness_category as mnys,
	dte_bucket,
	occ,
	expiry_date,
	ttm_days as dte,
	strike_price,
	spot_price,
	mid_price,
	bid_price,
	ask_price,
	intrinsic_price,
	time_value_mid_price as tv_mid,
	time_value_bid_price as tv_bid,
	time_value_ask_price as tv_ask,
	volume,
	open_interest as oi,
	bid_size,
	ask_size,
	round(sigma,4) as sigma,
	risk_free_rate,
	round(dividend_yield_annualized,4) as div_yield_ann,
	moneyness_ratio as mnys_ratio,
	moneyness_ratio_log as mnys_log,
	moneyness_standardized mnys_std,
	round(npv,2) as npv,
	round(delta,4) as delta,
	round(gamma,4) as gamma,
	round(theta,4) as theta,
	round(iv,4) as iv,
	round(vrp_spread,4) as vrp_spread,
	round(vrp_ratio,4) as vrp_ratio,
	round(spread_price,2) as spread_price,
	round(xaction_price,2) as xaction_price,
	round(credit_price,2) as credit_price,
	round(iv_cdf,4) as iv_cdf,
	iv_partition_n,
	symbol_rank,
	'sell_to_open' as order_side,
	bid_price as entry_price,
	'bid_price' as entry_price_ref
from symbol_count_cutoffs
order by
	symbol,
	option_type,
	moneyness_category,
	dte_bucket,
	iv_cdf desc
