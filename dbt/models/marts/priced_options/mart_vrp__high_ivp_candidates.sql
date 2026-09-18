--
-- FILE: `StockTrader/dbt/models/marts/mart_vrp__high_ivp_candidates`
--

{{ config(materialized='view') }}


--
-- IVP History
--

{% set ivp_lookback_months    = var('ivp_lookback_months', 12) %}
{% set min_iv_partition_n     = var('min_iv_partition_n', 30) %}
{% set min_iv_partition_dates = var('min_iv_partition_dates', 60) %}


--
-- Trading Activity Cutoffs
--

{% set min_volume               = var('min_volume', 5) %}
{% set min_open_interest        = var('min_open_interest', 40) %}
{% set min_bid_size             = var('min_bid_size', 8) %}
{% set min_ask_size             = var('min_ask_size', 8) %}
{% set max_bid_ask_spread_pct   = var('max_bid_ask_spread_pct', 0.09) %}
{% set min_time_value_bid_price = var('min_time_value_bid_price', 0) %}


--
-- Volatility Risk Premium Cutoffs
--

{% set min_vrp_ratio  = var('min_vrp_ratio', 1.15) %}
{% set min_vrp_spread = var('min_vrp_spread', 0.04) %}
{% set min_iv_cdf     = var('min_iv_cdf', 0.80) %}


--
-- Contract Structure Cutoffs
--

{% set min_abs_delta = var('min_abs_delta', 0.125) %}
{% set max_abs_delta = var('max_abs_delta', 0.45) %}
{% set min_dte       = var('min_dte', 7) %}
{% set max_dte       = var('max_dte', 90) %}


--
-- Position Size Cutoffs
--
-- Coarse candidate-generation limit only.
-- Execution remains authoritative and applies its margin constraint
-- using current option buying power.
--

{% set max_margin_est = var('max_margin_est', 6000) %}


--
-- Symbol Count Cutoffs
--

{% set max_symbol_rank = var('max_symbol_rank', 3) %}


with bounds as (
	select
		max(market_date) as asof_date
	from {{ ref('int_options__calcs_vrp') }}
),

ivp_population as (
	select
		v.*
	from {{ ref('int_options__calcs_vrp') }} v
	cross join bounds b
	where v.market_date > b.asof_date - interval '{{ ivp_lookback_months }} months'
	and v.market_date <= b.asof_date
	and v.dte_bucket != 'leaps'
	and v.moneyness_category in ('atm', 'otm', 'deep_otm')
),

ivp as (
	select
		*,

		cume_dist() over (
			partition by
				symbol,
				option_type,
				moneyness_category,
				dte_bucket
			order by iv
		) as iv_cdf,

		count(*) over (
			partition by
				symbol,
				option_type,
				moneyness_category,
				dte_bucket
		) as iv_partition_n,

		count(distinct market_date) over (
			partition by
				symbol,
				option_type,
				moneyness_category,
				dte_bucket
		) as iv_partition_dates

	from ivp_population
),

todays_contracts as (
	select
		i.*
	from ivp i
	cross join bounds b
	where i.market_date = b.asof_date
	and i.expiry_date > b.asof_date
	and i.ttm_days between {{ min_dte }} and {{ max_dte }}
),

candidate_features as (
	select
		*,

		--
		-- Liquidity
		--

		(ask_price - bid_price)
			/ nullif((ask_price + bid_price) / 2.0, 0)
			as bid_ask_spread_pct,

		--
		-- One-contract exposure diagnostics
		--

		100.0 * spot_price
			as underlying_notional,

		100.0 * strike_price
			as strike_notional,

		case
			when option_type = 'put'
				then 100.0 * strike_price
			else null
		end as assignment_notional,

		100.0 * spot_price * abs(delta)
			as delta_notional,

		--
		-- Reg-T Margin Estimate
		--
		-- Mirrors:
		-- StockTrader.execution.filters.margin.estimate_margin_reg_t()
		--

		100.0 * greatest(
			0.20 * spot_price
				- case
					when option_type = 'call'
						then greatest(strike_price - spot_price, 0)
					else greatest(spot_price - strike_price, 0)
				end
				+ mid_price,

			0.10 * case
				when option_type = 'call'
					then spot_price
				else strike_price
			end
				+ mid_price
		) as margin_est

	from todays_contracts
),

tradable_cutoffs as (
	select
		*
	from candidate_features
	where volume > {{ min_volume }}
	and open_interest > {{ min_open_interest }}
	and bid_size > {{ min_bid_size }}
	and ask_size > {{ min_ask_size }}
	and bid_ask_spread_pct < {{ max_bid_ask_spread_pct }}
	and time_value_bid_price > {{ min_time_value_bid_price }}
),

-- iv_cutoffs as (
-- 	select
-- 		*
-- 	from tradable_cutoffs
-- 	where vrp_ratio > {{ min_vrp_ratio }}
-- 	and vrp_spread > {{ min_vrp_spread }}
-- 	and iv_cdf > {{ min_iv_cdf }}
-- 	and iv_partition_n > {{ min_iv_partition_n }}
-- 	and iv_partition_dates >= {{ min_iv_partition_dates }}
-- ),

-- delta_cutoffs as (
-- 	select
-- 		*
-- 	from iv_cutoffs
-- 	where abs(delta) between {{ min_abs_delta }} and {{ max_abs_delta }}
-- ),

-- position_size_cutoffs as (
-- 	select
-- 		*,

-- 		credit_price
-- 			/ nullif(margin_est, 0)
-- 			as credit_on_margin

-- 	from delta_cutoffs
-- 	where margin_est <= {{ max_margin_est }}
-- ),

-- symbol_count_cutoffs as (
-- 	select
-- 		*,

-- 		row_number() over (
-- 			partition by symbol
-- 			order by
-- 				margin_est asc,
-- 				iv_cdf desc,
-- 				vrp_spread desc,
-- 				bid_ask_spread_pct asc,
-- 				open_interest desc
-- 		) as symbol_rank

-- 	from position_size_cutoffs

-- 	qualify symbol_rank <= {{ max_symbol_rank }}
-- )

iv_cutoffs as (
	select
		*
	from tradable_cutoffs
	where vrp_ratio > {{ min_vrp_ratio }}
	and vrp_spread > {{ min_vrp_spread }}
	and iv_cdf > {{ min_iv_cdf }}
	and iv_partition_n > {{ min_iv_partition_n }}
	and iv_partition_dates >= {{ min_iv_partition_dates }}
),

delta_cutoffs as (
	select
		*
	from iv_cutoffs
	where abs(delta) between {{ min_abs_delta }} and {{ max_abs_delta }}
),

position_size_cutoffs as (
	select
		*,
		credit_price
			/ nullif(margin_est, 0)
			as credit_on_margin
	from delta_cutoffs
	where margin_est <= {{ max_margin_est }}
),

symbol_count_cutoffs as (
	select
		*,
		row_number() over (
			partition by symbol
			order by
				margin_est asc,
				iv_cdf desc,
				vrp_spread desc,
				bid_ask_spread_pct asc,
				open_interest desc
		) as symbol_rank
	from position_size_cutoffs
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
	round(bid_ask_spread_pct, 4) as bid_ask_spread_pct,

	round(sigma, 4) as sigma,
	risk_free_rate,
	round(dividend_yield_annualized, 4) as div_yield_ann,

	moneyness_ratio as mnys_ratio,
	moneyness_ratio_log as mnys_log,
	moneyness_standardized as mnys_std,

	round(npv, 2) as npv,
	round(delta, 4) as delta,
	round(gamma, 4) as gamma,
	round(theta, 4) as theta,
	round(iv, 4) as iv,

	round(vrp_spread, 4) as vrp_spread,
	round(vrp_ratio, 4) as vrp_ratio,

	round(spread_price, 2) as spread_price,
	round(xaction_price, 2) as xaction_price,
	round(credit_price, 2) as credit_price,

	round(iv_cdf, 4) as iv_cdf,
	iv_partition_n,
	iv_partition_dates,

	round(underlying_notional, 2) as underlying_notional,
	round(strike_notional, 2) as strike_notional,
	round(assignment_notional, 2) as assignment_notional,
	round(delta_notional, 2) as delta_notional,
	round(margin_est, 2) as margin_est,
	round(credit_on_margin, 4) as credit_on_margin,

	symbol_rank,

	'sell_to_open' as order_side,
	mid_price as entry_price,
	'mid_price' as entry_price_ref,
	1 as quantity

from symbol_count_cutoffs

order by
	symbol,
	symbol_rank