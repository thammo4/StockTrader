--
-- FILE: `StockTrader/dbt/models/marts/options_pricing/bopm/mart_bopm__pays_dividends.sql`
--

{{ config(
	materialized='view',
	description='BOPM-with-dividends pricing model arguments for export to Python/QuantLib'
) }}

select
	-- Contract Identifiers
	market_date,
	symbol,
	occ,
	option_type,
	expiry_date,

	-- Options Pricing Model Parameters (S, K, r, q, σ, T)
	spot_price as S,
	strike_price as K,
	risk_free_rate as r,
	dividend_yield_annualized as q,
	sigma as σ,
	ttm_years as T,

	-- Market Prices
	mid_price as p_m,
	bid_price as p_b,
	ask_price as p_a,

	-- Intrinsic/Time Value
	intrinsic_price as p_i,
	time_value_mid_price as p_tm,
	time_value_bid_price as p_tb,
	time_value_ask_price as p_ta,

	-- Moneyness
	moneyness_ratio as mnys,
	moneyness_category as mnys_cat,

	-- Liquidity/Market Activity
	volume,
	open_interest as oi,

	-- Audit References
	rate_date_ref,
	dividend_date_ref,
	dividend_yield_ttm
from {{ ref('int_options__calcs_moneyness') }}

-- Filter Conditions:
-- 	1. Mart Model for Dividend-Bearing Stocks
-- 	2. Ensure no-arbitrage assumption not violated by quote sync/lag or malformed data
-- 	3. Ensure non-zero number of days between market date and expiry to appease quantlib pricing engine
where
	dividend_status = 'yes_dividend'
	and is_negative_ask_time_value = false
	and ttm_days >= 1
