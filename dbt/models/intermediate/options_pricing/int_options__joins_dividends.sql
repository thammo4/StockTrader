--
-- FILE: `StockTrader/dbt/models/intermediate/options_pricing/int_options__joins_dividends.sql`
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
		sigma,
		risk_free_rate,
		rate_date_ref
	from {{ ref('int_options__joins_risk_free_rates') }}
	{% if is_incremental() %}
	where market_date > (select max(market_date) from {{ this }})
	{% endif %}
),

dividends as (
	select
		market_date,
		symbol,
		prev_ex_dividend_date,
		prev_cash_amount,
		prev_frequency,
		dividend_cash_ttm,
		is_complete_dividend_history
	from {{ ref('int_dividends__maps_to_daily') }}
),

joined_options_dividends as (
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
		o.risk_free_rate,
		o.rate_date_ref,

		d.prev_ex_dividend_date as dividend_date_ref,
		d.is_complete_dividend_history,
		d.dividend_cash_ttm / o.spot_price as dividend_yield_ttm,
		
		case
			when not d.is_complete_dividend_history then 0.0
			when d.prev_frequency is null then null
			else (d.prev_cash_amount*d.prev_frequency)/o.spot_price
		end as dividend_yield_annualized,

		case
			when d.is_complete_dividend_history = true then 'yes_dividend'
			else 'no_dividend'
		end as dividend_status
	from options_base o
	join dividends d using (market_date, symbol)
)

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
	sigma,
	risk_free_rate,
	rate_date_ref,
	dividend_yield_ttm,
	dividend_yield_annualized,
	dividend_status,
	is_complete_dividend_history,
	dividend_date_ref
from joined_options_dividends
