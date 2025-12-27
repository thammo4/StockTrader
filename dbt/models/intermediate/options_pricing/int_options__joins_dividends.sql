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
		dividend_yield_annualized,
		dividend_yield_ttm,
		prev_ex_dividend_date as dividend_date_ref,
		case
			when prev_ex_dividend_date is null then 'no_dividend_history'
			when dividend_yield_annualized is null then 'no_yield_calc'
			when dividend_yield_annualized = 0.0 then 'zero_yield'
			else 'yes_dividend'
		end as dividend_status
	from {{ ref('int_dim__daily_dividends') }}
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
	o.risk_free_rate,
	o.rate_date_ref,
	d.dividend_yield_annualized,
	d.dividend_yield_ttm,
	d.dividend_date_ref,
	d.dividend_status
from options_base o
left join dividends d using (market_date, symbol)