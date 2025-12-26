--
-- FILE: `StockTrader/dbt/models/intermediate/dimensions/int_dim__daily_dividends.sql`
--

{{ config(
	materialized='incremental',
	incremental_strategy='append'
) }}

with symbol_calendar as (
	select distinct
		market_date,
		symbol,
		spot_price
	from {{ ref('int_options__joins_spot_price') }}
	{% if is_incremental() %}
	where market_date > (select max(market_date) from {{ this }})
	{% endif %}
),

dividends as (
	select
		symbol,
		ex_dividend_date,
		cash_amount,
		frequency
	from {{ ref('stg_tradier__dividends') }}
	where
		is_regular_dividend = true
		and ex_dividend_date is not null
		and cash_amount is not null
),

agg_dividends as (
	select
		sc.market_date,
		sc.symbol,
		sc.spot_price,
		max(d.ex_dividend_date) as prev_ex_dividend_date,
		arg_max(d.cash_amount, d.ex_dividend_date) as prev_cash_amount,
		arg_max(d.frequency, d.ex_dividend_date) as prev_frequency,
		sum(
			case
				when d.ex_dividend_date > sc.market_date - interval '365 days'
				then d.cash_amount
				else 0
			end
		) as dividend_cash_ttm
	from symbol_calendar sc
	left join dividends d
		on d.symbol = sc.symbol
		and d.ex_dividend_date < sc.market_date
	group by
		sc.market_date,
		sc.symbol,
		sc.spot_price
),

dataset as (
	select
		market_date,
		symbol,
		prev_ex_dividend_date,
		prev_cash_amount,
		prev_frequency,
		dividend_cash_ttm,
		case
			when spot_price is null then null
			else dividend_cash_ttm / spot_price
		end as dividend_yield_ttm,
		case
			when spot_price is null then null
			when prev_cash_amount is null then 0.0
			when prev_frequency is null then null
			else (prev_cash_amount*prev_frequency)/spot_price
		end as dividend_yield_annualized
	from agg_dividends
)


select
	market_date,
	symbol,
	prev_ex_dividend_date,
	prev_cash_amount,
	prev_frequency,
	dividend_cash_ttm,
	round(dividend_yield_ttm, 4) as dividend_yield_ttm,
	round(dividend_yield_annualized,4) as dividend_yield_annualized
from dataset











