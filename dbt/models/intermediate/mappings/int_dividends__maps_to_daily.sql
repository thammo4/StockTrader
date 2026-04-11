--
-- FILE: `StockTrader/dbt/models/intermediate/mappings/int_dividends__maps_to_daily.sql`
--

{{ config (materialized='view') }}

with symbol_calendar as (
	select distinct
		created_date as market_date,
		symbol
	from {{ ref('stg_tradier__options') }}
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
),

mapped_daily_dividends as (
	select
		s.market_date,
		s.symbol,
		max(d.ex_dividend_date) as prev_ex_dividend_date,
		arg_max(d.cash_amount, d.ex_dividend_date) as prev_cash_amount,
		arg_max(d.frequency, d.ex_dividend_date) as prev_frequency,
		coalesce(
			sum(case when d.ex_dividend_date >= s.market_date - interval '365 days' then d.cash_amount else 0 end),
			0.0
		) as dividend_cash_ttm,
		bool_or(d.ex_dividend_date is not null) as is_complete_dividend_history
	from symbol_calendar s
	left join dividends d on d.symbol=s.symbol and d.ex_dividend_date<=s.market_date
	group by
		s.market_date,
		s.symbol
)

select
	market_date,
	symbol,
	prev_ex_dividend_date,
	prev_cash_amount,
	prev_frequency,
	dividend_cash_ttm,
	is_complete_dividend_history
from mapped_daily_dividends
