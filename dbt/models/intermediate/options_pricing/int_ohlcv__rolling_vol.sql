--
-- FILE: `StockTrader/dbt/models/intermediate/options_pricing/int_ohlcv__rolling_vol.sql`
--

{{ config(materialized='view') }}

with rolling_vol as (
	select
		symbol,
		market_date,
		close_price,
		log_return,
		created_date,

		-- Compute rolling volatility as rolling-window standard deviation for historical volatility estimate
		stddev(log_return) over (
			partition by symbol
			order by market_date
			rows between {{ var('vol_window') - 1 }} preceding and current row
		) as rolling_vol_daily,

		-- Data quality check to determine if a full window has been used for the volatility estimate
		count(log_return) over (
			partition by symbol
			order by market_date
			rows between {{ var('vol_window') - 1 }} preceding and current row
		) as sample_size_vol_window
	from {{ ref('stg_tradier__ohlcv_bars') }}
	where log_return is not null
),
rolling_vol_annualized as (
	select
		*,
		rolling_vol_daily * sqrt({{ var('trading_days_per_annum') }}) as rolling_vol_annualized,
		case
			when sample_size_vol_window >= {{ var('vol_window') }}
			then true
			else false
		end as is_valid_window
	from rolling_vol
)

select
	symbol,
	market_date,
	close_price,
	log_return,
	rolling_vol_annualized,
	is_valid_window,
	sample_size_vol_window,
	created_date
from rolling_vol_annualized
order by symbol, market_date