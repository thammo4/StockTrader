--
-- FILE: `StockTrader/dbt/models/intermediate/options_pricing/int_ohlcv__rolling_vol.sql`
--

{{ config(
	materialized='view',
	description='Enhanced OHLCV data with historical rolling window volatility (std dev) added per vol_window var'
) }}

with ohlcv_bars_base as (
	select
		market_date,
		symbol,
		close_price,
		log_return
	from {{ ref('stg_tradier__ohlcv_bars') }}
	where log_return is not null
),

calc_rolling_vol as (
	select
		market_date,
		symbol,
		close_price,
		log_return,
		stddev_samp(log_return) over (
			partition by symbol
			order by market_date
			rows between {{ var('vol_window')-1 }} preceding and current row
		) as vol_rolling_day,
		count(log_return) over (
			partition by symbol
			order by market_date
			rows between {{ var('vol_window')-1 }} preceding and current row
		) as vol_window_sample_size
	from ohlcv_bars_base
),
calc_rolling_vol_annualized as (
	select
		market_date,
		symbol,
		close_price,
		round(log_return, 4) as log_return,
		vol_rolling_day,
		vol_window_sample_size >= {{ var('vol_window')-1 }} as vol_is_valid_window,
		case
			when vol_window_sample_size >= {{ var('vol_window')-1 }}
			then vol_rolling_day * sqrt({{ var('trading_days_per_annum') }})
			else null
		end as vol_rolling_day_annualized
	from calc_rolling_vol
)

select
	market_date,
	symbol,
	close_price,
	log_return,
	vol_rolling_day,
	vol_rolling_day_annualized,
	vol_is_valid_window
from calc_rolling_vol_annualized