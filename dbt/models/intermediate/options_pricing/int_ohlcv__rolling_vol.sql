--
-- FILE: `StockTrader/dbt/models/intermediate/options_pricing/int_ohlcv__rolling_vol.sql`
--

{{ config(
	materialized='view',
	description='Enhanced OHLCV data with historical rolling window volatility (std dev) added per vol_window var'
) }}

with ohlcv_ranked as (
	select
		market_date,
		symbol,
		close_price,
		log_return,
		created_date,
		row_number() over (
			partition by market_date, symbol
			order by created_date desc
		) as rank_i
	from {{ ref('stg_tradier__ohlcv_bars') }}
),

ohlcv_dedup as (
	select
		market_date,
		symbol,
		close_price,
		log_return,
		created_date
	from ohlcv_ranked
	where rank_i = 1
),

rolling_vol as (
	select
		market_date,
		symbol,
		close_price,
		log_return,
		created_date,
		stddev_samp(log_return) over(
			partition by symbol
			order by market_date
			rows between {{ var('vol_window')-1 }} preceding and current row
		) as vol_rolling_day,
		count(log_return) over(
			partition by symbol
			order by market_date
			rows between {{ var('vol_window')-1 }} preceding and current row
		) as vol_window_sample_size,
	from ohlcv_dedup
	where log_return is not null
),

final_dataset as (
	select
		market_date,
		symbol,
		close_price,
		round(log_return, 4) as log_return,
		vol_rolling_day,
		(vol_window_sample_size >= {{ var('vol_window')-1 }}) as vol_is_valid_window,
		case
			when vol_window_sample_size >= {{ var('vol_window')-1 }}
			then vol_rolling_day * sqrt({{ var('trading_days_per_annum') }})
			else null
		end as vol_rolling_day_annualized,
		created_date
	from rolling_vol
)

select
	market_date,
	symbol,
	close_price,
	log_return,
	vol_rolling_day,
	vol_rolling_day_annualized,
	vol_is_valid_window,
	created_date
from final_dataset
