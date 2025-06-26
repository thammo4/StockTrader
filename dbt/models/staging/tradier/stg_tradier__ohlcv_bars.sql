--
-- FILE: `StockTrader/dbt/models/staging/tradier/stg_tradier__ohlcv_bars.sql`
--

with source as (
	select * from {{ source('tradier_raw', 'ohlcv_bars') }}
),
with_lag as (
	select
		symbol,
		round(open,2) as open_price,
		round(high,2) as high_price,
		round(low,2) as low_price,
		round(close,2) as close_price,
		volume,
		date::date as market_date,
		created_date::date as created_date,
		lag(close) over(
			partition by symbol
			order by date
		) as prev_close_price
	from source
)

select
	symbol,
	open_price,
	high_price,
	low_price,
	close_price,
	prev_close_price,
	case
		when prev_close_price is not null and prev_close_price > 0
		then round(ln(close_price/prev_close_price),4)
		else null
	end as log_return,
	volume,
	market_date,
	created_date
from with_lag