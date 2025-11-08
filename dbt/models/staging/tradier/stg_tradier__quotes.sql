--
-- FILE: `StockTrader/dbt/models/staging/tradier/stg_tradier__quotes.sql`
--

with source as (
	select * from {{ source('tradier_raw', 'quotes_af') }}
)

select
	-- ID Fields
	symbol,
	description as company_name,
	exch as exchange,
	type as security_type,
	root_symbols,

	-- Price/OHLCV
	last as last_price,
	open as open_price,
	high as high_price,
	low as low_price,
	close as close_price,
	prevclose as prev_close_price,

	-- Derived Price/Return Metrics
	round(ln(close/nullif(prevclose,0)),4) as log_return,

	-- Price Changes
	change as change_price,
	change_percentage as change_price_pct,

	-- Market Data
	bid as bid_price,
	ask as ask_price,
	round((bid+ask)/2.0,2) as mid_price,
	round(ask-bid,2) as bid_ask_spread,

	-- Volume/Activity
	volume,
	average_volume,
	last_volume as last_price_volume,

	-- Market Depth
	bidsize as bid_size,
	bidexch as bid_exchange,
	asksize as ask_size,
	askexch as ask_exchange,

	-- 52-Week Range
	week_52_high as week52_high_price,
	week_52_low as week52_low_price,

	-- Timestamps
	case
		when trade_date is not null and trade_date > 0 then to_timestamp(trade_date/1000)
		else null
	end as trade_date,
	case
		when bid_date is not null and bid_date > 0 then to_timestamp(bid_date/1000)
		else null
	end as bid_date,
	case
		when ask_date is not null and ask_date > 0 then to_timestamp(ask_date/1000)
		else null
	end as ask_date,
	created_date::date as created_date,

	-- Data Quality Indicators
	case
		when bid is not null and ask is not null and bid > 0 and ask > 0 and ask-bid > 0
		then true
		else false
	end as is_valid_price
from source