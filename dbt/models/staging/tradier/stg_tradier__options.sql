--
-- FILE: `StockTrader/dbt/models/staging/tradier/stg_tradier__options.sql`
--

with source as (
	select * from {{ source('tradier_raw', 'options_af') }}
)

select
	-- Contract/ID
	regexp_extract(symbol, '(^[A-Z]+)', 1) as underlying,
	symbol as occ,
	cast(
		'20' || substr(regexp_extract(symbol, '^[A-Z]+([0-9]{6})', 1), 1, 2)
		|| '-' ||
		substr(regexp_extract(symbol, '^[A-Z]+([0-9]{6})',1),3,2)
		|| '-' ||
		substr(regexp_extract(symbol, '^[A-Z]+([0-9]{6})',1),5,2)
		as date
	) as expiry_date,
	option_type,
	strike as strike_price,

	-- Market Data/TS
	bidexch as bid_exchange,
	askexch as ask_exchange,
	bid as bid_price,
	ask as ask_price,
	round((bid+ask)/2.0, 2) as mid_price,
	last as last_price,
	change as change_price,
	change_percentage as change_price_pct,
	prevclose as prev_close_price,

	-- Timestamps
	to_timestamp(trade_date/1000) as trade_date,
	to_timestamp(bid_date/1000) as bid_date,
	to_timestamp(ask_date/1000) as ask_date,
	created_date::date as created_date,

	-- Trading Activity
	volume,
	last_volume as last_price_volume,
	bidsize as bid_size,
	asksize as ask_size,
	open_interest,

	-- OHLCV Snapshot
	open as open_price,
	high as high_price,
	low as low_price,
	close as close_price,

	-- Data Quality Indicators
	case
		when bid is not null and ask is not null and bid > 0 and ask > 0 and ask > bid
		then true
		else false
	end as is_valid_price
from source