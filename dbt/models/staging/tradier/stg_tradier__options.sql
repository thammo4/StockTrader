--
-- FILE: `StockTrader/dbt/models/staging/tradier/stg_tradier__options.sql`
--
with source as (
	select
		symbol,
		last,
		change,
		volume,
		open,
		high,
		low,
		bid,
		ask,
		strike,
		change_percentage,
		last_volume,
		trade_date,
		prevclose,
		bidsize,
		bidexch,
		bid_date,
		asksize,
		askexch,
		ask_date,
		open_interest,
		option_type,
		created_date
	from read_parquet("../data/warehouse/options_af/*.parquet", union_by_name=true)
),
parsed_occ as (
	select
		*,
		{{ occ_parse_underlying('symbol') }} as underlying,
		regexp_extract(symbol, '^[A-Z]{1,6}\d?([0-9]{6})', 1) as expiry_yymmdd
	from source
),
with_expiry_date as (
	select
		*,
		cast(
			'20' ||
			substr(expiry_yymmdd,1,2)
			|| '-' ||
			substr(expiry_yymmdd,3,2)
			|| '-' ||
			substr(expiry_yymmdd,5,2)
			as date
		) as expiry_date
	from parsed_occ
)

select
	-- Contract/ID
	underlying,
	symbol as occ,
	expiry_date,
	option_type,
	strike as strike_price,

	-- Market Data
	bid as bid_price,
	ask as ask_price,
	round((bid+ask)/2.0, 2) as mid_price,
	bidexch as bid_exchange,
	askexch as ask_exchange,

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
	last as close_price,
	change as change_price,
	change_percentage as change_price_pct,
	prevclose as prev_close_price,

	-- Timestamps
	to_timestamp(trade_date/1000) as trade_date,
	to_timestamp(bid_date/1000) as bid_date,
	to_timestamp(ask_date/1000) as ask_date,
	cast(created_date as date) as created_date,

	-- Data Quality Indicators
	case
		when bid is not null and ask is not null and bid > 0 and ask > 0 and ask-bid > 0
		then true
		else false
	end as is_valid_price
from with_expiry_date
