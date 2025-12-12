--
-- FILE: `StockTrader/dbt/models/intermediate/data_quality/int_options__data_quality_metrics.sql`
--

{{ config(
	materialization='incremental',
	incremental_strategy='append',
	unique_key='created_date',
	on_schema_change='fail',
	description='Per-diem aggregated data quality metrics for options data'
) }}

with options_data as (
	select *
	from {{ ref('stg_tradier__options') }}

	{% if is_incremental() %}
	where created_date > (select max(created_date) from {{ this }})
	{% endif %}
),

quality_indicators as (
	select
		-- per-diem grouping
		created_date,

		-- Symbol/OCC Record Counts
		count(distinct symbol) as n_symbol,
		count(distinct occ) as n_occ,

		-- Underlying Symbols
		sum(case when symbol is null then 1 else 0 end) as n_symbol_null,
		sum(case when symbol is not null and not regexp_matches(symbol, '^[A-Z0-9]{1,6}$') then 1 else 0 end) as n_symbol_invalid,

		-- OCC Symbols
		sum(case when occ is null then 1 else 0 end) as n_occ_null,
		sum(case when occ is not null and not regexp_matches(occ, '^[A-Z0-9]{1,6}[0-9]{6}[CP][0-9]{8}$') then 1 else 0 end) as n_occ_invalid,

		-- Option Types
		sum(case when option_type is null then 1 else 0 end) as n_type_null,
		sum(case when option_type not in ('call', 'put') then 1 else 0 end) as n_type_invalid,

		-- Expiry Dates
		sum(case when expiry_date is null then 1 else 0 end) as n_expiry_null,

		-- Strike Prices
		sum(case when strike_price is null then 1 else 0 end) as n_strike_null,
		sum(case when strike_price <= 0 then 1 else 0 end) as n_strike_negative,

		-- OHLC Prices
		sum(case when open_price <=0 or high_price <=0 or low_price <= 0 or close_price <= 0 then 1 else 0 end) as n_ohlc_negative,
		sum(
			case
				when high_price is not null and low_price is not null and (high_price - low_price < 0) then 1
				else 0
			end
		) as n_high_lt_low,
		sum(
			case
				when open_price is not null and high_price is not null and low_price is not null and (open_price < low_price or open_price > high_price) then 1
				else 0
			end
		) as n_open_range_invalid,
		sum(
			case
				when close_price is not null and high_price is not null and low_price is not null and (close_price < low_price or open_price > high_price) then 1
				else 0
			end
		) as n_close_range_invalid,

		-- Bid/Ask Prices
		sum(case when bid_price is null then 1 else 0 end) as n_bid_price_null,
		sum(case when ask_price is null then 1 else 0 end) as n_ask_price_null,
		sum(
			case
				when bid_price is not null and ask_price is not null and (ask_price - bid_price < 0) then 1
				else 0
			end
		) as n_ask_lt_bid,

		-- Volume/Open Interest
		sum(case when volume is null then 1 else 0 end) as n_volume_null,
		sum(case when volume < 0 then 1 else 0 end) as n_volume_negative,
		sum(case when open_interest is null then 1 else 0 end) as n_open_interest_null,

		-- Bid/Ask Sizes
		sum(case when bid_size is null then 1 else 0 end) as n_bid_size_null,
		sum(case when bid_size < 0 then 1 else 0 end) as n_bid_size_negative,
		sum(case when ask_size is null then 1 else 0 end) as n_ask_size_null,
		sum(case when ask_size < 0 then 1 else 0 end) as n_ask_size_negative,

		-- Timestamps
		sum(case when trade_date is null then 1 else 0 end) as n_trade_date_null,
		sum(case when bid_date is null then 1 else 0 end) as n_bid_date_null,
		sum(case when ask_date is null then 1 else 0 end) as n_ask_date_null



	from options_data
	group by created_date
)

select
	created_date as market_date,
	n_symbol,
	n_occ,
	n_symbol_null,
	n_symbol_invalid,
	n_occ_null,
	n_occ_invalid,
	n_type_null,
	n_type_invalid,
	n_expiry_null,
	n_strike_null,
	n_strike_negative,
	n_high_lt_low,
	n_open_range_invalid,
	n_close_range_invalid,
	n_bid_price_null,
	n_ask_price_null,
	n_ask_lt_bid,
	n_volume_null,
	n_volume_negative,
	n_open_interest_null,
	n_bid_size_null,
	n_bid_size_negative,
	n_ask_size_null,
	n_ask_size_negative,
	n_trade_date_null,
	n_bid_date_null,
	n_ask_date_null

from quality_indicators
order by market_date desc
