--
-- FILE: `StockTrader/dbt/models/staging/portfolio/stg_portfolio__positions_snapshots.sql`
--

-- NOTE:
-- 		• s3://portfolio-snapshots bucket contains data outside of desired market hours
-- 		• we identify these by recognizing that extra-market-hours records will just repeat preceding

{{ config(materialized='view') }}

with source as (select * from {{ source('portfolio', 'portfolio__positions_snapshots') }}),


--
-- Remove Weekends
--

trading_days as (select * from source where isodow(market_date) between 1 and 5),


--
-- Use Bid/Ask Price/Size to Identify Repeats
--

prev_marks as (
	select
		*,
		lag(bid_price) over w as prev_bid_price,
		lag(ask_price) over w as prev_ask_price,
		lag(bid_size) over w as prev_bid_size,
		lag(ask_size) over w as prev_ask_size
	from trading_days
	window w as (partition by tradier_id order by market_date, snapshot_ts)
),

rep_flags as (
	select
		*,
		coalesce(
			bid_price = prev_bid_price and
			ask_price = prev_ask_price and
			bid_size = prev_bid_size and
			ask_size = prev_ask_size
			,
			false
		) as is_repeat
	from prev_marks
),

--
-- Flag Stale Records (e.g. repeated)
--

stale_flags as (
	select
		*,
		bool_and(is_repeat) over (partition by market_date, snapshot_ts) as is_stale
	from rep_flags
)

select
	market_date,
	snapshot_ts,
	symbol,
	occ,
	option_type,
	expiry_date,
	expiry_type,
	n_contracts,
	strike_price,
	round(mid_price,2) as mid_price,
	round(bid_price,2) as bid_price,
	round(ask_price,2) as ask_price,
	volume,
	open_interest,
	bid_size,
	ask_size,
	quantity::integer as quantity,
	round(cost_basis,2) as cost_basis,
	round(market_value,2) as market_value,
	round(upl,2) as upl,
	round(upl_pct,2) as upl_pct,
	days_held,
	(acq_date + acq_time::time) at time zone 'UTC' as acq_ts,
	tradier_id,
	ingest_ts,
	(bid_price>0 and ask_price>0 and ask_price >= bid_price) as is_valid_price
from stale_flags
where is_stale is false
