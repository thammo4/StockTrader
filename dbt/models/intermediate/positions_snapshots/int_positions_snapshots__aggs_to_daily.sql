--
-- FILE: `StockTrader/dbt/models/intermediate/positions_snapshots/int_positions_snapshots__aggs_to_daily.sql`
--

with snaps as (select * from main_staging.stg_portfolio__positions_snapshots),

--
-- Valid Quote Observations
--
-- Restrict quote-path calculations to internally valid bid/ask
-- observations. The overall position state remains available above.
--

valid_quotes as (
	select
		*,
		ask_price - bid_price 										as spread,
		10000.0 * (ask_price - bid_price) / nullif(mid_price, 0) 	as spread_bps
	from snaps
	where is_valid_price
),


--
-- Intraday Quote Steps
--

quote_steps as (
	select
		*,

		lag(mid_price) over w 								as mid_prev,
		mid_price - lag(mid_price) over w 					as mid_step,
		ln(mid_price / nullif(lag(mid_price) over w, 0)) 	as mid_step_log_return
	from valid_quotes
	window w as (partition by market_date, tradier_id order by snapshot_ts)
),


--
-- Daily Position State
--

daily_state as (
	select
		market_date,
		tradier_id,

		-- Position identity
		arg_max(symbol, snapshot_ts) 				as symbol,
		arg_max(occ, snapshot_ts) 					as occ,
		arg_max(option_type, snapshot_ts) 			as option_type,
		arg_max(expiry_date, snapshot_ts) 			as expiry_date,
		arg_max(expiry_type, snapshot_ts) 			as expiry_type,
		arg_max(n_contracts, snapshot_ts) 			as n_contracts,
		arg_max(strike_price, snapshot_ts) 			as strike_price,
		arg_max(acq_ts, snapshot_ts) 				as acq_ts,

		-- Observation Coverage

		count(*) as n_snaps,
		cast((min(snapshot_ts) at time zone 'UTC') at time zone 'America/New_York' as time) as ts_min_ny,
		cast((max(snapshot_ts) at time zone 'UTC') at time zone 'America/New_York' as time) as ts_max_ny,

		-- Position State
		arg_min(quantity, snapshot_ts) 				as quantity_open,
		arg_max(quantity, snapshot_ts) 				as quantity_close,
		arg_min(cost_basis, snapshot_ts) 			as cost_basis_open,
		arg_max(cost_basis, snapshot_ts) 			as cost_basis_close,
		arg_max(days_held, snapshot_ts) 			as days_held_close,

		-- Marked Position Value
		arg_min(market_value, snapshot_ts) 			as market_value_open,
		max(market_value) 							as market_value_high,
		min(market_value) 							as market_value_low,
		arg_max(market_value, snapshot_ts) 			as market_value_close,

		-- Unrealized PnL
		arg_min(upl, snapshot_ts) 					as upl_open,
		max(upl) 									as upl_high,
		min(upl) 									as upl_low,
		arg_max(upl, snapshot_ts) 					as upl_close,
		arg_min(upl_pct, snapshot_ts) 				as upl_pct_open,
		max(upl_pct) 								as upl_pct_high,
		min(upl_pct) 								as upl_pct_low,
		arg_max(upl_pct, snapshot_ts) 				as upl_pct_close,
	from quote_steps
	group by
		market_date,
		tradier_id
),


--
-- Daily Quote / Liquidity Path
--

daily_quotes as (
	select
		market_date,
		tradier_id,

		-- Midpoint OHLC
		arg_min(mid_price, snapshot_ts) 			as mid_open,
		max(mid_price) 								as mid_high,
		min(mid_price) 								as mid_low,
		arg_max(mid_price, snapshot_ts) 			as mid_close,

		-- Last Observed Market
		arg_max(bid_price, snapshot_ts) 			as bid_close,
		arg_max(ask_price, snapshot_ts) 			as ask_close,
		round(arg_max(spread, snapshot_ts),2) 		as spread_close,
		round(arg_max(spread_bps, snapshot_ts),2) 	as spread_close_bps,
		round(median(spread_bps),2) 				as spread_bps_median,
		round(max(spread_bps),2) 					as spread_bps_max,

		-- Intraday Mark Path
		round(sqrt(sum(coalesce(mid_step_log_return * mid_step_log_return, 0))),6) 	as mid_rv,
		round(sum(abs(mid_step)),2) 												as mid_abs_path,
		round(sum(abs(mid_step_log_return)),4) 										as mid_abs_log_path,
		count(*) filter (where mid_step != 0) 										as n_mid_moves,

		-- Liquidity / Market State
		arg_max(volume, snapshot_ts) 			as volume_close,
		arg_max(open_interest, snapshot_ts) 	as open_interest_close,
		arg_max(bid_size, snapshot_ts) 			as bid_size_close,
		arg_max(ask_size, snapshot_ts) 			as ask_size_close,
		median(bid_size) 						as bid_size_median,
		median(ask_size) 						as ask_size_median
	from quote_steps
	group by
		market_date,
		tradier_id
),


--
-- Combine Daily State and Daily Quote Path
--

daily as (
	select
		s.*,

		q.mid_open,
		q.mid_high,
		q.mid_low,
		q.mid_close,

		q.bid_close,
		q.ask_close,

		q.spread_close,
		q.spread_close_bps,
		q.spread_bps_median,
		q.spread_bps_max,

		q.mid_rv,
		q.mid_abs_path,
		q.mid_abs_log_path,
		q.n_mid_moves,

		q.volume_close,
		q.open_interest_close,
		q.bid_size_close,
		q.ask_size_close,
		q.bid_size_median,
		q.ask_size_median

	from daily_state s

	left join daily_quotes q
		using (market_date, tradier_id)
)


select
	*,

	-- Contract Age / Maturity
	date_diff('day', market_date, expiry_date) as dte,

	-- Daily Midpoint Movement
	round(mid_close - mid_open,2) 						as mid_change,
	round(mid_close / nullif(mid_open, 0) - 1,4) 		as mid_return,
	round(ln(mid_close / nullif(mid_open, 0)),4) 		as mid_log_return,
	round(mid_high - mid_low,2) 						as mid_range,
	round((mid_high - mid_low)/nullif(mid_open, 0),4) 	as mid_range_pct,

	-- Daily Position-Value Movement
	market_value_close - market_value_open as market_value_change,

	-- Daily Unrealized-PnL Movement
	round(upl_close - upl_open,2) as upl_change,
	round(upl_pct_close - upl_pct_open,2) as upl_pct_change

from daily