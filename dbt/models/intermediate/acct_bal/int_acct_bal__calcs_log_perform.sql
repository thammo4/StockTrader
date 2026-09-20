--
--  FILE: `StockTrader/dbt/models/intermediate/acct_bal/int_acct_bal__calcs_log_perform.sql`
--

{{ config(materialized='view') }}

with daily as (select * from {{ ref('int_acct_bal__aggs_to_daily') }}),

--
-- Session spine. The balance ingest has holes (08-06, 08-31, 09-01..09-03, 09-15)
-- and a naive lag() turns a four-session gap into a "daily" return.
-- VERIFY: if the OHLCV DAG failed on the same days the spine inherits the holes.
--
sessions as (
	select
		market_date,
		row_number() over (order by market_date) as session_i
	from (select distinct market_date from {{ ref('stg_tradier__ohlcv_bars') }})
),

lagged as (
	select
		d.*,
		s.session_i,
		lag(d.wealth_close) over w 														as wealth_prev,
		lag(d.pnl_open) over w 															as pnl_open_prev,
		lag(d.req_margin) over w 														as req_margin_prev,
		lag(d.option_value_short) over w 												as option_value_short_prev,
		s.session_i - lag(s.session_i) over w 											as n_sessions_gap
	from daily d
	join sessions s using (market_date)
	window w as (partition by d.acct_id order by d.market_date)
),

daily_returns as (
	select
		*,
		wealth_close - wealth_prev 														as pnl_daily,
		wealth_close / nullif(wealth_prev,0) - 1 										as return_simp,
		ln(wealth_close / nullif(wealth_prev,0)) 										as log_return,
		-- Variance Drag
		-- VD = r - ln(1+r) >= 0
		(wealth_close/nullif(wealth_prev,0)-1) - ln(wealth_close/nullif(wealth_prev,0)) as drag_log
	from lagged
),

--
-- ATTRIBUTION
--
-- Tradier pnl_open is LEVEL
-- 	• unrealized p/l embeds in open book.
-- 	• day-over-day delta
-- 	• last night unrealized profit already counted into last night wealth
--
-- Tradier pnl_close is SESSION-RESET STATE VARIABLE
-- 	• not a lifetime counter
--
-- 		dW = pnl_close + d(pnl_open) - fees + expiry_effect
--

attribution as (
	select
		*,
		pnl_close 																		as pnl_realized,
		pnl_open 																		as pnl_open_level,
		pnl_open - pnl_open_prev 														as pnl_open_diff,
		pnl_close + (pnl_open - pnl_open_prev) 											as pnl_attributed,
		pnl_daily - (pnl_close + (pnl_open-pnl_open_prev)) 								as pnl_residual,
		pnl_close - lag(pnl_close) over (partition by acct_id order by market_date) 	as pnl_close_level_change
	from daily_returns
),

pnl_categorization as (
	select
		*,
		req_margin - req_margin_prev 													as req_margin_diff,
		abs(option_value_short) - abs(option_value_short_prev) 							as short_liability_diff,
		case
			when pnl_residual is null then 'undefined'
			when abs(pnl_residual) < 0.005 then 'exact'
			-- .40 dollars per contract trade
			when pnl_residual < 0 and abs(pnl_residual) <= 10.00 then 'fees'
			when n_sessions_gap > 1 then 'gap_span'
			else 'noexplain'
		end 																			as pnl_residual_category
	from attribution
)


select
	acct_id,
	acct_type,
	market_date,
	session_i,
	n_sessions_gap,
	n_snaps,
	ts_max,
	wealth_prev,
	wealth_close as wealth,
	pnl_daily,
	return_simp,
	log_return,
	10000.0 * log_return as log_return_bp,
	drag_log,
	10000.0 * drag_log as drap_bp,
	pnl_realized,
	pnl_open_level,
	pnl_open_diff,
	pnl_attributed,
	pnl_residual,
	pnl_residual/nullif(wealth_prev,0) as pnl_residual_wealth_frac,
	pnl_residual_category,
	pnl_close_level_change,
	req_margin_diff,
	short_liability_diff,

	req_margin / nullif(wealth_close,0) as req_margin_wealth_frac,
	option_buy_pwr / nullif(wealth_close,0) as option_buy_pwr_wealth_frac,
	abs(option_value_short) / nullif(wealth_close,0) as short_liability_wealth_frac,

	wealth_range,
	wealth_vol_rv,
	wealth_vol_parkinson,

	extract(hour from ts_max) >= 20 as is_post_close,

	(log_return is not null and coalesce(n_sessions_gap,0)=1) as is_valid_return,
	(pnl_residual_category in ('exact', 'fees')) as is_valid_attrib
from pnl_categorization



































