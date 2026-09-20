--
-- FILE: `StockTrader/dbt/models/intermediate/acct_bal/int_acct_bal__calcs_log_perform.sql`
--

{{ config(materialized='view') }}

--
-- Daily Account Balance Data
--

with daily as (
	select *
	from {{ ref('int_acct_bal__aggs_to_daily') }}
),

--
-- Session Spine
--
-- The balance ingest contains missing/frozen observations. A naive lag()
-- can therefore turn a multi-session interval into an apparent daily return.
--
-- VERIFY:
-- If the OHLCV DAG fails on the same day as the balance DAG, this spine
-- inherits the same hole.
--

sessions as (
	select
		market_date,
		row_number() over (order by market_date) as session_i
	from (
		select distinct market_date
		from {{ ref('stg_tradier__ohlcv_bars') }}
	)
	where market_date >= (
		select min(created_date)
		from {{ ref('stg_tradier__options') }}
	)
),

--
-- Observable Account States
--
-- Remove frozen states BEFORE lagging.
-- This causes the next valid observation to reference the previous valid
-- wealth state while n_sessions_gap records the length of the interval.
--

observable as (
	select *
	from daily
	where not is_frozen
),

--
-- Lagged Account State
--

lagged as (
	select
		o.*,
		s.session_i,

		lag(o.wealth_close) over w 								as wealth_prev,
		lag(o.pnl_open) over w 									as pnl_open_prev,
		lag(o.req_margin) over w 								as req_margin_prev,
		lag(o.option_value_short) over w 							as option_value_short_prev,

		s.session_i
			- lag(s.session_i) over w 							as n_sessions_gap

	from observable o
	join sessions s using (market_date)

	window w as (
		partition by o.acct_id
		order by o.market_date
	)
),

--
-- Daily Returns
--

daily_returns as (
	select
		*,

		wealth_close - wealth_prev 								as pnl_daily,

		wealth_close / nullif(wealth_prev, 0) - 1 					as return_simp,

		ln(
			wealth_close / nullif(wealth_prev, 0)
		) 														as return_log,

		--
		-- Arithmetic/Log Return Difference
		--
		-- VD = r - ln(1+r) >= 0
		--

		(
			wealth_close / nullif(wealth_prev, 0) - 1
		)
		-
		ln(
			wealth_close / nullif(wealth_prev, 0)
		) 														as drag_log

	from lagged
),

--
-- PnL Attribution
--
-- Tradier pnl_open is a LEVEL:
--     unrealized contribution = Δ(pnl_open)
--
-- Tradier pnl_close is a SESSION-RESET STATE VARIABLE:
--     realized contribution for the current session = pnl_close
--
-- Initial decomposition:
--
--     ΔW_t
--         = pnl_close_t
--         + Δ(pnl_open)_t
--         + pnl_unattributed_raw_t
--

attribution as (
	select
		*,

		pnl_open - pnl_open_prev 								as pnl_open_diff,

		--
		-- Diagnostic only. pnl_close resets by session, so this is not
		-- itself a realized-PnL contribution.
		--

		pnl_close
			- lag(pnl_close) over (
				partition by acct_id
				order by market_date
			) 													as pnl_close_diff,

		pnl_close
			+ (pnl_open - pnl_open_prev) 							as pnl_attributed,

		pnl_daily
			- (
				pnl_close
				+ (pnl_open - pnl_open_prev)
			) 													as pnl_unattributed_raw

	from daily_returns
),

--
-- Fee Candidate Inference
--
-- Infer the integer number of per-contract fees implied by a negative
-- raw unexplained wealth contribution.
--
-- This is an inferred quantity, not an observed fill count.
--

fee_candidates as (
	select
		*,

		case
			when pnl_unattributed_raw is null then null

			when pnl_unattributed_raw < 0 then
				round(
					abs(pnl_unattributed_raw)
					/ nullif(
						{{ var('tradier_options_per_contract_fee') }},
						0
					)
				)::bigint

			else 0
		end 													as n_fee_contracts_candidate

	from attribution
),

--
-- Fee Attribution
--
-- Signed wealth contribution from inferred transaction fees.
--
-- Require:
--     1. one-session interval
--     2. negative raw unexplained wealth
--     3. positive inferred contract count
--     4. residual approximately equals:
--
--            n_contracts * per_contract_fee
--
-- The $10 upper bound preserves the conservative range previously used for
-- fee identification and prevents large unexplained losses that happen to
-- be multiples of $0.40 from automatically becoming "fees".
--

fee_attribution as (
	select
		*,

		case
			when pnl_unattributed_raw is null then null

			when
				n_sessions_gap = 1
				and pnl_unattributed_raw < 0
				and abs(pnl_unattributed_raw) <= 10.00
				and n_fee_contracts_candidate > 0
				and abs(
					pnl_unattributed_raw
					+
					(
						n_fee_contracts_candidate
						* {{ var('tradier_options_per_contract_fee') }}
					)
				) < 0.005
			then
				-1.0
				* n_fee_contracts_candidate
				* {{ var('tradier_options_per_contract_fee') }}

			else 0.0
		end 													as pnl_fee

	from fee_candidates
),

--
-- PnL Decomposition
--
-- After fee attribution:
--
--     ΔW_t
--         = pnl_attributed
--         + pnl_fee
--         + pnl_unattributed
--

pnl_decomposition as (
	select
		*,

		pnl_attributed + pnl_fee 								as pnl_explained,

		pnl_unattributed_raw - pnl_fee 							as pnl_unattributed,

		req_margin - req_margin_prev 							as req_margin_diff,

		abs(option_value_short)
			- abs(option_value_short_prev) 						as option_value_short_diff

	from fee_attribution
),

--
-- Remaining Unattributed PnL Classification
--
-- Fee effects have already been removed.
-- This field therefore describes only the residual that remains.
--

pnl_categorization as (
	select
		*,

		case
			when pnl_unattributed is null then 'undefined'
			when abs(pnl_unattributed) < 0.005 then 'exact'
			else 'unexplained'
		end 													as pnl_unattributed_category

	from pnl_decomposition
),

--
-- Final Calculations
--
-- Keep all miscellaneous ratios, basis-point transformations,
-- interval classifications, and validity flags out of the final SELECT.
--

final_calcs as (
	select
		*,

		--
		-- Canonical Wealth State
		--

		wealth_close 											as wealth,

		--
		-- Return Scaling
		--

		10000.0 * return_log 									as return_log_bp,
		10000.0 * drag_log 										as drag_bp,

		--
		-- PnL Fractions
		--

		pnl_fee
			/ nullif(wealth_prev, 0) 							as pnl_fee_wealth_frac,

		pnl_unattributed
			/ nullif(wealth_prev, 0) 							as pnl_unattributed_wealth_frac,

		--
		-- Balance-Sheet / Exposure Fractions
		--

		req_margin
			/ nullif(wealth_close, 0) 							as req_margin_wealth_frac,

		option_buy_pwr
			/ nullif(wealth_close, 0) 							as option_buy_pwr_wealth_frac,

		abs(option_value_short)
			/ nullif(wealth_close, 0) 							as short_liability_wealth_frac,

		--
		-- Interval Classification
		--

		case
			when n_sessions_gap is null then 'undefined'
			when n_sessions_gap = 1 then 'one_session'
			else 'multi_session'
		end 													as interval_category,

		--
		-- Snapshot Timing
		--

		extract(
			hour from
			(ts_max at time zone 'UTC')
				at time zone 'America/New_York'
		) >= 16 												as is_post_close,

		--
		-- Validity Flags
		--

		(
			return_log is not null
			and n_sessions_gap = 1
		) 														as is_valid_return,

		(
			n_sessions_gap = 1
			and pnl_unattributed is not null
			and abs(pnl_unattributed) < 0.005
		) 														as is_valid_attrib

	from pnl_categorization
)

select
	--
	-- Account
	--

	acct_id,
	acct_type,

	--
	-- Observation / Session
	--

	market_date,
	session_i,
	n_sessions_gap,
	interval_category,
	n_snaps,
	ts_min,
	ts_max,

	--
	-- Wealth
	--

	wealth_open,
	wealth_high,
	wealth_low,
	wealth,
	wealth_prev,

	--
	-- Returns
	--

	pnl_daily,
	return_simp,
	return_log,
	return_log_bp,
	drag_log,
	drag_bp,

	--
	-- PnL Attribution
	--

	pnl_close,
	pnl_open,
	pnl_open_diff,
	pnl_close_diff,
	pnl_attributed,

	--
	-- Fee Attribution
	--

	n_fee_contracts_candidate,
	pnl_fee,
	pnl_fee_wealth_frac,

	--
	-- Explained / Unattributed PnL
	--

	pnl_explained,
	pnl_unattributed_raw,
	pnl_unattributed,
	pnl_unattributed_wealth_frac,
	pnl_unattributed_category,

	--
	-- Account / Exposure State
	--

	cash,
	option_buy_pwr,
	req_margin,
	req_option,
	market_value,
	option_value_short,
	option_value_long,
	n_orders_pending,

	--
	-- Exposure Changes
	--

	req_margin_diff,
	option_value_short_diff,

	--
	-- Exposure Fractions
	--

	req_margin_wealth_frac,
	option_buy_pwr_wealth_frac,
	short_liability_wealth_frac,

	--
	-- Intraday Wealth Risk
	--

	wealth_range,
	wealth_vol_rv,
	wealth_vol_parkinson,

	--
	-- Quality / Validity
	--

	is_post_close,
	is_valid_return,
	is_valid_attrib

from final_calcs