--
-- FILE: `StockTrader/dbt/models/staging/tradier/stg_tradier__acct_bal.sql`
--

with source as (
	select
		account_number,
		account_type,
		total_equity,
		equity,
		total_cash,
		pending_cash,
		uncleared_funds,
		"margin.stock_buying_power",
		"margin.option_buying_power",
		current_requirement,
		option_requirement,
		"margin.fed_call",
		"margin.maintenance_call",
		"margin.sweep",
		market_value,
		long_market_value,
		short_market_value,
		stock_long_value,
		"margin.stock_short_value",
		option_long_value,
		option_short_value,
		open_pl,
		close_pl,
		pending_orders_count,
		created_date,
		created_ts
	from {{ source('tradier_raw', 'account_af') }}
)

select
	-- Account Identifiers
	account_number::varchar 		as acct_id,
	account_type::varchar 			as acct_type,

	-- Overall Portfolio Cash/Equity
	total_equity::double 			as value,
	equity::double 					as equity,
	total_cash::double 				as cash,
	pending_cash::double 			as cash_pending,
	uncleared_funds::double 		as cash_uncleared,

	-- Buying Power
	"margin.stock_buying_power"::double as stock_buy_pwr,
	"margin.option_buying_power"::double as option_buy_pwr,

	-- Margin Requirements
	current_requirement::double 	as req_margin,
	option_requirement::double 		as req_option,

	-- Margin Calls/Sweeps
	"margin.fed_call"::double 		as margin_fed,
	"margin.maintenance_call"::double as margin_maintain,
	"margin.sweep"::double as margin_sweep,

	-- Market Values
	market_value::double as market_value,
	long_market_value::double as market_value_long,
	short_market_value::double as market_value_short,
	stock_long_value::double as stock_value_long,
	"margin.stock_short_value"::double as stock_value_short,
	option_long_value::double as option_value_long,
	option_short_value::double as option_value_short,

	open_pl::double as pnl_open,
	close_pl::double as pnl_close,

	pending_orders_count::int as n_orders_pending,

	created_date::date as created_date,
	(created_date || ' ' || created_ts)::timestamp as created_ts
from source




















