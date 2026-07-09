--
-- FILE: `StockTrader/dbt/models/intermediate/portfolio/int_portfolio__calcs_pnl.sql`
--

{{ config(materialized='view') }}

with snapshots as (
	select
		market_date,
		snapshot_ts,
		equity,
		cash,
		value,
		upnl,
		pnl,
		option_req,
		created_date,
		account_env
	from {{ ref('stg_tradier__account_snapshots') }}
)

select
	market_date,
	snapshot_ts,

	equity,
	cash,
	value,
	upnl,
	pnl,
	option_req,

	lag(market_date) over w as prev_market_date,
	lag(equity) over w as prev_equity,
	round(equity-lag(equity) over w, 2) as pnl_1d,
	round(equity-lag(equity) over w/nullif(abs(lag(equity) over w), 0) * 100, 4) as pnl_1d_pct
from snapshots
window w as (partition by account_env order by market_date)




