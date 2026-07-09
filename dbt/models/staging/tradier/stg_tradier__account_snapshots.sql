--
-- FILE: `StockTrader/dbt/models/staging/tradier/stg_tradier__account_snapshots.sql`
--

with source as ( select * from {{ source('tradier_raw', 'account_af') }})

select
	account_env,
	market_date::date as market_date,
	snapshot_ts::timestamp as snapshot_ts,
	equity,
	cash,
	value,
	upnl,
	pnl,
	option_req,
	created_date::date as created_date
from source
-- qualify row_number() over (partition by account_env, market_date order by snapshot_ts desc) = 1