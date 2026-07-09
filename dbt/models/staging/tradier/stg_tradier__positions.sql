--
-- FILE: `StockTrader/dbt/models/staging/tradier/stg_tradier__positions.sql`
--

with source as (select * from {{ source('tradier_raw', 'positions_af') }})

select
	market_date::date as market_date,
	occ,

	{{ occ_parse_underlying('occ') }} as symbol,
	{{ occ_parse_option_type('occ') }} as option_type,
	{{ occ_parse_expiry_date('occ') }} as expiry_date,
	{{ occ_parse_strike('occ') }} as strike_price,

	quantity::bigint as quantity,
	round(cost_basis, 2) as cost_basis,
	acq_date::date as acq_date,
	tradier_id,

	created_date::date as created_date,
	account_env
from source
qualify row_number() over (partition by account_env, occ, market_date order by snapshot_ts desc) = 1