--
-- FILE: `StockTrader/dbt/models/intermediate/options_pricing/int_options__calc_ttm.sql`
--

with options_ttm as (
	select
		*,
		(expiry_date - created_date) as time_to_expiry_days,
		(expiry_date - created_date)/365.0 as time_to_expiry_years,
		(expiry_date - created_date)/{{ var('trading_days_per_annum') }} as time_to_expiry_trading_years
	from {{ ref('stg_tradier__options') }}
)

select * from options_ttm