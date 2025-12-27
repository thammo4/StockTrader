--
-- FILE: `StockTrader/dbt/models/marts/options_pricing/bopm/mart_bopm__pays_dividends.sql`
--

{{ config(
	materialized='view',
	description='BOPM-with-dividends pricing model arguments for export to Python/QuantLib'
) }}

select
	market_date,
	symbol,
	occ,
	expiry_date,

	spot_price as S,
	strike_price as K,
	risk_free_rate as r,
	dividend_yield_annualized as q,
	sigma,
	ttm_days/365.0 as T,
	case when option_type='call' then 'C' else 'P' end as CP,
	mid_price,
	bid_price,
	ask_price,
	volume,
	open_interest,
	rate_date_ref,
	dividend_date_ref,
	dividend_yield_ttm
from {{ ref('int_options__joins_dividends') }}
where dividend_status = 'yes_dividend'
