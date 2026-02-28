--
-- FILE: `StockTrader/dbt/models/intermediate/priced_options/int_priced__materialized.sql`
--

{{ config(
	materialized='incremental',
	incremental_strategy='append',
	description='Materialized, metadata-stripped pricing outputs for bopm_dividends population. Filters to analytically viable records (ok, iv_fail). Eliminates staging view resolution overhead for downstream joins.'
) }}

with source as (
	select
		market_date,
		occ,
		npv,
		delta,
		gamma,
		theta,
		iv,
		pricing_status
	from {{ ref('stg_qlib_priced__outputs') }}
	where pricing_status in ('ok', 'iv_fail')
	{% if is_incremental() %}
		and market_date > (select max(market_date) from {{ this }})
	{% endif %}
)

select * from source