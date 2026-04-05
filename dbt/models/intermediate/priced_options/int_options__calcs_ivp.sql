--
-- FILE: `StockTrader/dbt/models/intermediate/priced_options/int_options__calcs_ivp.sql`
--


{{ config(materialized='view') }}

with source as (
	select * from {{ ref('int_options__calcs_vrp') }}
),

ivp as (
	select
		*,
		cume_dist() over (
			partition by symbol, option_type, moneyness_category, dte_bucket
			order by iv
		) as iv_cdf,
		count(*) over (
			partition by symbol, option_type, moneyness_category, dte_bucket
		) as iv_partition_n
	from source
)

select * from ivp