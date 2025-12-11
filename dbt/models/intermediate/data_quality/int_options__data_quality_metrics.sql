--
-- FILE: `StockTrader/dbt/models/intermediate/data_quality/int_options__data_quality_metrics.sql`
--

{{ config(
	materialization='incremental',
	incremental_strategy='append',
	unique_key='created_date',
	on_schema_change='fail',
	description='Per-diem aggregated data quality metrics for options data'
) }}

with options_data as (
	select *
	from {{ ref('stg_tradier__options') }}

	{% if is_incremental() %}
	where created_date > (select max(created_date) from {{ this }})
	{% endif %}
),

quality_indicators as (
	select
		-- per-diem grouping
		created_date,

		-- Symbol/OCC Record Counts
		count(distinct symbol) as n_symbol,
		count(distinct occ) as n_occ,

		-- Underlying Symbols
		sum(case when symbol is null then 1 else 0 end) as n_symbol_null,
		sum(case when symbol is not null and not regexp_matches(symbol, '^[A-Z0-9]{1,6}$') then 1 else 0 end) as n_symbol_invalid,

		-- OCC Symbols
		sum(case when occ is null then 1 else 0 end) as n_occ_null,
		sum(case when occ is not null and not regexp_matches(occ, '^[A-Z0-9]{1,6}[0-9]{6}[CP][0-9]{8}$') then 1 else 0 end) as n_occ_invalid,

		-- Option Types
		sum(case when option_type is null then 1 else 0 end) as n_type_null,
		sum(case when option_type not in ('call', 'put') then 1 else 0 end) as n_type_invalid,

		-- Expiry Dates
		sum(case when expiry_date is null then 1 else 0 end) as n_expiry_null,

		-- Strike Prices
		sum(case when strike_price is null then 1 else 0 end) as n_strike_null,
		sum(case when strike_price <= 0 then 1 else 0 end) as n_strike_negative
	from options_data
	group by created_date
)

select
	created_date as market_date,
	n_symbol,
	n_occ,
	n_symbol_null,
	n_symbol_invalid,
	n_occ_null,
	n_occ_invalid,
	n_type_null,
	n_type_invalid,
	n_expiry_null,
	n_strike_null,
	n_strike_negative
from quality_indicators
order by market_date desc