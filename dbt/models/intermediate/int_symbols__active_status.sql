--
-- FILE: `StockTrader/dbt/models/intermediate/int_symbols__active_status.sql`
--

{{ config(
	materialized='table',
	description='Symbol universe with active/inactive trading status metadata'
) }}

with symbol_universe as (select distinct symbol from largecap_all),

global_quotes_freshness as (
	select max(created_date) as max_quotes_ingest_date
	from {{ ref('stg_tradier__quotes') }}
),

global_options_freshness as (
	select max(created_date) as max_options_ingest_date
	from {{ ref('stg_tradier__options') }}
),

symbol_last_quotes as (
	select
		symbol,
		max(created_date) as max_quotes_date
	from {{ ref('stg_tradier__quotes') }}
	where symbol is not null
	and is_valid_price = true
	group by symbol
),

symbol_last_options as (
	select
		underlying as symbol,
		max(created_date) as max_options_date
	from {{ ref('stg_tradier__options') }}
	where symbol is not null
	and is_valid_price = true
	group by symbol
),

symbols_current_quotes as (
	select
		slq.symbol,
		true as has_current_quotes
	from symbol_last_quotes slq
	inner join global_quotes_freshness gqf on slq.max_quotes_date = gqf.max_quotes_ingest_date
),

symbols_current_options as (
	select
		slo.symbol,
		true as has_current_options
	from symbol_last_options slo
	inner join global_options_freshness gof on slo.max_options_date = gof.max_options_ingest_date
),

-- universe_symbol_dates as (
-- 	select
-- 		su.symbol,
-- 		slq.max_quotes_date,
-- 		slo.max_options_date,
-- 		coalesce(scq.has_current_quotes,true) as has_current_quotes,
-- 		coalesce(sco.has_current_options,true) as has_current_options
-- 	from symbol_universe su
-- 	left join symbol_last_quotes slq on su.symbol = slq.symbol
-- 	left join symbol_last_options slo on su.symbol = slo.symbol
-- 	left join symbols_current_quotes scq on su.symbol = scq.symbol
-- 	left join symbols_current_options sco on su.symbol = sco.symbol
-- ),
universe_symbol_dates as (
	select
		su.symbol,
		slq.max_quotes_date,
		slo.max_options_date,
		coalesce(scq.has_current_quotes,false) as has_current_quotes,
		coalesce(sco.has_current_options,false) as has_current_options
	from symbol_universe su
	left join symbol_last_quotes slq on su.symbol = slq.symbol
	left join symbol_last_options slo on su.symbol = slo.symbol
	left join symbols_current_quotes scq on su.symbol = scq.symbol
	left join symbols_current_options sco on su.symbol = sco.symbol
),

universe_active_mdata as (
	select
		symbol,
		has_current_quotes,
		has_current_options,
		case
			when has_current_quotes and has_current_options then true
			else false
		end as is_active_traded,
		max_quotes_date,
		max_options_date
	from universe_symbol_dates
)

select
	symbol,
	is_active_traded,
	has_current_quotes,
	has_current_options,
	max_quotes_date,
	max_options_date,
	current_timestamp as created_date_mdata
from universe_active_mdata
order by
	is_active_traded desc,
	symbol asc
