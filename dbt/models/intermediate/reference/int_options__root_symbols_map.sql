--
-- FILE: `StockTrader/dbt/models/intermediate/reference/int_options__root_symbols_map.sql`
--

{{ config(materialized='table', description='Maps option root symbols to their current trading symbol via quotes root_symbols field') }}

with quotes_root_symbols as (
	select
		symbol as current_trading_symbol,
		root_symbols
	from {{ ref('stg_tradier__quotes') }}
	where root_symbols is not null
),
root_symbols_exploded as (
	select
		current_trading_symbol,
		trim(unnest(string_split(root_symbols, ','))) as root_symbol
	from quotes_root_symbols
),
symbol_map as (
	select distinct
		root_symbol,
		current_trading_symbol,
		case
			when root_symbol = current_trading_symbol then true
			else false
		end as is_primary_symbol
	from root_symbols_exploded
)
select
	root_symbol,
	current_trading_symbol,
	is_primary_symbol,
	current_date as created_at
from symbol_map
order by root_symbol, is_primary_symbol desc