--
-- FILE: `StockTrader/dbt/tests/assert_options_underlying_map_valid.sql`
--

-- Validate that all options records contain valid underlying symbol.
-- Underlying symbol parsed from OCC received from Tradier using `occ_parse_underlying` macro in occ_utils.sql

select
	market_date,
	n_occ,
	n_symbol_null,
	n_symbol_invalid,
	n_symbol_null + n_symbol_invalid as n_symbol_bad
from {{ ref('int_options__data_quality_metrics') }}
where
	n_symbol_null > 0
	or n_symbol_invalid > 0
