--
-- FILE: `StockTrader/dbt/tests/assert_options_occ_id_valid.sql`
--

-- Validate OCC symbols in order to correctly identify specific contracts for trading.
-- Missing/invalid OCC renders record unusable.

select
	market_date,
	n_occ,
	n_occ_null,
	n_occ_invalid,
	n_occ_null + n_occ_invalid as n_occ_bad
from {{ ref('int_options__data_quality_metrics') }}
where
	n_occ_null > 0
	or n_occ_invalid > 0
