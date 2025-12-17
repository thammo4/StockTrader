--
-- FILE: `StockTrader/dbt/tests/assert_options_pricing_inputs_valid.sql`
--

-- Validate that options records have sufficient fields for pricing model inputs
-- Fields Validated:
-- 	• option_type must be call/put
-- 	• expiry_date must be present
-- 	• strike_price must be present and non-negative

select
	market_date,
	n_occ,
	n_type_null,
	n_type_invalid,
	n_expiry_null,
	n_strike_null,
	n_strike_negative,
	(n_type_null + n_type_invalid + n_expiry_null + n_strike_null + n_strike_negative) as n_model_inputs_load
from {{ ref('int_options__data_quality_metrics') }}
where
	n_type_null > 0
	or n_type_invalid > 0
	or n_expiry_null > 0
	or n_strike_null > 0
	or n_strike_negative > 0
