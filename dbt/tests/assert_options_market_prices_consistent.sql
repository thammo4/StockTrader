--
-- FILE: `StockTrader/dbt/tests/assert_options_market_prices_consistent.sql`
--

-- Validates intra-record market prices of options are consistent.
-- Negative bid-ask spreads violate no-arbitrage assumptions of pricing models, so records must be excluded.
-- OHLC violations (high < low), (high < open/close), (open/close > low) indicate either nonsense pricing or timestamp sync issue from Tradier at time of ingest.

with price_consistency as (
	select
		market_date,
		n_occ,
		n_symbol,
		n_ask_lt_bid,
		n_high_lt_low,
		n_ask_lt_bid+n_high_lt_low as n_prices_bad,
		round((n_ask_lt_bid+n_high_lt_low)/n_occ,4) as p_prices_bad
	from {{ ref('int_options__data_quality_metrics') }}
)

select
	market_date,
	n_occ,
	n_symbol,
	n_ask_lt_bid,
	n_high_lt_low,
	n_prices_bad,
	p_prices_bad
from price_consistency
where p_prices_bad > 0.01
