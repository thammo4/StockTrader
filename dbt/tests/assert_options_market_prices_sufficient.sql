--
-- FILE: `StockTrader/dbt/tests/assert/options_market_prices_sufficient.sql`
--

-- Validate that each historical trading day contains a sufficient proportion of contracts with valid bid_price and ask_price.
-- The bid_price and ask_price are used to compute the mid_price, which is in turn needed downstream for implied volatility computation.

with price_coverage as (
	select
		market_date,
		n_occ,
		greatest(n_bid_price_null, n_ask_price_null) as n_bid_ask_bad,
		round(greatest(n_bid_price_null, n_ask_price_null)/n_occ,4) as p_bid_ask_bad
	from {{ ref('int_options__data_quality_metrics') }}
)

select
	market_date,
	n_occ,
	n_bid_ask_bad,
	p_bid_ask_bad
from price_coverage
where
	p_bid_ask_bad > 0.05

