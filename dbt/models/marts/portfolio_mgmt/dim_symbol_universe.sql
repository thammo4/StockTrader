--
-- FILE: `StockTrader/dbt/models/marts/portfolio_mgmt/dim_symbol_universe.sql`
--

with base as (
	select distinct upper(trim(symbol)) as symbol
	from {{ ref('largecap_all') }}
	where coalesce(trim(symbol), '') <> ''
)

select symbol
from base
order by symbol
