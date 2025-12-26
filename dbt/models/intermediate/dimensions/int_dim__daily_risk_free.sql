--
-- FILE: `StockTrader/dbt/models/intermediate/dimensions/int_dim__daily_risk_free.sql`
--

{{ config(materialized='table') }}

with market_calendar as (
	select
		market_date
	from {{ ref('int_dim__market_dates') }}
),

rates_of_interest as (
	select
		rate_date,
		round(rate_pct/100.0,4) as risk_free_rate
	from {{ ref('stg_fred__rates') }}
	where series_id = '{{ var("risk_free_rate_series") }}'
)

select
	m.market_date,
	r.risk_free_rate,
	r.rate_date as rate_date_ref
from market_calendar m
asof left join rates_of_interest r on date_trunc('month', m.market_date) >= r.rate_date
