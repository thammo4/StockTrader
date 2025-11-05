--
-- FILE: `StockTrader/dbt/models/staging/tradier/stg_tradier__dividends.sql`
--

with source as (
	select * from {{ source('tradier_raw', 'dividends_af') }}
)

select
	symbol,
	round(cash_amount,2) as cash_amount,
	ex_date::date as ex_dividend_date,
	frequency,
	case
		when frequency = 0 then 'special'
		when frequency = 1 then 'annual'
		when frequency = 2 then 'semi_annual'
		when frequency = 4 then 'quarterly'
		when frequency = 12 then 'monthly'
		else 'unknown'
	end as dividend_type,
	frequency > 0 and frequency < 13 as is_regular_dividend,
	created_date::date as created_date
from source
order by symbol, ex_dividend_date
