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
	frequency
from source
order by ex_dividend_date
