--
-- FILE: `StockTrader/dbt/macros/occ_utils.sql`
--


--
-- occ_parse_underlying
-- Extract underlying root symbol given an OCC symbol
-- Accounts for conventional ticker symbols and (ticker symbol + adjustment digit)
-- EX:
-- 		• AAPL240118C00190000 -> AAPL
-- 		• MSFT250321P00250000 -> MSFT
-- 		• CVX1270115P00100000 -> CVX
--

{% macro occ_parse_underlying(occ) -%}
case
	when regexp_matches(upper(trim({{ occ }})),
		'^([A-Z]{1,6})\d?(\d{2})(\d{2})(\d{2})([CP])(\d{8})$')
	then regexp_extract(upper(trim({{ occ }})),
		'^([A-Z]{1,6})\d?(\d{2})(\d{2})(\d{2})([CP])(\d{8})$', 1)
	else null
end
{%- endmacro %}
