--
-- FILE: `StockTrader/dbt/macros/occ_utils.sql`
--


--
-- occ_parse_underlying
-- Extract underlying root symbol given an OCC symbol
-- EX: AAPL240118C00190000, MSFT250321P00250000
--

{% macro occ_parse_underlying(occ) -%}
	case
		when regexp_matches(upper(trim({{ occ }})),
			'^([A-Z]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d{8})$')
		then regexp_extract(upper(trim({{ occ }})),
			'^([A-Z]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d{8})$', 1)
		else null
	end
{%- endmacro %}
