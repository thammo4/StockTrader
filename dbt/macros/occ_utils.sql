--
-- FILE: `StockTrader/dbt/macros/occ_utils.sql`
--

{% macro occ_pattern() -%}
'^([A-Z0-9]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d{8})$'
{%- endmacro %}

{% macro occ_normalized(occ) -%}
upper(trim({{ occ }}))
{%- endmacro %}

{% macro occ_parse_expiry_date(occ) -%}
	case
		when regexp_matches({{ occ_normalized(occ) }}, {{ occ_pattern() }})
		then cast(
			'20' || regexp_extract({{ occ_normalized(occ) }}, {{ occ_pattern() }}, 2)
			|| '-' || regexp_extract({{ occ_normalized(occ) }}, {{ occ_pattern() }}, 3)
			|| '-' || regexp_extract({{ occ_normalized(occ) }}, {{ occ_pattern() }}, 4)
			as date
		)
		else null
	end
{%- endmacro %}

{% macro occ_parse_strike(occ) -%}
	case
		when regexp_matches({{ occ_normalized(occ) }}, {{ occ_pattern() }})
		then regexp_extract({{ occ_normalized(occ) }}, {{ occ_pattern() }}, 6)::bigint / 1000.0
		else null
	end
{%- endmacro %}

{% macro occ_parse_option_type(occ) -%}
	case
		when regexp_matches({{ occ_normalized(occ) }}, {{ occ_pattern() }})
		then
			case regexp_extract({{ occ_normalized(occ) }}, {{ occ_pattern() }}, 5)
				when 'C' then 'call'
				when 'P' then 'put'
			end
		else null
	end
{%- endmacro %}


--
-- occ_parse_underlying
-- Extract underlying root symbol given an OCC symbol
-- EX: AAPL240118C00190000, MSFT250321P00250000
--

{% macro occ_parse_underlying(occ) -%}
	case
		when regexp_matches({{ occ_normalized(occ) }}, {{ occ_pattern() }})
		then regexp_extract({{ occ_normalized(occ) }}, {{ occ_pattern() }}, 1)
		else null
	end
{%- endmacro %}
