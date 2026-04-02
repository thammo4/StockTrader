--
-- FILE: `StockTrader/dbt/models/intermediate/options_pricing/int_ohlcv__calcs_rolling_vols.sql
--

{{ config(materialized='view') }}

{% set trading_days = var('trading_days_per_annum') %}

{% set windows = [
	{'days': 5, 'label': '5d'},
	{'days': 21, 'label': '21d'},
	{'days': 42, 'label': '42d'},
	{'days': 63, 'label': '63d'},
	{'days': 252, 'label': '252d'}
] %}

with ohlcv_bars_base as (
	select
		market_date,
		symbol,
		log_return
	from {{ ref('stg_tradier__ohlcv_bars') }}
	where log_return is not null
),

calcs_rolling_vol as (
	select
		market_date,
		symbol,
		{% for w in windows %}
		stddev_samp(log_return) over (
			partition by symbol
			order by market_date
			rows between {{ w.days-1 }} preceding and current row
		) as vol_daily_{{ w.label }},
		count(log_return) over (
			partition by symbol
			order by market_date
			rows between {{ w.days-1 }} preceding and current row
		) as vol_n_{{ w.label }}
		{%- if not loop.last %}, {% endif %}
		{% endfor %}
	from ohlcv_bars_base
),

calcs_annualized as (
	select
		market_date,
		symbol,
		{% for w in windows %}
		vol_daily_{{ w.label }},
		vol_n_{{ w.label }} > {{ w.days -1 }} as vol_is_valid_{{ w.label }},
		case
			when vol_n_{{ w.label }} > {{ w.days-1 }}
			then vol_daily_{{ w.label }}*sqrt({{ trading_days }})
			else null
		end as sigma_{{ w.label }}
		{%- if not loop.last %}, {% endif %}
		{% endfor %}
	from calcs_rolling_vol
)

select
	market_date,
	symbol,
	{% for w in windows %}
	vol_daily_{{ w.label }},
	sigma_{{ w.label }},
	vol_is_valid_{{ w.label }}
	{%- if not loop.last %},{% endif %}
	{% endfor %}
from calcs_annualized
