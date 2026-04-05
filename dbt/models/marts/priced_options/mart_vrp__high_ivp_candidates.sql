--
-- FILE: `StockTrader/dbt/models/marts/mart_vrp__high_ivp_candidates`
--

{{ config(materialized='view') }}

{% set ivp_threshold = 0.80 %}
{% set n_partition_min = 30 %}
{% set min_volume = 10 %}
{% set min_open_interest = 100 %}
{% set max_bid_ask_spread_pct = 0.10 %}


with source as (select * from {{ ref('int_options__calcs_ivp') }}),

ok_partition as (select * from source where iv_partition_n >= {{ n_partition_min }}),

ok_tradable as (
	select *
	from ok_partition
	where volume >= {{ min_volume }}
	and open_interest >= {{ min_open_interest }}
	and (ask_price - bid_price) / mid_price <= {{ max_bid_ask_spread_pct }}
),

high_ivp as (
	select *
	from ok_tradable
	where iv_cdf >= {{ ivp_threshold }}
)

select * from high_ivp