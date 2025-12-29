--
-- FILE: `StockTrader/dbt/models/intermediate/options_pricing/int_options__calcs_moneyness.sql`
--

with options_base as (
	select
		market_date,
		symbol,
		occ,
		option_type,
		expiry_date,
		ttm_days,
		strike_price,
		bid_price,
		ask_price,
		mid_price,
		bid_ask_spread,
		volume,
		open_interest,
		bid_size,
		ask_size,
		spot_price,
		sigma,
		risk_free_rate,
		rate_date_ref,
		dividend_yield_annualized,
		dividend_yield_ttm,
		dividend_date_ref,
		dividend_status
	from {{ ref('int_options__joins_dividends') }}
),

with_time as (
	select
		*,
		ttm_days/365.0 as ttm_years
	from options_base
),

with_moneyness as (
	select
		*,
		round(spot_price / strike_price, 4) as moneyness_ratio,
		round(ln(spot_price / strike_price), 4) as moneyness_ratio_log,
		case
			when sigma > 0 and ttm_days > 0
			then round(ln(spot_price / strike_price)/(sigma * sqrt(ttm_years)), 4)
			else null
		end as moneyness_standardized,
		case
			when option_type = 'call'
			then
				case
					when (spot_price - 1.05*strike_price) >= 0 then 'deep_itm'
					when (spot_price - 1.02*strike_price) >= 0 then 'itm'
					when (spot_price - 0.98*strike_price) >= 0 then 'atm'
					when (spot_price - 0.95*strike_price) >= 0 then 'otm'
					else 'deep_otm'
				end
			else
				case
					when (spot_price - 0.95*strike_price) <= 0 then 'deep_itm'
					when (spot_price - 0.98*strike_price) <= 0 then 'itm'
					when (spot_price - 1.02*strike_price) <= 0 then 'atm'
					when (spot_price - 1.05*strike_price) <= 0 then 'otm'
					else 'deep_otm'
				end
		end as moneyness_category
	from with_time
),

with_intrinsic_value as (
	select
		*,
		case
			when option_type = 'call'
			then greatest(0, spot_price - strike_price)
			else greatest(0, strike_price - spot_price)
		end as intrinsic_price
	from with_moneyness
),

with_time_value as (
	select
		*,
		mid_price - intrinsic_price as time_value_mid_price,
		bid_price - intrinsic_price as time_value_bid_price,
		ask_price - intrinsic_price as time_value_ask_price
	from with_intrinsic_value
),

with_dquality_tolerance as ( select *, 0.05 as epsilon from with_time_value ),

with_dquality_flags as (
	select
		*,
		case
			when time_value_bid_price + epsilon < 0
			then true
			else false
		end as is_negative_bid_time_value,
		case
			when time_value_ask_price + epsilon < 0
			then true
			else false
		end as is_negative_ask_time_value
	from with_dquality_tolerance
),

with_proper_rounding as (
	select
		market_date,
		symbol,
		occ,
		option_type,
		expiry_date,
		ttm_days,
		round(ttm_years,4) as ttm_years,
		strike_price,
		spot_price,
		mid_price,
		bid_price,
		ask_price,
		bid_ask_spread,
		round(intrinsic_price, 2) as intrinsic_price,
		round(time_value_mid_price,2) as time_value_mid_price,
		round(time_value_bid_price,2) as time_value_bid_price,
		round(time_value_ask_price,2) as time_value_ask_price,
		volume,
		open_interest,
		bid_size,
		ask_size,
		round(sigma,4) as sigma,
		risk_free_rate,
		rate_date_ref,
		dividend_yield_annualized,
		dividend_yield_ttm,
		dividend_date_ref,
		dividend_status,
		moneyness_ratio,
		moneyness_ratio_log,
		moneyness_standardized,
		moneyness_category,
		is_negative_bid_time_value,
		is_negative_ask_time_value
	from with_dquality_flags
)

select
	market_date,
	symbol,
	occ,
	option_type,
	expiry_date,
	ttm_days,
	ttm_years,
	strike_price,
	spot_price,
	mid_price,
	bid_price,
	ask_price,
	bid_ask_spread,
	intrinsic_price,
	time_value_mid_price,
	time_value_bid_price,
	time_value_ask_price,
	volume,
	open_interest,
	bid_size,
	ask_size,
	sigma,
	risk_free_rate,
	rate_date_ref,
	dividend_yield_annualized,
	dividend_yield_ttm,
	dividend_date_ref,
	dividend_status,
	moneyness_ratio,
	moneyness_ratio_log,
	moneyness_standardized,
	moneyness_category,
	is_negative_bid_time_value,
	is_negative_ask_time_value
from with_proper_rounding
