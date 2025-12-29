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
		round(ttm_years,4),
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





-- D describe main_intermediate.int_options__calcs_moneyness;
-- ┌────────────────────────────┬─────────────┬─────────┬─────────┬─────────┬─────────┐
-- │        column_name         │ column_type │  null   │   key   │ default │  extra  │
-- │          varchar           │   varchar   │ varchar │ varchar │ varchar │ varchar │
-- ├────────────────────────────┼─────────────┼─────────┼─────────┼─────────┼─────────┤
-- │ market_date                │ DATE        │ YES     │ NULL    │ NULL    │ NULL    │
-- │ symbol                     │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
-- │ occ                        │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
-- │ option_type                │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
-- │ expiry_date                │ DATE        │ YES     │ NULL    │ NULL    │ NULL    │
-- │ ttm_days                   │ BIGINT      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ ttm_years                  │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ strike_price               │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ spot_price                 │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ mid_price                  │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ bid_price                  │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ ask_price                  │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ bid_ask_spread             │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ intrinsic_price            │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ time_value_mid_price       │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ time_value_bid_price       │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ time_value_ask_price       │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ volume                     │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ open_interest              │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ bid_size                   │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ ask_size                   │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ risk_free_rate             │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ rate_date_ref              │ DATE        │ YES     │ NULL    │ NULL    │ NULL    │
-- │ dividend_yield_annualized  │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ dividend_yield_ttm         │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ dividend_date_ref          │ DATE        │ YES     │ NULL    │ NULL    │ NULL    │
-- │ dividend_status            │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
-- │ moneyness_ratio            │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ moneyness_ratio_log        │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ moneyness_standardized     │ DOUBLE      │ YES     │ NULL    │ NULL    │ NULL    │
-- │ moneyness_category         │ VARCHAR     │ YES     │ NULL    │ NULL    │ NULL    │
-- │ is_negative_bid_time_value │ BOOLEAN     │ YES     │ NULL    │ NULL    │ NULL    │
-- │ is_negative_ask_time_value │ BOOLEAN     │ YES     │ NULL    │ NULL    │ NULL    │
-- ├────────────────────────────┴─────────────┴─────────┴─────────┴─────────┴─────────┤
-- │ 33 rows                                                                6 columns │
-- └──────────────────────────────────────────────────────────────────────────────────┘
-- D select market_date, occ, ttm_days, ttm_years, strike_price as K, spot_price as S, mid_price as p_m, bid_price as p_b, ask_price as p_a, intrinsic_price as p_i, time_value_mid_price as p_tm, time_value_bid_price as p_tb, time_value_ask_price as p_ta, risk_free_rate, dividend_yield_annualized as q, dividend_status as q_status, moneyness_ratio as M, moneyness_ratio_log as logM, moneyness_ratio as stdM, moneyness_category as catM, is_negative_bid_time_value as bad_bids, is_negative_ask_time_value as bad_asks from main_intermediate.int_options__calcs_moneyness;
-- 100% ▕████████████████████████████████████████████████████████████▏ 
-- ┌─────────────┬─────────────────────┬──────────┬──────────────────────┬────────┬────────┬────────┬────────┬────────┬────────────────────┬─────────────────────┬──────────────────────┬────────────────────┬────────────────┬────────┬─────────────────────┬────────┬─────────┬────────┬──────────┬──────────┬──────────┐
-- │ market_date │         occ         │ ttm_days │      ttm_years       │   K    │   S    │  p_m   │  p_b   │  p_a   │        p_i         │        p_tm         │         p_tb         │        p_ta        │ risk_free_rate │   q    │      q_status       │   M    │  logM   │  stdM  │   catM   │ bad_bids │ bad_asks │
-- │    date     │       varchar       │  int64   │        double        │ double │ double │ double │ double │ double │       double       │       double        │        double        │       double       │     double     │ double │       varchar       │ double │ double  │ double │ varchar  │ boolean  │ boolean  │
-- ├─────────────┼─────────────────────┼──────────┼──────────────────────┼────────┼────────┼────────┼────────┼────────┼────────────────────┼─────────────────────┼──────────────────────┼────────────────────┼────────────────┼────────┼─────────────────────┼────────┼─────────┼────────┼──────────┼──────────┼──────────┤
-- │ 2025-11-07  │ AAPL251121C00240000 │       14 │ 0.038356164383561646 │  240.0 │ 268.47 │  28.78 │  28.35 │   29.2 │ 28.470000000000027 │ 0.30999999999997385 │ -0.12000000000002586 │  0.729999999999972 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.1186 │  0.1121 │ 1.1186 │ deep_itm │ true     │ false    │
-- │ 2025-11-07  │ AAPL251121C00242500 │       14 │ 0.038356164383561646 │  242.5 │ 268.47 │  26.48 │  26.15 │   26.8 │ 25.970000000000027 │  0.5099999999999731 │   0.1799999999999713 │ 0.8299999999999734 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.1071 │  0.1017 │ 1.1071 │ deep_itm │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121P00242500 │       14 │ 0.038356164383561646 │  242.5 │ 268.47 │   0.33 │   0.32 │   0.34 │                0.0 │                0.33 │                 0.32 │               0.34 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.1071 │  0.1017 │ 1.1071 │ deep_otm │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121P00245000 │       14 │ 0.038356164383561646 │  245.0 │ 268.47 │   0.41 │    0.4 │   0.42 │                0.0 │                0.41 │                  0.4 │               0.42 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0958 │  0.0915 │ 1.0958 │ deep_otm │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121C00245000 │       14 │ 0.038356164383561646 │  245.0 │ 268.47 │  24.05 │  23.85 │  24.25 │ 23.470000000000027 │  0.5799999999999734 │  0.37999999999997414 │ 0.7799999999999727 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0958 │  0.0915 │ 1.0958 │ deep_itm │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121C00247500 │       14 │ 0.038356164383561646 │  247.5 │ 268.47 │  21.65 │   21.2 │   22.1 │ 20.970000000000027 │  0.6799999999999713 │    0.229999999999972 │ 1.1299999999999741 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0847 │  0.0813 │ 1.0847 │ deep_itm │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121P00247500 │       14 │ 0.038356164383561646 │  247.5 │ 268.47 │   0.51 │    0.5 │   0.51 │                0.0 │                0.51 │                  0.5 │               0.51 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0847 │  0.0813 │ 1.0847 │ deep_otm │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121P00250000 │       14 │ 0.038356164383561646 │  250.0 │ 268.47 │   0.65 │   0.63 │   0.66 │                0.0 │                0.65 │                 0.63 │               0.66 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0739 │  0.0713 │ 1.0739 │ deep_otm │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121C00250000 │       14 │ 0.038356164383561646 │  250.0 │ 268.47 │  19.27 │   19.1 │  19.45 │ 18.470000000000027 │  0.7999999999999723 │   0.6299999999999741 │  0.979999999999972 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0739 │  0.0713 │ 1.0739 │ deep_itm │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121C00252500 │       14 │ 0.038356164383561646 │  252.5 │ 268.47 │  16.95 │  16.65 │  17.25 │ 15.970000000000027 │   0.979999999999972 │   0.6799999999999713 │ 1.2799999999999727 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0632 │  0.0613 │ 1.0632 │ deep_itm │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121P00252500 │       14 │ 0.038356164383561646 │  252.5 │ 268.47 │   0.83 │   0.82 │   0.84 │                0.0 │                0.83 │                 0.82 │               0.84 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0632 │  0.0613 │ 1.0632 │ deep_otm │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121P00255000 │       14 │ 0.038356164383561646 │  255.0 │ 268.47 │   1.09 │   1.07 │   1.11 │                0.0 │                1.09 │                 1.07 │               1.11 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0528 │  0.0515 │ 1.0528 │ deep_otm │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121C00255000 │       14 │ 0.038356164383561646 │  255.0 │ 268.47 │  14.78 │  14.65 │   14.9 │ 13.470000000000027 │   1.309999999999972 │    1.179999999999973 │  1.429999999999973 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0528 │  0.0515 │ 1.0528 │ deep_itm │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121C00257500 │       14 │ 0.038356164383561646 │  257.5 │ 268.47 │   12.6 │  12.55 │  12.65 │ 10.970000000000027 │  1.6299999999999724 │   1.5799999999999734 │  1.679999999999973 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0426 │  0.0417 │ 1.0426 │ itm      │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121P00257500 │       14 │ 0.038356164383561646 │  257.5 │ 268.47 │   1.48 │   1.44 │   1.52 │                0.0 │                1.48 │                 1.44 │               1.52 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0426 │  0.0417 │ 1.0426 │ otm      │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121P00260000 │       14 │ 0.038356164383561646 │  260.0 │ 268.47 │   1.92 │   1.91 │   1.94 │                0.0 │                1.92 │                 1.91 │               1.94 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0326 │  0.0321 │ 1.0326 │ otm      │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121C00260000 │       14 │ 0.038356164383561646 │  260.0 │ 268.47 │   10.6 │  10.55 │  10.65 │  8.470000000000027 │  2.1299999999999724 │   2.0799999999999734 │  2.179999999999973 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0326 │  0.0321 │ 1.0326 │ itm      │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121C00262500 │       14 │ 0.038356164383561646 │  262.5 │ 268.47 │   8.73 │    8.7 │   8.75 │  5.970000000000027 │   2.759999999999973 │    2.729999999999972 │ 2.7799999999999727 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0227 │  0.0225 │ 1.0227 │ itm      │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121P00265000 │       14 │ 0.038356164383561646 │  265.0 │ 268.47 │   3.33 │    3.3 │   3.35 │                0.0 │                3.33 │                  3.3 │               3.35 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0131 │   0.013 │ 1.0131 │ atm      │ false    │ false    │
-- │ 2025-11-07  │ AAPL251121C00265000 │       14 │ 0.038356164383561646 │  265.0 │ 268.47 │    7.0 │   6.95 │   7.05 │ 3.4700000000000273 │  3.5299999999999727 │    3.479999999999973 │ 3.5799999999999725 │         0.0382 │ 0.0039 │ yes_dividend        │ 1.0131 │   0.013 │ 1.0131 │ atm      │ false    │ false    │
-- │     ·       │         ·           │        · │           ·          │    ·   │    ·   │     ·  │     ·  │     ·  │          ·         │           ·         │            ·         │          ·         │            ·   │    ·   │      ·              │    ·   │     ·   │    ·   │  ·       │  ·       │   ·      │
-- │     ·       │         ·           │        · │           ·          │    ·   │    ·   │     ·  │     ·  │     ·  │          ·         │           ·         │            ·         │          ·         │            ·   │    ·   │      ·              │    ·   │     ·   │    ·   │  ·       │  ·       │   ·      │
-- │     ·       │         ·           │        · │           ·          │    ·   │    ·   │     ·  │     ·  │     ·  │          ·         │           ·         │            ·         │          ·         │            ·   │    ·   │      ·              │    ·   │     ·   │    ·   │  ·       │  ·       │   ·      │
-- │ 2025-12-01  │ UNH251205C00260000  │        4 │ 0.010958904109589041 │  260.0 │ 323.21 │  65.25 │   62.9 │   67.6 │  63.20999999999998 │  2.0400000000000205 │ -0.30999999999998096 │  4.390000000000015 │         0.0378 │ 0.0274 │ yes_dividend        │ 1.2431 │  0.2176 │ 1.2431 │ deep_itm │ true     │ false    │
-- │ 2025-12-01  │ UNH251205C00265000  │        4 │ 0.010958904109589041 │  265.0 │ 323.21 │   60.2 │   57.8 │   62.6 │  58.20999999999998 │  1.9900000000000233 │  -0.4099999999999824 │  4.390000000000022 │         0.0378 │ 0.0274 │ yes_dividend        │ 1.2197 │  0.1986 │ 1.2197 │ deep_itm │ true     │ false    │
-- │ 2025-12-01  │ UNH251205C00270000  │        4 │ 0.010958904109589041 │  270.0 │ 323.21 │  55.15 │  52.65 │  57.65 │  53.20999999999998 │   1.940000000000019 │   -0.559999999999981 │  4.440000000000019 │         0.0378 │ 0.0274 │ yes_dividend        │ 1.1971 │  0.1799 │ 1.1971 │ deep_itm │ true     │ false    │
-- │ 2025-12-01  │ UNH251205C00272500  │        4 │ 0.010958904109589041 │  272.5 │ 323.21 │  52.78 │   50.4 │  55.15 │  50.70999999999998 │  2.0700000000000216 │ -0.30999999999998096 │  4.440000000000019 │         0.0378 │ 0.0274 │ yes_dividend        │ 1.1861 │  0.1707 │ 1.1861 │ deep_itm │ true     │ false    │
-- │ 2025-12-01  │ UNH251205C00275000  │        4 │ 0.010958904109589041 │  275.0 │ 323.21 │  50.13 │   47.6 │  52.65 │  48.20999999999998 │   1.920000000000023 │  -0.6099999999999781 │  4.440000000000019 │         0.0378 │ 0.0274 │ yes_dividend        │ 1.1753 │  0.1615 │ 1.1753 │ deep_itm │ true     │ false    │
-- │ 2025-12-01  │ UNH251205C00277500  │        4 │ 0.010958904109589041 │  277.5 │ 323.21 │  47.75 │  45.35 │  50.15 │  45.70999999999998 │  2.0400000000000205 │  -0.3599999999999781 │  4.440000000000019 │         0.0378 │ 0.0274 │ yes_dividend        │ 1.1647 │  0.1525 │ 1.1647 │ deep_itm │ true     │ false    │
-- │ 2025-12-01  │ UNH251205P00280000  │        4 │ 0.010958904109589041 │  280.0 │ 323.21 │   0.02 │   0.01 │   0.03 │                0.0 │                0.02 │                 0.01 │               0.03 │         0.0378 │ 0.0274 │ yes_dividend        │ 1.1543 │  0.1435 │ 1.1543 │ deep_otm │ false    │ false    │
-- │ 2025-12-01  │ UNH251205C00280000  │        4 │ 0.010958904109589041 │  280.0 │ 323.21 │  45.45 │   43.1 │   47.8 │  43.20999999999998 │  2.2400000000000233 │ -0.10999999999997812 │  4.590000000000018 │         0.0378 │ 0.0274 │ yes_dividend        │ 1.1543 │  0.1435 │ 1.1543 │ deep_itm │ true     │ false    │
-- │ 2025-12-01  │ UNH251205C00282500  │        4 │ 0.010958904109589041 │  282.5 │ 323.21 │  42.72 │   40.3 │  45.15 │  40.70999999999998 │  2.0100000000000193 │  -0.4099999999999824 │  4.440000000000019 │         0.0378 │ 0.0274 │ yes_dividend        │ 1.1441 │  0.1346 │ 1.1441 │ deep_itm │ true     │ false    │
-- │ 2025-11-26  │ ADBE260109C00340000 │       44 │  0.12054794520547946 │  340.0 │ 317.52 │    9.6 │    6.6 │   12.6 │                0.0 │                 9.6 │                  6.6 │               12.6 │         0.0382 │    0.0 │ no_dividend_history │ 0.9339 │ -0.0684 │ 0.9339 │ deep_otm │ false    │ false    │
-- │ 2025-11-26  │ ADBE260109P00345000 │       44 │  0.12054794520547946 │  345.0 │ 317.52 │  35.25 │  31.15 │  39.35 │ 27.480000000000018 │   7.769999999999982 │   3.6699999999999804 │ 11.869999999999983 │         0.0382 │    0.0 │ no_dividend_history │ 0.9203 │  -0.083 │ 0.9203 │ deep_itm │ false    │ false    │
-- │ 2025-11-26  │ ADBE260109C00345000 │       44 │  0.12054794520547946 │  345.0 │ 317.52 │    9.3 │    5.1 │   13.5 │                0.0 │                 9.3 │                  5.1 │               13.5 │         0.0382 │    0.0 │ no_dividend_history │ 0.9203 │  -0.083 │ 0.9203 │ deep_otm │ false    │ false    │
-- │ 2025-11-26  │ ADBE260109P00350000 │       44 │  0.12054794520547946 │  350.0 │ 317.52 │  38.78 │  34.95 │   42.6 │  32.48000000000002 │   6.299999999999983 │   2.4699999999999847 │ 10.119999999999983 │         0.0382 │    0.0 │ no_dividend_history │ 0.9072 │ -0.0974 │ 0.9072 │ deep_itm │ false    │ false    │
-- │ 2025-11-26  │ ADBE260109C00350000 │       44 │  0.12054794520547946 │  350.0 │ 317.52 │    8.0 │    3.8 │   12.2 │                0.0 │                 8.0 │                  3.8 │               12.2 │         0.0382 │    0.0 │ no_dividend_history │ 0.9072 │ -0.0974 │ 0.9072 │ deep_otm │ false    │ false    │
-- │ 2025-11-26  │ ADBE260109P00355000 │       44 │  0.12054794520547946 │  355.0 │ 317.52 │  42.68 │   38.9 │  46.45 │  37.48000000000002 │  5.1999999999999815 │   1.4199999999999804 │  8.969999999999985 │         0.0382 │    0.0 │ no_dividend_history │ 0.8944 │ -0.1116 │ 0.8944 │ deep_itm │ false    │ false    │
-- │ 2025-11-26  │ ADBE260109C00355000 │       44 │  0.12054794520547946 │  355.0 │ 317.52 │   6.86 │   2.66 │  11.05 │                0.0 │                6.86 │                 2.66 │              11.05 │         0.0382 │    0.0 │ no_dividend_history │ 0.8944 │ -0.1116 │ 0.8944 │ deep_otm │ false    │ false    │
-- │ 2025-11-26  │ ADBE260109P00360000 │       44 │  0.12054794520547946 │  360.0 │ 317.52 │   46.5 │   42.5 │   50.5 │  42.48000000000002 │   4.019999999999982 │  0.01999999999998181 │  8.019999999999982 │         0.0382 │    0.0 │ no_dividend_history │  0.882 │ -0.1256 │  0.882 │ deep_itm │ false    │ false    │
-- │ 2025-11-26  │ ADBE260109C00360000 │       44 │  0.12054794520547946 │  360.0 │ 317.52 │    5.9 │   1.69 │   10.1 │                0.0 │                 5.9 │                 1.69 │               10.1 │         0.0382 │    0.0 │ no_dividend_history │  0.882 │ -0.1256 │  0.882 │ deep_otm │ false    │ false    │
-- │ 2025-11-26  │ ADBE260109P00365000 │       44 │  0.12054794520547946 │  365.0 │ 317.52 │  50.68 │   46.7 │  54.65 │  47.48000000000002 │  3.1999999999999815 │  -0.7800000000000153 │   7.16999999999998 │         0.0382 │    0.0 │ no_dividend_history │ 0.8699 │ -0.1394 │ 0.8699 │ deep_itm │ true     │ false    │
-- │ 2025-11-26  │ ADBE260109C00365000 │       44 │  0.12054794520547946 │  365.0 │ 317.52 │   5.04 │   0.82 │   9.25 │                0.0 │                5.04 │                 0.82 │               9.25 │         0.0382 │    0.0 │ no_dividend_history │ 0.8699 │ -0.1394 │ 0.8699 │ deep_otm │ false    │ false    │
-- ├─────────────┴─────────────────────┴──────────┴──────────────────────┴────────┴────────┴────────┴────────┴────────┴────────────────────┴─────────────────────┴──────────────────────┴────────────────────┴────────────────┴────────┴─────────────────────┴────────┴─────────┴────────┴──────────┴──────────┴──────────┤
-- │ 41021870 rows (40 shown)                                                                                                                                                                                                                                                                                  22 columns │
