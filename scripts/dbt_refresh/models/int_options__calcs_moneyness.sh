#
# FILE: `StockTrader/scripts/dbt_refresh/models/int_options__calcs_moneyness.sh`
#

DDB_TARGET_SCHEMA="main_intermediate"
DDB_TARGET_TABLE="int_options__calcs_moneyness"
DDB_DATE_FILTER_KIND="where_market_date"


#
# RETRIEVE MARKET DATES FROM UPSTREAM-OF-TARGET TABLE
#

DDB_MARKET_DATES_SQL=$(cat << 'EOF'
	SELECT DISTINCT market_date::VARCHAR
	  FROM main_intermediate.int_options__joins_dividends
	 ORDER BY 1
	 ;
EOF
)


#
# PRIMARY QUERY
#

DDB_SELECT_SQL=$(cat << 'EOF'
	WITH
		options_base AS (
			SELECT
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
			FROM main_intermediate.int_options__joins_dividends
		),
		with_time AS (
			SELECT
				*,
				ttm_days / 365.0 AS ttm_years
			FROM options_base
		),
		with_moneyness AS (
			SELECT
				*,
				round(spot_price / strike_price, 4) AS moneyness_ratio,
				round(ln(spot_price / strike_price), 4) AS moneyness_ratio_log,
				CASE
					WHEN sigma > 0 AND ttm_days > 0
					THEN round(ln(spot_price / strike_price) / (sigma * sqrt(ttm_years)), 4)
					ELSE NULL
				END AS moneyness_standardized,
				CASE
					WHEN option_type = 'call'
					THEN
						CASE
							WHEN (spot_price - 1.05 * strike_price) >= 0 THEN 'deep_itm'
							WHEN (spot_price - 1.02 * strike_price) >= 0 THEN 'itm'
							WHEN (spot_price - 0.98 * strike_price) >= 0 THEN 'atm'
							WHEN (spot_price - 0.95 * strike_price) >= 0 THEN 'otm'
							ELSE 'deep_otm'
						END
					ELSE
						CASE
							WHEN (spot_price - 0.95 * strike_price) <= 0 THEN 'deep_itm'
							WHEN (spot_price - 0.98 * strike_price) <= 0 THEN 'itm'
							WHEN (spot_price - 1.02 * strike_price) <= 0 THEN 'atm'
							WHEN (spot_price - 1.05 * strike_price) <= 0 THEN 'otm'
							ELSE 'deep_otm'
						END
				END AS moneyness_category
			FROM with_time
		),
		with_intrinsic_value AS (
			SELECT
				*,
				CASE
					WHEN option_type = 'call'
					THEN greatest(0, spot_price - strike_price)
					ELSE greatest(0, strike_price - spot_price)
				END AS intrinsic_price
			FROM with_moneyness
		),
		with_time_value AS (
			SELECT
				*,
				mid_price - intrinsic_price AS time_value_mid_price,
				bid_price - intrinsic_price AS time_value_bid_price,
				ask_price - intrinsic_price AS time_value_ask_price
			FROM with_intrinsic_value
		),
		with_dquality_tolerance AS (
			SELECT
				*,
				0.05 AS epsilon
			FROM with_time_value
		),
		with_dquality_flags AS (
			SELECT
				*,
				CASE
					WHEN time_value_bid_price + epsilon < 0 THEN true
					ELSE false
				END AS is_negative_bid_time_value,
				CASE
					WHEN time_value_ask_price + epsilon < 0 THEN true
					ELSE false
				END AS is_negative_ask_time_value
			FROM with_dquality_tolerance
		),
		with_proper_rounding AS (
			SELECT
				market_date,
				symbol,
				occ,
				option_type,
				expiry_date,
				ttm_days,
				round(ttm_years, 4) AS ttm_years,
				strike_price,
				spot_price,
				mid_price,
				bid_price,
				ask_price,
				bid_ask_spread,
				round(intrinsic_price, 2) AS intrinsic_price,
				round(time_value_mid_price, 2) AS time_value_mid_price,
				round(time_value_bid_price, 2) AS time_value_bid_price,
				round(time_value_ask_price, 2) AS time_value_ask_price,
				volume,
				open_interest,
				bid_size,
				ask_size,
				round(sigma, 4) AS sigma,
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
			FROM with_dquality_flags
		)
	SELECT
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
	FROM with_proper_rounding
EOF
)
