#
# FILE: `StockTrader/scripts/dbt_refresh/models/int_options__calcs_pricing_metrics.sh`
#

DDB_TARGET_SCHEMA="main_intermediate"
DDB_TARGET_TABLE="int_options__calcs_pricing_metrics"
DDB_DATE_FILTER_KIND="where_market_date"


#
# RETRIEVE MARKET DATES FROM UPSTREAM-OF-TARGET TABLE
#

DDB_MARKET_DATES_SQL=$(cat << 'EOF'
	SELECT DISTINCT market_date::VARCHAR
	  FROM main_intermediate.int_options__joins_qlib_priced
	 ORDER BY 1
	 ;
EOF
)


#
# PRIMARY QUERY
#

DDB_SELECT_SQL=$(cat << 'EOF'
	WITH source AS (
		SELECT
			market_date,
			symbol,
			occ,
			option_type,
			expiry_date,
			ttm_days,
			strike_price,
			spot_price,
			mid_price,
			bid_price,
			ask_price,
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
			dividend_yield_annualized,
			moneyness_ratio,
			moneyness_ratio_log,
			moneyness_standardized,
			moneyness_category,
			npv,
			delta,
			gamma,
			theta,
			iv
		FROM main_intermediate.int_options__joins_qlib_priced
	),
	npv_quality_metrics AS (
		SELECT
			*,
			ROUND(npv - mid_price, 4) AS npv_err,
			ROUND((npv - mid_price) / NULLIF(mid_price, 0), 4) AS npv_err_pct,
			bid_price <= npv AND npv <= ask_price AS npv_in_spread
		FROM source
	),
	theta_normalized AS (
		SELECT
			*,
			ROUND(theta * 365, 4) AS theta_annualized,
			ROUND(theta / NULLIF(mid_price, 0), 6) AS theta_pct_mid,
			ROUND(theta / NULLIF(spot_price, 0), 6) AS theta_pct_spot
		FROM npv_quality_metrics
	),
	gamma_normalized AS (
		SELECT
			*,
			ROUND(gamma * spot_price / 100.0, 6) AS gamma_pct
		FROM theta_normalized
	),
	liquidity_filtered AS (
		SELECT
			*,
			ROUND((ask_price - bid_price) / NULLIF(mid_price, 0), 6) AS bid_ask_spread_pct
		FROM gamma_normalized
	)
	SELECT
		market_date,
		symbol,
		occ,
		option_type,
		expiry_date,
		ttm_days,
		strike_price,
		spot_price,
		mid_price,
		bid_price,
		ask_price,
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
		dividend_yield_annualized,
		moneyness_ratio,
		moneyness_ratio_log,
		moneyness_standardized,
		moneyness_category,
		npv,
		delta,
		gamma,
		theta,
		iv,
		npv_err,
		npv_err_pct,
		npv_in_spread,
		theta_annualized,
		theta_pct_mid,
		theta_pct_spot,
		gamma_pct,
		bid_ask_spread_pct
	FROM liquidity_filtered
EOF
)
