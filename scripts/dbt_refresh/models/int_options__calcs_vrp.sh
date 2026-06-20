#
# FILE: `StockTrader/scripts/dbt_refresh/models/int_options__calcs_vrp.sh`
#

DDB_TARGET_SCHEMA="main_intermediate"
DDB_TARGET_TABLE="int_options__calcs_vrp"
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
	WITH options_base AS (
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
	dte_buckets AS (
		SELECT
			*,
			CASE
				WHEN ttm_days BETWEEN 1 AND 7 THEN 'weekly'
				WHEN ttm_days BETWEEN 8 AND 30 THEN 'monthly'
				WHEN ttm_days BETWEEN 31 AND 60 THEN 'intermediate'
				WHEN ttm_days BETWEEN 61 AND 120 THEN 'quarterly'
				ELSE 'leaps'
			END AS dte_bucket
		FROM options_base
	),
	vrp_metrics AS (
		SELECT
			*,
			iv - sigma AS vrp_spread,
			iv / NULLIF(sigma, 0) AS vrp_ratio
		FROM dte_buckets
	),
	xaction_prices AS (
		SELECT
			*,
			100*(ask_price-bid_price) AS spread_price,
			100*(ask_price-bid_price) + .70 AS xaction_price,
			100*bid_price AS credit_price
		FROM vrp_metrics
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
		dte_bucket,
		vrp_spread,
		vrp_ratio,
		spread_price,
		xaction_price,
		credit_price
	FROM xaction_prices
EOF
)
