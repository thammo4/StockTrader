#
# FILE: `StockTrader/scripts/dbt_refresh/models/int_options__data_quality_metrics.sh`
#

DDB_TARGET_SCHEMA="main_intermediate"
DDB_TARGET_TABLE="int_options__data_quality_metrics"
DDB_DATE_FILTER_KIND="where_created_date"


#
# RETRIEVE MARKET DATES FROM UPSTREAM SOURCE TABLE
#

DDB_MARKET_DATES_SQL=$(cat << 'EOF'
	SELECT DISTINCT created_date::VARCHAR AS market_date
	  FROM main_staging.stg_tradier__options
	 ORDER BY 1
	 ;
EOF
)


#
# PRIMARY QUERY
#

DDB_SELECT_SQL=$(cat << 'EOF'
	WITH options_data AS (
		SELECT *
		FROM main_staging.stg_tradier__options
	),
	quality_indicators AS (
		SELECT
			created_date,
			COUNT(DISTINCT symbol) AS n_symbol,
			COUNT(DISTINCT occ) AS n_occ,
			SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) AS n_symbol_null,
			SUM(CASE WHEN symbol IS NOT NULL AND NOT regexp_matches(symbol, '^[A-Z0-9]{1,6}$') THEN 1 ELSE 0 END) AS n_symbol_invalid,
			SUM(CASE WHEN occ IS NULL THEN 1 ELSE 0 END) AS n_occ_null,
			SUM(CASE WHEN occ IS NOT NULL AND NOT regexp_matches(occ, '^[A-Z0-9]{1,6}[0-9]{6}[CP][0-9]{8}$') THEN 1 ELSE 0 END) AS n_occ_invalid,
			SUM(CASE WHEN option_type IS NULL THEN 1 ELSE 0 END) AS n_type_null,
			SUM(CASE WHEN option_type NOT IN ('call', 'put') THEN 1 ELSE 0 END) AS n_type_invalid,
			SUM(CASE WHEN expiry_date IS NULL THEN 1 ELSE 0 END) AS n_expiry_null,
			SUM(CASE WHEN ttm_days < 0 THEN 1 ELSE 0 END) AS n_ttm_days_negative,
			SUM(CASE WHEN strike_price IS NULL THEN 1 ELSE 0 END) AS n_strike_null,
			SUM(CASE WHEN strike_price <= 0 THEN 1 ELSE 0 END) AS n_strike_negative,
			SUM(CASE WHEN open_price <= 0 OR high_price <= 0 OR low_price <= 0 OR close_price <= 0 THEN 1 ELSE 0 END) AS n_ohlc_negative,
			SUM(CASE WHEN high_price IS NOT NULL AND low_price IS NOT NULL AND (high_price - low_price < 0) THEN 1 ELSE 0 END) AS n_high_lt_low,
			SUM(CASE WHEN open_price IS NOT NULL AND high_price IS NOT NULL AND low_price IS NOT NULL AND (open_price < low_price OR open_price > high_price) THEN 1 ELSE 0 END) AS n_open_range_invalid,
			SUM(CASE WHEN close_price IS NOT NULL AND high_price IS NOT NULL AND low_price IS NOT NULL AND (close_price < low_price OR open_price > high_price) THEN 1 ELSE 0 END) AS n_close_range_invalid,
			SUM(CASE WHEN bid_price IS NULL THEN 1 ELSE 0 END) AS n_bid_price_null,
			SUM(CASE WHEN ask_price IS NULL THEN 1 ELSE 0 END) AS n_ask_price_null,
			SUM(CASE WHEN bid_price IS NOT NULL AND ask_price IS NOT NULL AND (ask_price - bid_price < 0) THEN 1 ELSE 0 END) AS n_ask_lt_bid,
			SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) AS n_volume_null,
			SUM(CASE WHEN volume < 0 THEN 1 ELSE 0 END) AS n_volume_negative,
			SUM(CASE WHEN open_interest IS NULL THEN 1 ELSE 0 END) AS n_open_interest_null,
			SUM(CASE WHEN bid_size IS NULL THEN 1 ELSE 0 END) AS n_bid_size_null,
			SUM(CASE WHEN bid_size < 0 THEN 1 ELSE 0 END) AS n_bid_size_negative,
			SUM(CASE WHEN ask_size IS NULL THEN 1 ELSE 0 END) AS n_ask_size_null,
			SUM(CASE WHEN ask_size < 0 THEN 1 ELSE 0 END) AS n_ask_size_negative,
			SUM(CASE WHEN trade_date IS NULL THEN 1 ELSE 0 END) AS n_trade_date_null,
			SUM(CASE WHEN bid_date IS NULL THEN 1 ELSE 0 END) AS n_bid_date_null,
			SUM(CASE WHEN ask_date IS NULL THEN 1 ELSE 0 END) AS n_ask_date_null
		FROM options_data
		GROUP BY created_date
	)
	SELECT
		created_date AS market_date,
		n_symbol,
		n_occ,
		n_symbol_null,
		n_symbol_invalid,
		n_occ_null,
		n_occ_invalid,
		n_type_null,
		n_type_invalid,
		n_expiry_null,
		n_ttm_days_negative,
		n_strike_null,
		n_strike_negative,
		n_high_lt_low,
		n_open_range_invalid,
		n_close_range_invalid,
		n_bid_price_null,
		n_ask_price_null,
		n_ask_lt_bid,
		n_volume_null,
		n_volume_negative,
		n_open_interest_null,
		n_bid_size_null,
		n_bid_size_negative,
		n_ask_size_null,
		n_ask_size_negative,
		n_trade_date_null,
		n_bid_date_null,
		n_ask_date_null
	FROM quality_indicators
EOF
)
