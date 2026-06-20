#
# FILE: `StockTrader/scripts/dbt_refresh/models/int_options__creates_base_dset.sh`
#

DDB_TARGET_SCHEMA="main_intermediate"
DDB_TARGET_TABLE="int_options__creates_base_dset"
DDB_DATE_FILTER_KIND="where_market_date"


#
# RETRIEVE MARKET DATES FROM UPSTREAM-OF-TARGET TABLE
#

DDB_MARKET_DATES_SQL=$(cat << 'EOF'
	SELECT DISTINCT created_date::VARCHAR AS market_date
	  FROM main_staging.stg_tradier__options
	 WHERE is_valid_price = true
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
			created_date AS market_date,
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
			ask_size
		FROM main_staging.stg_tradier__options
		WHERE is_valid_price = true
	)
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
		ask_size
	FROM options_base
EOF
)
