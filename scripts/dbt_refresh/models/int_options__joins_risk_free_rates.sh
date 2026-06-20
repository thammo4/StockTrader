#
# FILE: `StockTrader/scripts/dbt_refresh/models/int_options__joins_risk_free_rates.sh`
#

DDB_TARGET_SCHEMA="main_intermediate"
DDB_TARGET_TABLE="int_options__joins_risk_free_rates"
DDB_DATE_FILTER_KIND="where_o_market_date"


#
# RETRIEVE MARKET DATES FROM UPSTREAM-OF-TARGET TABLES
#

DDB_MARKET_DATES_SQL=$(cat << 'EOF'
	WITH
		options_dates AS (
			SELECT DISTINCT market_date
			  FROM main_intermediate.int_options__joins_spots_and_vols
		),
		rate_dates AS (
			SELECT DISTINCT market_date
			  FROM main_intermediate.int_risk_free_rates__maps_to_daily
		)
	SELECT o.market_date::VARCHAR AS market_date
	  FROM options_dates o
	  JOIN rate_dates r USING (market_date)
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
				sigma
			FROM main_intermediate.int_options__joins_spots_and_vols
		),
		market_date_rates AS (
			SELECT
				market_date,
				risk_free_rate,
				rate_date_ref
			FROM main_intermediate.int_risk_free_rates__maps_to_daily
		)
	SELECT
		o.market_date,
		o.symbol,
		o.occ,
		o.option_type,
		o.expiry_date,
		o.ttm_days,
		o.strike_price,
		o.bid_price,
		o.ask_price,
		o.mid_price,
		o.bid_ask_spread,
		o.volume,
		o.open_interest,
		o.bid_size,
		o.ask_size,
		o.spot_price,
		o.sigma,
		m.risk_free_rate,
		m.rate_date_ref
	FROM options_base o
	JOIN market_date_rates m USING (market_date)
EOF
)
