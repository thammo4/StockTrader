#
# FILE: `StockTrader/scripts/dbt_refresh/models/int_options__joins_spots_and_vols.sh`
#

DDB_TARGET_SCHEMA="main_intermediate"
DDB_TARGET_TABLE="int_options__joins_spots_and_vols"
DDB_DATE_FILTER_KIND="and_market_date"


#
# RETRIEVE MARKET DATES FROM UPSTREAM-OF-TARGET TABLES
#

DDB_MARKET_DATES_SQL=$(cat << 'EOF'
	WITH
		options_dates AS (SELECT DISTINCT market_date FROM main_intermediate.int_options__creates_base_dset),
		spots_dates AS (SELECT DISTINCT market_date FROM main_staging.stg_tradier__ohlcv_bars),
		vols_dates AS (SELECT DISTINCT market_date FROM main_intermediate.int_ohlcv__calcs_rolling_vols)
	SELECT o.market_date::VARCHAR AS market_date
	FROM options_dates o
	JOIN spots_dates s USING (market_date)
	JOIN vols_dates v USING (market_date)
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
				mid_price,
				bid_price,
				ask_price,
				bid_ask_spread,
				volume::INT AS volume,
				open_interest::INT AS open_interest,
				bid_size,
				ask_size
			FROM main_intermediate.int_options__creates_base_dset
		),

		underlying_vols AS (
			SELECT
				market_date,
				symbol,
				sigma_5d,
				sigma_21d,
				sigma_42d,
				sigma_63d,
				sigma_252d
			FROM main_intermediate.int_ohlcv__calcs_rolling_vols
		),

		underlying_spots AS (
			SELECT
				market_date,
				symbol,
				close_price AS spot_price
			FROM main_staging.stg_tradier__ohlcv_bars
		),

		joined_options_underlyings AS (
			SELECT
				o.market_date,
				o.symbol,
				o.occ,
				o.option_type,
				o.expiry_date,
				o.ttm_days,
				o.strike_price,
				o.mid_price,
				o.bid_price,
				o.ask_price,
				o.bid_ask_spread,
				o.volume,
				o.open_interest,
				o.bid_size,
				o.ask_size,
				us.spot_price,
				CASE
					WHEN o.ttm_days BETWEEN 1 AND 7 THEN uv.sigma_5d
					WHEN o.ttm_days BETWEEN 8 AND 30 THEN uv.sigma_21d
					WHEN o.ttm_days BETWEEN 31 AND 60 THEN uv.sigma_42d
					WHEN o.ttm_days BETWEEN 61 AND 120 THEN uv.sigma_63d
					ELSE uv.sigma_252d
				END AS sigma,
				uv.sigma_5d,
				uv.sigma_21d,
				uv.sigma_42d,
				uv.sigma_63d,
				uv.sigma_252d
			FROM options_base o
			JOIN underlying_spots us USING (market_date, symbol)
			JOIN underlying_vols uv USING (market_date, symbol)
		)

	SELECT
		market_date,
		symbol,
		occ,
		option_type,
		expiry_date,
		ttm_days,
		strike_price,
		mid_price,
		bid_price,
		ask_price,
		bid_ask_spread,
		volume,
		open_interest,
		bid_size,
		ask_size,
		spot_price,
		sigma,
		sigma_5d,
		sigma_21d,
		sigma_42d,
		sigma_63d,
		sigma_252d
	FROM joined_options_underlyings
	WHERE sigma IS NOT NULL
EOF
)
