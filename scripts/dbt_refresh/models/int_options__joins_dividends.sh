#
# FILE: `StockTrader/scripts/dbt_refresh/models/int_options__joins_dividends.sh`
#

DDB_TARGET_SCHEMA="main_intermediate"
DDB_TARGET_TABLE="int_options__joins_dividends"
DDB_DATE_FILTER_KIND="where_market_date"


#
# RETRIEVE MARKET DATES FROM UPSTREAM-OF-TARGET TABLES
#

DDB_MARKET_DATES_SQL=$(cat << 'EOF'
	WITH
		options_dates AS (
			SELECT DISTINCT market_date
			  FROM main_intermediate.int_options__joins_risk_free_rates
		),
		dividends_dates AS (
			SELECT DISTINCT market_date
			  FROM main_intermediate.int_dividends__maps_to_daily
		)
	SELECT o.market_date::VARCHAR AS market_date
	  FROM options_dates o
	  JOIN dividends_dates d USING (market_date)
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
				rate_date_ref
			FROM main_intermediate.int_options__joins_risk_free_rates
		),
		dividends as (
			SELECT
				market_date,
				symbol,
				prev_ex_dividend_date,
				prev_cash_amount,
				prev_frequency,
				dividend_cash_ttm,
				is_complete_dividend_history
			FROM main_intermediate.int_dividends__maps_to_daily
		),
		joined_options_dividends as (
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
				o.risk_free_rate,
				o.rate_date_ref,
				d.prev_ex_dividend_date AS dividend_date_ref,
				d.is_complete_dividend_history,
				d.dividend_cash_ttm / o.spot_price AS dividend_yield_ttm,
				CASE
					WHEN not d.is_complete_dividend_history THEN 0.0
					WHEN d.prev_frequency IS NULL THEN NULL
					ELSE (d.prev_cash_amount*d.prev_frequency)/o.spot_price
				END AS dividend_yield_annualized,
				CASE
					WHEN d.is_complete_dividend_history = true THEN 'yes_dividend'
					ELSE 'no_dividend'
				END AS dividend_status
			FROM options_base o
			JOIN dividends d USING (market_date, symbol)
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
		ask_size,
		spot_price,
		sigma,
		risk_free_rate,
		rate_date_ref,
		dividend_yield_ttm,
		dividend_yield_annualized,
		dividend_status,
		is_complete_dividend_history,
		dividend_date_ref
	FROM joined_options_dividends
EOF
)
