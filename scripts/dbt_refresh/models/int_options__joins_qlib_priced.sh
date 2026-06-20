#
# FILE: `StockTrader/scripts/dbt_refresh/models/int_options__joins_qlib_priced.sh`
#

DDB_TARGET_SCHEMA="main_intermediate"
DDB_TARGET_TABLE="int_options__joins_qlib_priced"
DDB_DATE_FILTER_KIND="where_market_date"


#
# RETRIEVE MARKET DATES FROM SOURCE TABLES
#

DDB_MARKET_DATES_SQL=$(cat << 'EOF'
	WITH
		options_dates AS (
			SELECT DISTINCT market_date
			  FROM main_intermediate.int_options__calcs_moneyness
			 WHERE dividend_status = 'yes_dividend'
			   AND is_negative_ask_time_value = false
			   AND ttm_days >= 1
		),
		qlib_priced_dates AS (
			SELECT DISTINCT market_date
			  FROM main_staging.stg_qlib_priced__outputs
			 WHERE pricing_status = 'ok'
		)
	SELECT o.market_date::VARCHAR AS market_date
	  FROM options_dates o
	  JOIN qlib_priced_dates q USING (market_date)
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
				spot_price,
				mid_price,
				bid_price,
				ask_price,
				intrinsic_price,
				time_value_mid_price,
				time_value_bid_price,
				time_value_ask_price,
				volume::INT AS volume,
				open_interest::INT AS open_interest,
				bid_size,
				ask_size,
				sigma,
				risk_free_rate,
				dividend_yield_annualized,
				moneyness_ratio,
				moneyness_ratio_log,
				moneyness_standardized,
				moneyness_category
			FROM main_intermediate.int_options__calcs_moneyness
			WHERE dividend_status = 'yes_dividend'
			  AND is_negative_ask_time_value = false
			  AND ttm_days >= 1
		),
		qlib_priced AS (
			SELECT
				market_date,
				occ,
				npv,
				delta,
				gamma,
				theta,
				iv
			FROM main_staging.stg_qlib_priced__outputs
			WHERE pricing_status = 'ok'
		),
		joined_options_qlib_priced AS (
			SELECT
				o.market_date,
				o.symbol,
				o.occ,
				o.option_type,
				o.expiry_date,
				o.ttm_days,
				o.strike_price,
				o.spot_price,
				o.mid_price,
				o.bid_price,
				o.ask_price,
				o.intrinsic_price,
				o.time_value_mid_price,
				o.time_value_bid_price,
				o.time_value_ask_price,
				o.volume,
				o.open_interest,
				o.bid_size,
				o.ask_size,
				o.sigma,
				o.risk_free_rate,
				o.dividend_yield_annualized,
				o.moneyness_ratio,
				o.moneyness_ratio_log,
				o.moneyness_standardized,
				o.moneyness_category,
				q.npv,
				q.delta,
				q.gamma,
				q.theta,
				q.iv
			FROM options_base o
			JOIN qlib_priced q USING (market_date, occ)
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
		iv
	FROM joined_options_qlib_priced
EOF
)
