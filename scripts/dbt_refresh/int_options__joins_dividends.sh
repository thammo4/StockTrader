#! /usr/bin/env bash

#
# FILE: `StockTrader/scripts/dbt_refresh/int_options__joins_dividends.sh`
#

set -euo pipefail
log () { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

#
# ARGS
#

VERBOSE=false

while [[ $# -gt 0 ]]; do
	case $1 in
		--verbose) VERBOSE=true; shift ;;
		*) echo "Unknown option: $1"; exit 1 ;;
	esac
done


#
# CONFIGURE DDB PATH/SCHEMA/TABLE
#

DDB_PATH="${STOCK_TRADER_DWH}/stocktrader_analytics_dev.duckdb"
DDB_TARGET_SCHEMA="main_intermediate"
DDB_TARGET_TABLE="int_options__joins_dividends"
DDB_TARGET_SCHEMA_DOT_TABLE="${DDB_TARGET_SCHEMA}.${DDB_TARGET_TABLE}"


#
# HELPER FUNCTIONS - DDB EXEC, VERBOSE OUTPUT
#

run_ddb () { duckdb "$DDB_PATH" -c "$1"; }
run_ddb_csv () { duckdb "$DDB_PATH" --noheader -csv -c "$1"; }
ekko () { echo -e "$1"; echo "		-----------------------------------------------------"; }


#
# VALIDATE DDB DWH EXISTS AT PATH
#

[[ -f "$DDB_PATH" ]] || { log "ERROR: ddb dwh n/a, path=$DDB_PATH"; exit 1; }


#
# RETRIEVE MARKET DATES FROM UPSTREAM-OF-TARGET TABLE
#

DDB_MARKET_DATES_SQL=$(cat << EOF
	WITH
		options_dates AS (SELECT DISTINCT market_date FROM main_intermediate.int_options__joins_risk_free_rates),
		dividends_dates AS (SELECT DISTINCT market_date FROM main_intermediate.int_dividends__maps_to_daily)
	SELECT o.market_date::VARCHAR
	FROM options_dates o
	JOIN dividends_dates d USING (market_date)
	ORDER BY 1
	;
EOF
)


#
# PRIMARY QUERY
#

DDB_SELECT_JOINED_SQL=$(cat << EOF
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


if [[ "$VERBOSE" == "true" ]]; then
	echo "		!!!!!!!!!!!!!!!!!! SQL STATEMENTS !!!!!!!!!!!!!!!!!!";
	ekko "DDB_MARKET_DATES_SQL\n${DDB_MARKET_DATES_SQL}"
	ekko "DDB_SELECT_JOINED_SQL\n${DDB_SELECT_JOINED_SQL}"
fi



#
# 1. FETCH MARKET DATES COMMON TO BOTH SOURCE TABLES
#

log "1. Fetching market dates common to upstream (int_options__joins_risk_free_rates, int_dividends__maps_to_daily)"

MARKET_DATES="$(run_ddb_csv "$DDB_MARKET_DATES_SQL")"
N_TOTAL=$(echo "$MARKET_DATES" | grep -c '.' || true)

log "Found $N_TOTAL market dates"



#
# 2. DROP EXISTING TARGET TABLE
#

log "2. Dropping existing table if exists: ${DDB_TARGET_SCHEMA_DOT_TABLE}"

run_ddb "DROP TABLE IF EXISTS ${DDB_TARGET_SCHEMA_DOT_TABLE};"

log "Dropped."




#
# 3. RECONSTRUCT TARGET TABLE BY ITERATING MARKET DATES AND INSERTING RECORDS
#

log "3. Reconstructing table via CTAS + Inserts"

i=0
n_inserted=0

while IFS= read -r dt; do
	[[ -z "$dt" ]] && continue
	(( i++ )) || true

	log "Processing date $i / $N_TOTAL: $dt"

	DDB_WHERE_MARKET_DATE_SQL="WHERE market_date = '${dt}'::DATE"

	if (( n_inserted == 0 )); then
		run_ddb "
			CREATE TABLE ${DDB_TARGET_SCHEMA_DOT_TABLE}
					  AS ${DDB_SELECT_JOINED_SQL} ${DDB_WHERE_MARKET_DATE_SQL}
			;
		"
	else
		run_ddb "
			INSERT INTO ${DDB_TARGET_SCHEMA_DOT_TABLE}
				${DDB_SELECT_JOINED_SQL} ${DDB_WHERE_MARKET_DATE_SQL}
			;
		"
	fi

	(( n_inserted++ )) || true

	if (( n_inserted % 10 == 0 )); then
		N_LOADED="$(run_ddb_csv "SELECT COUNT(*) FROM ${DDB_TARGET_SCHEMA_DOT_TABLE};")"
		log "Insert progress: dates=$n_inserted, records=$N_LOADED"
	fi
done <<< "$MARKET_DATES"



#
# 4. Summarize Results
#

N_RECORDS_LOADED="$(run_ddb_csv "SELECT COUNT(*) FROM ${DDB_TARGET_SCHEMA_DOT_TABLE};")"
N_DATES_LOADED="$(run_ddb_csv "SELECT COUNT(DISTINCT market_date) FROM ${DDB_TARGET_SCHEMA_DOT_TABLE};")"

log "Insert results: inserted=$n_inserted"
log "Table ${DDB_TARGET_SCHEMA_DOT_TABLE}: n=${N_RECORDS_LOADED}, dates=${N_DATES_LOADED}"

log "Done."
