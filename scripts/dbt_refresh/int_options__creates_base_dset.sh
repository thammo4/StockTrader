#!/usr/bin/env bash

#
# FILE: `StockTrader/scripts/dbt_refresh/int_options__creates_base_dset.sh`
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
DDB_TARGET_TABLE="int_options__creates_base_dset"
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
	SELECT DISTINCT created_date AS market_date
	  FROM main_staging.stg_tradier__options
	 ORDER BY created_date DESC
	;
EOF
)


#
# PRIMARY QUERY
#

# DDB_SELECT_SQL=$(cat << EOF
# 	WITH 
# 		options_base AS (
# 			SELECT
# 				market_date,
# 				symbol,
# 				occ,
# 				option_type,
# 				expiry_date,
# 				ttm_days,
# 				strike_price,
# 				bid_price,
# 				ask_price,
# 				mid_price,
# 				bid_ask_spread,
# 				volume,
# 				open_interest,
# 				bid_size,
# 				ask_size
# 			FROM main_intermediate.int_options__filters_bad_prices
# 		),
# 		underlying_price_and_rolling_vol AS (
# 			SELECT 
# 				market_date,
# 				symbol,
# 				close_price AS spot_price,
# 				vol_rolling_day_annualized AS sigma,
# 				vol_is_valid_window AS sigma_is_valid
# 			FROM main_intermediate.int_ohlcv__rolling_vol
# 		    WHERE vol_rolling_day_annualized IS NOT NULL
# 		)
# 	SELECT
# 		   o.market_date,
# 		   o.symbol,
# 		   o.occ,
# 		   o.option_type,
# 		   o.expiry_date,
# 		   o.ttm_days,
# 		   o.strike_price,
# 		   o.bid_price,
# 		   o.ask_price,
# 		   o.mid_price,
# 		   o.bid_ask_spread,
# 		   o.volume,
# 		   o.open_interest,
# 		   o.bid_size,
# 		   o.ask_size,
# 		   u.spot_price,
# 		   u.sigma,
# 		   u.sigma_is_valid
# 	FROM options_base o
# 	JOIN underlying_price_and_rolling_vol u USING (symbol, market_date)
# EOF
# )

DDB_SELECT_SQL=$(cat << EOF
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




if [[ "$VERBOSE" == "true" ]]; then
	echo "		!!!!!!!!!!!!!!!!!! SQL STATEMENTS !!!!!!!!!!!!!!!!!!";
	ekko "DDB_MARKET_DATES_SQL\n${DDB_MARKET_DATES_SQL}"
	ekko "DDB_SELECT_SQL\n${DDB_SELECT_SQL}"
fi


#
# 1. FETCH MARKET DATES COMMON TO BOTH SOURCE TABLES
#

log "1. Fetching market dates from upstream source stg_tradier__options"

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

	# DDB_AND_MARKET_DATE_SQL="AND o.market_date = '${dt}'::DATE"
	DDB_WHERE_MARKET_DATE_SQL="WHERE market_date = '${dt}'::DATE"

	if (( n_inserted == 0 )); then
		run_ddb "
			CREATE TABLE ${DDB_TARGET_SCHEMA_DOT_TABLE}
					  AS ${DDB_SELECT_SQL} ${DDB_WHERE_MARKET_DATE_SQL}
			;
		"
	else
		run_ddb "
			INSERT INTO ${DDB_TARGET_SCHEMA_DOT_TABLE}
				${DDB_SELECT_SQL} ${DDB_WHERE_MARKET_DATE_SQL}
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
# 4. SUMMARIZE RESULTS
#

N_RECORDS_LOADED="$(run_ddb_csv "SELECT COUNT(*) FROM ${DDB_TARGET_SCHEMA_DOT_TABLE};")"
N_DATES_LOADED="$(run_ddb_csv "SELECT COUNT(DISTINCT market_date) FROM ${DDB_TARGET_SCHEMA_DOT_TABLE};")"

log "Insert results: inserted=$n_inserted"
log "Table ${DDB_TARGET_SCHEMA_DOT_TABLE}: n=${N_RECORDS_LOADED}, dates=${N_DATES_LOADED}"

log "Done."
