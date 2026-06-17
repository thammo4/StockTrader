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
	SELECT DISTINCT created_date::VARCHAR AS market_date
	  FROM main_staging.stg_tradier__options
	 WHERE is_valid_price = true
	 ORDER BY 1
	;
EOF
)

#
# DETERMINE WHETHER TARGET SCHEMA.TABLE EXISTS IN DDB
#

DDB_TABLE_EXISTS_SQL=$(cat << EOF
	SELECT COUNT(*)
	  FROM information_schema.tables
	 WHERE table_schema = '${DDB_TARGET_SCHEMA}'
	   AND table_name = '${DDB_TARGET_TABLE}'
	;
EOF
)

#
# RETRIEVE EXISTING MARKET DATES IN TARGET SCHEMA.TABLE [if exists]
#

DDB_MARKET_DATES_EXISTING_SQL=$(cat << EOF
	SELECT DISTINCT market_date::VARCHAR
	  FROM ${DDB_TARGET_SCHEMA_DOT_TABLE}
	 ORDER BY market_date ASC
	;
EOF
)


#
# PRIMARY QUERY
#

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
	ekko "DDB_TABLE_EXISTS_SQL\n${DDB_TABLE_EXISTS_SQL}"
	ekko "DDB_MARKET_DATES_EXISTING_SQL\n${DDB_MARKET_DATES_EXISTING_SQL}"
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

log "2. Checking for existing target table: ${DDB_TARGET_SCHEMA_DOT_TABLE}"

IS_EXISTING_TARGET_TABLE="$(run_ddb_csv "$DDB_TABLE_EXISTS_SQL")"
MARKET_DATES_EXISTING=""
N_EXISTING=0

if (( IS_EXISTING_TARGET_TABLE > 0 )); then
	log "Table exists, will skip existing market dates."
	MARKET_DATES_EXISTING="$(run_ddb_csv "$DDB_MARKET_DATES_EXISTING_SQL")"
	N_EXISTING=$(echo "$MARKET_DATES_EXISTING" | grep -c '.' || true)
	log "Found $N_EXISTING target dates."
else
	log "Table n/a, will CTAS on first insert."
fi

if [[ "$VERBOSE" == "true" ]]; then
	echo "		!!!!!!!!!!!!!!!!!! SQL STATEMENTS !!!!!!!!!!!!!!!!!!"
	ekko "MARKET_DATES:\n${MARKET_DATES}"
	ekko "IS_EXISTING_TARGET_TABLE:\n${IS_EXISTING_TARGET_TABLE}"
	ekko "MARKET_DATES_EXISTING:\n${MARKET_DATES_EXISTING}"
fi

#
# IF NO UPSTREAM DATES/TARGET TABLE THEN WE ARE DONE
#

if (( N_TOTAL == 0 )) && (( IS_EXISTING_TARGET_TABLE == 0 )); then
	log "No upstream market dates, no target table."
	log "Done."
	exit 0
fi


#
# 3. RECONSTRUCT TARGET TABLE BY ITERATING MARKET DATES AND INSERTING RECORDS
#

log "3. Reconstructing table via CTAS + Inserts"

i=0
n_inserted=0
n_skipped=0

while IFS= read -r dt; do
	[[ -z "$dt" ]] && continue

	if echo "$MARKET_DATES_EXISTING" | grep -qx "$dt"; then
		(( n_skipped++ )) || true
		continue
	fi

	(( i++ )) || true
	log "Processing date $i / $(( N_TOTAL - n_skipped )): $dt"

	DDB_WHERE_MARKET_DATE_SQL="WHERE market_date = '${dt}'::DATE"

	if (( n_inserted == 0 )) && (( IS_EXISTING_TARGET_TABLE == 0 )); then
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
		log "Insert progress: dates=$n_inserted, skips=$n_skipped, records=$N_LOADED"
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
