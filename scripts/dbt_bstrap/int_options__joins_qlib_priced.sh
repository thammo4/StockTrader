#!/usr/bin/env bash

#
# FILE: `StockTrader/scripts/dbt_bstrap/int_options__joins_qlib_priced.sh`
#

set -euo pipefail
log () { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

#
# ARGS
#

VERBOSE=false

while [[ $# -gt 0 ]]; do
	case $1 in
		--verbose) 	VERBOSE=true; shift ;;
		*) echo "Unknown option: $1"; exit 1 ;;
	esac
done


#
# CONFIGURE DDB PATH/SCHEMA/TABLE
#

DDB_PATH="${STOCK_TRADER_DWH}/stocktrader_analytics_dev.duckdb"
DDB_TARGET_SCHEMA="main_intermediate"
DDB_TARGET_TABLE="int_options__joins_qlib_priced"
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
# RETRIEVE MARKET DATES FROM SOURCE TABLES
#

DDB_MARKET_DATES_SQL=$(cat << EOF
	WITH 
		options_market_dates AS (
			SELECT DISTINCT market_date
			  FROM main_intermediate.int_options__calcs_moneyness
			 WHERE dividend_status = 'yes_dividend'
			   AND is_negative_ask_time_value = false
			   AND ttm_days >= 1
		),
		priced_market_dates AS (
			SELECT DISTINCT market_date
			  FROM main_intermediate.int_priced__materialized
			 WHERE pricing_status = 'ok'
		)
	SELECT p.market_date::VARCHAR
	  FROM options_market_dates o
	  JOIN priced_market_dates p USING (market_date)
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
# RETRIEVE ANY DATES WHICH ALREADY EXIST IN TARGET SCHEMA.TABLE
# schema.table may/not exist
# helps support idempotent re-runs
#

DDB_MARKET_DATES_EXISTING_SQL=$(cat << EOF
	SELECT DISTINCT market_date::VARCHAR
	  FROM ${DDB_TARGET_SCHEMA_DOT_TABLE}
	 ORDER BY market_date ASC
	;
EOF
)


#
# PRIMARY JOIN QUERY
# will iterate market dates, append an AND-clause, and use in CTAS or insert
#

DDB_SELECT_JOINED_SQL=$(cat << EOF
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
		   o.volume::INT AS volume,
		   o.open_interest::INT AS open_interest,
		   o.bid_size,
		   o.ask_size,
		   o.sigma,
		   o.risk_free_rate,
		   o.dividend_yield_annualized,
		   o.moneyness_ratio,
		   o.moneyness_ratio_log,
		   o.moneyness_standardized,
		   o.moneyness_category,
		   p.npv,
		   p.delta,
		   p.gamma,
		   p.theta,
		   p.iv
	FROM main_intermediate.int_options__calcs_moneyness o
	JOIN main_intermediate.int_priced__materialized p USING (market_date, occ)
   WHERE o.dividend_status = 'yes_dividend'
     AND o.is_negative_ask_time_value = false
     AND o.ttm_days >= 1
     AND p.pricing_status = 'ok'

EOF
)


if [[ "$VERBOSE" == "true" ]]; then
	echo "		!!!!!!!!!!!!!!!!!! SQL STATEMENTS !!!!!!!!!!!!!!!!!!";
	ekko "DDB_MARKET_DATES_SQL\n${DDB_MARKET_DATES_SQL}"
	ekko "DDB_TABLE_EXISTS_SQL\n${DDB_TABLE_EXISTS_SQL}"
	ekko "DDB_MARKET_DATES_EXISTING_SQL\n${DDB_MARKET_DATES_EXISTING_SQL}"
	ekko "DDB_SELECT_JOINED_SQL\n${DDB_SELECT_JOINED_SQL}"
fi



#################################################
# 1. Fetch market dates common to source tables #
# 	- int_options__cals_moneyness				#
# 	- int_priced__materialized					#
#################################################

log "1. Fetching market dates common to int_options__calcs_moneyness and int_priced__materialized"

MARKET_DATES="$(run_ddb_csv "$DDB_MARKET_DATES_SQL")"

N_TOTAL=$(echo "$MARKET_DATES" | grep -c '.' || true)

log "Found $N_TOTAL market dates"



#
# 2. If target table already exists, determine whether there exist market dates present.
#

log "2. Checking for existing table, target=${DDB_TARGET_SCHEMA_DOT_TABLE}"

MARKET_DATES_EXISTING=""
IS_EXISTING_TARGET_TABLE="$(run_ddb_csv "$DDB_TABLE_EXISTS_SQL")";

if [[ "$IS_EXISTING_TARGET_TABLE" -gt 0 ]]; then
	log "Table exists, will skip market dates present."
	MARKET_DATES_EXISTING="$(run_ddb_csv "$DDB_MARKET_DATES_EXISTING_SQL")"
	N_EXISTING=$(echo "$MARKET_DATES_EXISTING" | grep -c '.' || true)
	log "Found $N_EXISTING dates. Skippin."
else
	log "Table n/a, will ctas"
fi


if [[ "$VERBOSE" == "true" ]]; then
	echo "		!!!!!!!!!!!!!!!!!! SQL RESULTS !!!!!!!!!!!!!!!!!!";
	ekko "MARKET DATES:\n${MARKET_DATES}";
	ekko "IS_EXISTING_TARGET_TABLE: ${IS_EXISTING_TARGET_TABLE}"
	ekko "MARKET_DATES_EXISTING: ${MARKET_DATES_EXISTING}"
fi


#######################################################################
# 3. Iterate over market dates + insert joined priced/options records #
#######################################################################

i=0
n_inserted=0
n_skipped=0

while IFS= read -r dt; do
	[[ -z "$dt" ]] && continue
	if echo "$MARKET_DATES_EXISTING" | grep -qx "$dt"; then
		(( n_skipped++ )) || true
		continue
	fi

	(( i++  )) || true
	log "Inserting date $i / $(( N_TOTAL - n_skipped )): $dt"

	# HAS_BECOME_EXISTING_TARGET_TABLE="$(run_ddb_csv "$DDB_TABLE_EXISTS_SQL")"

	DDB_AND_MARKET_DATE_SQL="AND o.market_date='${dt}'::DATE"

	if (( n_inserted == 0 )) && [[ "$IS_EXISTING_TARGET_TABLE" -eq 0 ]]; then
		run_ddb "
			CREATE TABLE ${DDB_TARGET_SCHEMA_DOT_TABLE}
					  AS ${DDB_SELECT_JOINED_SQL} ${DDB_AND_MARKET_DATE_SQL}
		"
		(( n_inserted++ )) || true
		continue
	fi

	run_ddb "
		INSERT INTO ${DDB_TARGET_SCHEMA_DOT_TABLE}
			${DDB_SELECT_JOINED_SQL} ${DDB_AND_MARKET_DATE_SQL}
	"

	(( n_inserted++ )) || true

	if (( n_inserted % 10 == 0 )); then
		N_LOADED="$(run_ddb_csv "SELECT COUNT(*) FROM ${DDB_TARGET_SCHEMA_DOT_TABLE}")"
		log "Insert progress: dates=$n_inserted, records=$N_LOADED"
	fi
done <<< "$MARKET_DATES"



########################
# 4. Summarize Results #
########################

N_RECORDS_LOADED="$(run_ddb_csv "SELECT COUNT(*) FROM ${DDB_TARGET_SCHEMA_DOT_TABLE};")"
N_DATES_LOADED="$(run_ddb_csv "SELECT COUNT(DISTINCT market_date) FROM ${DDB_TARGET_SCHEMA_DOT_TABLE};")"


log "Insert results: skipped=$n_skipped, inserted=$n_inserted"
log "Table ${DDB_TARGET_SCHEMA_DOT_TABLE}: n=${N_RECORDS_LOADED}, dates=${N_DATES_LOADED}"

log "Done."
