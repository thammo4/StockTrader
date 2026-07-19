#!/usr/bin/env bash

#
# FILE: `StockTrader/scripts/dbt_refresh/dbt_batch_load.sh`
#

set -euo pipefail

log () { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
ekko () { echo -e "$1"; echo "        -----------------------------------------------------"; }

VERBOSE=false
MODEL=""
GROUP=""
FORCE_DATES=""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/models"
GROUP_DIR="${SCRIPT_DIR}/groups"

while [[ $# -gt 0 ]]; do
	case "$1" in
		--verbose)
			VERBOSE=true
			shift
			;;
		--model)
			[[ $# -ge 2 ]] || { log "ERROR: --model requires a model name"; exit 1; }
			MODEL="$2"
			shift 2
			;;
		--group)
			[[ $# -ge 2 ]] || { log "ERROR: --group requires a group name"; exit 1; }
			GROUP="$2"
			shift 2
			;;
		--force-dates)
			[[ $# -ge 2 ]] || { log "ERROR: --force-dates needs > 1 comma-sep YYYY-MM-DD"; exit 1; }
			FORCE_DATES="$2"
			shift 2
			;;
		*)
			echo "Unknown option: $1"
			exit 1
			;;
	esac
done

if [[ -n "$MODEL" && -n "$GROUP" ]]; then
	log "ERROR: supply only one of --model or --group"
	exit 1
fi

if [[ -z "$MODEL" && -z "$GROUP" ]]; then
	log "ERROR: supply one of --model or --group"
	exit 1
fi


#
# CONFIGURE DDB PATH
#

DDB_PATH="${STOCK_TRADER_DWH}/stocktrader_analytics_dev.duckdb"


#
# HELPER FUNCTIONS
#

run_ddb () { duckdb "$DDB_PATH" -c "$1"; }
run_ddb_csv () { duckdb "$DDB_PATH" --noheader -csv -c "$1"; }


#
# VALIDATE DDB DWH EXISTS AT PATH
#

[[ -f "$DDB_PATH" ]] || { log "ERROR: ddb dwh n/a, path=$DDB_PATH"; exit 1; }


#
# BUILD MODEL LIST
#

build_model_list () {
	if [[ -n "$MODEL" ]]; then
		echo "$MODEL"
		return
	fi

	local group_file="${GROUP_DIR}/${GROUP}.txt"

	[[ -f "$group_file" ]] || { log "ERROR: group n/a, path=${group_file}"; exit 1; }

	grep -v '^[[:space:]]*$' "$group_file" | grep -v '^[[:space:]]*#'
}


#
# REFRESH MODEL
#

refresh_model () {
	local model_name="$1"
	local config_path="${CONFIG_DIR}/${model_name}.sh"

	[[ -f "$config_path" ]] || { log "ERROR: model config n/a, path=${config_path}"; exit 1; }

	local DDB_TARGET_SCHEMA=""
	local DDB_TARGET_TABLE=""
	local DDB_MARKET_DATES_SQL=""
	local DDB_SELECT_SQL=""
	local DDB_DATE_FILTER_KIND=""

	source "$config_path"

	: "${DDB_TARGET_SCHEMA:?model config missing DDB_TARGET_SCHEMA}"
	: "${DDB_TARGET_TABLE:?model config missing DDB_TARGET_TABLE}"
	: "${DDB_MARKET_DATES_SQL:?model config missing DDB_MARKET_DATES_SQL}"
	: "${DDB_SELECT_SQL:?model config missing DDB_SELECT_SQL}"
	: "${DDB_DATE_FILTER_KIND:?model config missing DDB_DATE_FILTER_KIND}"

	local DDB_TARGET_SCHEMA_DOT_TABLE="${DDB_TARGET_SCHEMA}.${DDB_TARGET_TABLE}"

	local DDB_TABLE_EXISTS_SQL
	DDB_TABLE_EXISTS_SQL=$(cat << EOF
	SELECT COUNT(*)
	  FROM information_schema.tables
	 WHERE table_schema = '${DDB_TARGET_SCHEMA}'
	   AND table_name = '${DDB_TARGET_TABLE}'
	;
EOF
)

	local DDB_MARKET_DATES_EXISTING_SQL
	DDB_MARKET_DATES_EXISTING_SQL=$(cat << EOF
	SELECT DISTINCT market_date::VARCHAR
	  FROM ${DDB_TARGET_SCHEMA_DOT_TABLE}
	 ORDER BY market_date ASC
	;
EOF
)

	if [[ "$VERBOSE" == "true" ]]; then
		echo "        !!!!!!!!!!!!!!!!!! MODEL: ${model_name} !!!!!!!!!!!!!!!!!!"
		ekko "DDB_TARGET_SCHEMA_DOT_TABLE:\n${DDB_TARGET_SCHEMA_DOT_TABLE}"
		ekko "DDB_MARKET_DATES_SQL\n${DDB_MARKET_DATES_SQL}"
		ekko "DDB_TABLE_EXISTS_SQL:\n${DDB_TABLE_EXISTS_SQL}"
		ekko "DDB_MARKET_DATES_EXISTING_SQL:\n${DDB_MARKET_DATES_EXISTING_SQL}"
		ekko "DDB_SELECT_SQL\n${DDB_SELECT_SQL}"
	fi

	log "============================================================"
	log "Refreshing model: ${model_name}"
	log "Target: ${DDB_TARGET_SCHEMA_DOT_TABLE}"


	#
	# 1. FETCH MARKET DATES
	#

	log "1. Fetching market dates"

	local MARKET_DATES
	local N_TOTAL

	MARKET_DATES="$(run_ddb_csv "$DDB_MARKET_DATES_SQL")"
	N_TOTAL=$(echo "$MARKET_DATES" | grep -c '.' || true)

	log "Found $N_TOTAL market dates"


	#
	# 2. CHECK EXISTING TARGET TABLE + DATES
	#

	log "2. Checking for existing target table: ${DDB_TARGET_SCHEMA_DOT_TABLE}"

	local IS_EXISTING_TARGET_TABLE
	local MARKET_DATES_EXISTING
	local N_EXISTING

	IS_EXISTING_TARGET_TABLE="$(run_ddb_csv "$DDB_TABLE_EXISTS_SQL")"
	MARKET_DATES_EXISTING=""
	N_EXISTING=0

	if (( IS_EXISTING_TARGET_TABLE > 0 )); then
		log "Table exists, will skip existing market dates."

		if [[ -n "$FORCE_DATES" ]] && (( IS_EXISTING_TARGET_TABLE > 0 )); then
			local fd
			for fd in ${FORCE_DATES//,/ }; do
				log "Force-date DELETE: ${DDB_TARGET_SCHEMA_DOT_TABLE} @ ${fd}"
				run_ddb "DELETE FROM ${DDB_TARGET_SCHEMA_DOT_TABLE} WHERE market_date = '${fd}'::DATE;"
			done
		fi

		MARKET_DATES_EXISTING="$(run_ddb_csv "$DDB_MARKET_DATES_EXISTING_SQL")"
		N_EXISTING=$(echo "$MARKET_DATES_EXISTING" | grep -c '.' || true)
		log "Found $N_EXISTING target dates."
	else
		log "Table n/a, will CTAS on first insert."
	fi

	if [[ "$VERBOSE" == "true" ]]; then
		echo "        !!!!!!!!!!!!!!!!!! SQL RESULTS !!!!!!!!!!!!!!!!!!"
		ekko "MARKET_DATES:\n${MARKET_DATES}"
		ekko "IS_EXISTING_TARGET_TABLE:\n${IS_EXISTING_TARGET_TABLE}"
		ekko "MARKET_DATES_EXISTING:\n${MARKET_DATES_EXISTING}"
	fi


	#
	# IF NO UPSTREAM DATES OR TARGET TABLE THEN DONE
	#

	if (( N_TOTAL == 0 )) && (( IS_EXISTING_TARGET_TABLE == 0 )); then
		log "No upstream market dates, no target table."
		log "Done: ${model_name}"
		return
	fi


	#
	# 3. POPULATE TARGET TABLE
	#

	log "3. Populating table via CTAS + Inserts, skip existing"

	local i=0
	local n_inserted=0
	local n_skipped=0
	local dt=""
	local DDB_DATE_FILTER_SQL=""
	local N_LOADED=""

	while IFS= read -r dt; do
		[[ -z "$dt" ]] && continue

		if echo "$MARKET_DATES_EXISTING" | grep -qx "$dt"; then
			(( n_skipped++ )) || true
			continue
		fi

		(( i++ )) || true
		log "Inserting date $i / $(( N_TOTAL - n_skipped )): $dt"

		case "$DDB_DATE_FILTER_KIND" in
			where_market_date)
				DDB_DATE_FILTER_SQL="WHERE market_date = '${dt}'::DATE"
				;;
			where_created_date)
				DDB_DATE_FILTER_SQL="WHERE created_date = '${dt}'::DATE"
				;;
			where_o_market_date)
				DDB_DATE_FILTER_SQL="WHERE o.market_date = '${dt}'::DATE"
				;;
			and_market_date)
				DDB_DATE_FILTER_SQL="AND market_date = '${dt}'::DATE"
				;;
			*)
				log "ERROR: unsupported DDB_DATE_FILTER_KIND=${DDB_DATE_FILTER_KIND}"
				exit 1
				;;
		esac

		if (( n_inserted == 0 )) && (( IS_EXISTING_TARGET_TABLE == 0 )); then
			run_ddb "
				CREATE TABLE ${DDB_TARGET_SCHEMA_DOT_TABLE}
						  AS ${DDB_SELECT_SQL} ${DDB_DATE_FILTER_SQL}
				;
			"
		else
			run_ddb "
				INSERT INTO ${DDB_TARGET_SCHEMA_DOT_TABLE}
					${DDB_SELECT_SQL} ${DDB_DATE_FILTER_SQL}
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

	local N_RECORDS_LOADED
	local N_DATES_LOADED

	N_RECORDS_LOADED="$(run_ddb_csv "SELECT COUNT(*) FROM ${DDB_TARGET_SCHEMA_DOT_TABLE};")"
	N_DATES_LOADED="$(run_ddb_csv "SELECT COUNT(DISTINCT market_date) FROM ${DDB_TARGET_SCHEMA_DOT_TABLE};")"

	log "Insert results: inserted=$n_inserted, skipped=$n_skipped"
	log "Table ${DDB_TARGET_SCHEMA_DOT_TABLE}: n=${N_RECORDS_LOADED}, dates=${N_DATES_LOADED}"
	log "Done: ${model_name}"
}


#
# RUN REQUESTED MODEL(S)
#

MODEL_LIST="$(build_model_list)"

while IFS= read -r model_name; do
	[[ -z "$model_name" ]] && continue
	refresh_model "$model_name"
done <<< "$MODEL_LIST"

log "All requested refresh jobs done."