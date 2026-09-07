#!/usr/bin/env bash

#
# FILE: `StockTrader/scripts/minio_ddb_import/run.sh`
#

#
# Importer Config Requirements
#
# Variables:
# 	1. TARGET_SCHEMA (default: raw)
# 	2. TARGET_TABLE
# 	3. PARTITION_COL
# 	4. KEY_REGEX
# 	5. IMPORT_MODE
# 	6. ALLOW_EMPTY
#
# Functions:
# 	1. s3_glob(import_key) 				= s3 glob pattern
# 	2. partition_val(import_key) 		= sql literal value for partition column
# 	3. sql_ddl 							= CREATE TABLE IF NOT EXISTS
# 	4. sql_select(s3_glob, import_key) 	= SELECT stuff FROM glob
#


#
# Program Flow
#
# 1. Log config
# 2. DDB DDL
# 	- install httpfs
# 	- create schema
# 	- importer CTAS
# 3. Count number of objects matching s3 glob
# 4. Count existing rows for partition
# 5. If (mode=replace) -> create DELETE FROM statement
# 6. Construct + Execute SQL Transaction
# 7. Results Summary
#



set -euo pipefail
log () { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }


#
# Define CLI Arguments
#

IMPORTER=""
IMPORT_KEY=""
DRY_RUN=false
VERBOSE=false
OVERRIDE=""

# while [[ $# -gt 0 ]]; do
# 	case $1 in
# 		--importer) 	IMPORTER="$2"; shift 2 ;;
# 		--import-key) 	IMPORT_KEY="$2"; shift 2 ;;
# 		--override) 	OVERRIDE="replace"; shift ;;
# 		--dry-run) 		DRY_RUN=true; shift ;;
# 		--verbose) 		VERBOSE=true; shift ;;
# 		*) 				echo "Unknown option: '$1'"; exit 1 ;;
# 	esac
# done

usage() {
	cat <<-EOF
	Usage: $0 --importer <name> [--import-key <key>] [--override] [--dry-run] [--verbose] [--help]

	Options:
	  --importer <name>   Name of the importer script (without .sh) located in importers/
	  --import-key <key>  Key identifying the data partition to import (must match importer's KEY_REGEX)
	  --override          Replace existing data for the partition instead of skipping (sets mode to replace)
	  --dry-run           Print actions without executing them
	  --verbose           Print the full SQL transaction before running
	  --help              Show this help message and exit

	Environment variables:
	  STOCK_TRADER_DWH       Path to the DuckDB database file (required)
	  MINIO_ENDPOINT         MinIO endpoint (default: 127.0.0.1:9000)
	  MINIO_ACCESS_KEY       MinIO access key (default: stocktrader)
	  MINIO_SECRET_KEY       MinIO secret key (default: stocktrader)
	EOF
}

while [[ $# -gt 0 ]]; do
	case $1 in
		--importer) 	IMPORTER="$2"; shift 2 ;;
		--import-key) 	IMPORT_KEY="$2"; shift 2 ;;
		--override) 	OVERRIDE="replace"; shift ;;
		--dry-run) 		DRY_RUN=true; shift ;;
		--verbose) 		VERBOSE=true; shift ;;
		--help|-h) 		usage; exit 0 ;;
		*) 				echo "Unknown option: '$1'"; usage >&2; exit 1 ;;
	esac
done


#
# Source Importer File with Predefined Vars and F(x)
#

IMPORTER_FILE="$(dirname "$0")/importers/${IMPORTER}.sh"
[[ -f "$IMPORTER_FILE" ]] || { log "ERROR: no importer file '${IMPORTER_FILE}'"; echo ""; echo "-----------------------------------------------------------------------------------------"; echo ""; usage; exit 1; }
source "$IMPORTER_FILE"


#
# Validate Importer Provisions
#

: "${TARGET_SCHEMA:=raw}"
for x in TARGET_TABLE PARTITION_COL KEY_REGEX IMPORT_MODE ALLOW_EMPTY; do
	[[ -n "${!x:-}" ]] || { log "ERROR: importer missing var ${x}"; exit 1; }
done

for x in s3_glob partition_val sql_ddl sql_select; do
	declare -F "$x" >/dev/null || { log "ERROR: importer missing function ${x}()"; exit 1; }
done

#
# If IMPORT_KEY empty and importer defines default_key -> IMPORT_KEY = default_key()
#

[[ -z "$IMPORT_KEY" ]] && declare -F default_key >/dev/null && IMPORT_KEY="$(default_key)"
[[ "$IMPORT_KEY" =~ $KEY_REGEX ]] || { log "ERROR: --import-key '${IMPORT_KEY}' !~ ${KEY_REGEX}"; exit 1; }

#
# Call Importer File Functions for S3 Glob and Partition Value
#

IMPORT_MODE="${OVERRIDE:-$IMPORT_MODE}"
S3_GLOB="$(s3_glob "$IMPORT_KEY")"
PARTITION_VAL="$(partition_val "$IMPORT_KEY")"
TARGET_SCHEMA_DOT_TABLE="${TARGET_SCHEMA}.${TARGET_TABLE}"


#
# Define DDB path and S3 Credentials
#

DDB_PATH="${STOCK_TRADER_DWH}/stocktrader_analytics_dev.duckdb"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-127.0.0.1:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-${MINIO_ROOT_USER:-stocktrader}}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-${MINIO_ROOT_PASSWORD:-stocktrader}}"
[[ -f "$DDB_PATH" ]] || { log "ERROR: ddb n/a, path=${DDB_PATH}"; exit 1; }

SQL_S3_CONFIG=$(cat << EOF
	LOAD httpfs;
	CREATE OR REPLACE SECRET minio_secret(
		TYPE S3,
		KEY_ID '${MINIO_ACCESS_KEY}',
		SECRET '${MINIO_SECRET_KEY}',
		REGION 'us-east-1',
		ENDPOINT '${MINIO_ENDPOINT}',
		URL_STYLE 'path',
		USE_SSL false
	);
EOF
)

#
# DDB EXECUTION HELPER FUNCTIONS
#

run_ddb () 			{ duckdb "$DDB_PATH" -c "$1"; }
run_ddb_s3 () 		{ duckdb "$DDB_PATH" -c "${SQL_S3_CONFIG} $1"; }
run_ddb_csv () 		{ duckdb "$DDB_PATH" -noheader -csv -c "$1"; }
run_ddb_s3_csv () 	{ duckdb "$DDB_PATH" -noheader -csv -c "${SQL_S3_CONFIG} $1" | tail -n 1; }

log "importer=${IMPORTER}, key=${IMPORT_KEY}, mode=${IMPORT_MODE}, glob=${S3_GLOB}, target=${TARGET_SCHEMA_DOT_TABLE}"

if [[ "$VERBOSE" == true ]]; then
    echo "ddb=${DDB_PATH}"
    echo "target=${TARGET_SCHEMA_DOT_TABLE}"
    echo "partition=${PARTITION_COL}=${PARTITION_VAL}"
    echo "s3_glob=${S3_GLOB}"
    echo "ddl:"
    sql_ddl
    echo "select:"
    sql_select "$S3_GLOB" "$IMPORT_KEY"
fi

[[ "$DRY_RUN" == true ]] && { log "DRY RUN - no mas"; exit 0; }

run_ddb "INSTALL httpfs; CREATE SCHEMA IF NOT EXISTS ${TARGET_SCHEMA}; $(sql_ddl);"

N_SOURCE_OBJECTS=$(run_ddb_s3_csv "SELECT COUNT(*) FROM glob('${S3_GLOB}');")
if [[ "$N_SOURCE_OBJECTS" -eq 0 ]]; then
	[[ "$ALLOW_EMPTY" == true ]] && { log "No objects, no import, no problem. glob=${S3_GLOB}"; exit 0; }
	log "ERROR: no objects glob=${S3_GLOB}"; exit 1;
fi

N_EXISTING=$(run_ddb_csv "SELECT COUNT(*) FROM ${TARGET_SCHEMA_DOT_TABLE} WHERE ${PARTITION_COL} = ${PARTITION_VAL};");
if [[ "$N_EXISTING" -gt 0 && "$IMPORT_MODE" == "skip" ]]; then
	log "SKIP: partition_col=${PARTITION_COL}=${PARTITION_VAL}=partition_val exists (n=${N_EXISTING})"
	log "to delete existing records and insert them back use --override"
	exit 0
fi

SQL_DELETE=""
[[ "$IMPORT_MODE" == "replace" ]] && SQL_DELETE="DELETE FROM ${TARGET_SCHEMA_DOT_TABLE} WHERE ${PARTITION_COL}=${PARTITION_VAL};"

log "Importing n=${N_SOURCE_OBJECTS} (n_existing=${N_EXISTING}, mode=${IMPORT_MODE})"

SQL_XACTION=$(cat << EOF
	BEGIN TRANSACTION;
	${SQL_DELETE}
	INSERT INTO ${TARGET_SCHEMA_DOT_TABLE} BY NAME
	$(sql_select "$S3_GLOB" "$IMPORT_KEY");
	COMMIT;
EOF
);

run_ddb_s3 "$SQL_XACTION"



if declare -F sql_summary >/dev/null; then
	run_ddb "$(sql_summary "$PARTITION_VAL")"
else
	run_ddb "SELECT ${PARTITION_COL}, COUNT(*) AS n FROM ${TARGET_SCHEMA_DOT_TABLE} WHERE ${PARTITION_COL} = ${PARTITION_VAL} GROUP BY 1;"
fi

log "Done."










































