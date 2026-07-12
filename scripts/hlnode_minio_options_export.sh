#!/usr/bin/env bash

#
# FILE: `StockTrader/scripts/hlnode_minio_options_export.sh`
#
# Exports options_af/SYMBOL.parquet files from a non-rudi homelab node
# to s3://db-merges/YYYY-MM-DD (current date) for later merge into rudi.
#
# Usage:
#   ./hlnode_minio_options_export.sh                                  # all symbols, full history
#   ./hlnode_minio_options_export.sh --last-run-date 2026-06-21       # only rows created_date >= cutoff
#   ./hlnode_minio_options_export.sh --symbol-list largecap_all.txt   # subset of SYMBOL.parquet files
#

set -euo pipefail

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$(hostname -s)] $*"; }


#
# Parse Arguments
#

LAST_RUN_DATE=""
SYMBOL_LIST=""

while [[ $# -gt 0 ]]; do
	case $1 in
		--last-run-date)
			LAST_RUN_DATE="$2"
			shift 2
			;;
		--symbol-list)
			SYMBOL_LIST="$2"
			shift 2
			;;
		*)
			echo "Unknown option: $1"
			echo "Usage: $0 [--last-run-date YYYY-MM-DD] [--symbol-list <file>]"
			exit 1
			;;
	esac
done

if [[ -n "$LAST_RUN_DATE" && ! "$LAST_RUN_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
	log "ERROR: --last-run-date must be YYYY-MM-DD (got '$LAST_RUN_DATE')"
	exit 1
fi


#
# Runtime Config
#

EXPORT_DATE="$(date +%Y-%m-%d)"
APP_ROOT="${STOCK_TRADER_HOME:-$HOME/Desktop/StockTrader}"
OPTIONS_DIR="${STOCK_TRADER_DWH:-$APP_ROOT/data/warehouse}/options_af"


#
# MinIO Config
#

# NOTE: on non-rudi nodes MINIO_ENDPOINT must point at rudi (e.g. 10.0.0.174:9000)
MINIO_ENDPOINT="${MINIO_ENDPOINT:?ERROR: MINIO_ENDPOINT not set (rudi host:port)}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-${MINIO_ROOT_USER:?ERROR: no MinIO access key}}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-${MINIO_ROOT_PASSWORD:?ERROR: no MinIO secret key}}"
S3_BUCKET="db-merges"
S3_PREFIX="s3://${S3_BUCKET}/${EXPORT_DATE}"

S3_CONFIG_SQL=$(cat << EOF
	INSTALL httpfs;
	LOAD httpfs;
	CREATE OR REPLACE SECRET minio_secret (
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
# HELPER FUNCTIONS - DDB EXEC (in-memory, S3 secret in-session)
#

# run_ddb ()     { duckdb -c "${S3_CONFIG_SQL} ${1}" >/dev/null; }
# run_ddb_csv () { duckdb -noheader -csv -c "${S3_CONFIG_SQL} ${1}" | tr -d '\r'; }
run_ddb () { duckdb << EOF > /dev/null
${S3_CONFIG_SQL} ${1}
EOF
}
run_ddb_csv () {
	duckdb -noheader -csv << EOF | tr -d '\r'
.output /dev/null
${S3_CONFIG_SQL}
.output
${1}
EOF
}


#
# Validate Resource Existence
#

log "Start options export, export_date=${EXPORT_DATE}, target=${S3_PREFIX}"

command -v duckdb >/dev/null 2>&1 || { log "ERROR: duckdb cli missing"; exit 1; }
[[ -d "$OPTIONS_DIR" ]] || { log "ERROR: no options dir at ${OPTIONS_DIR}"; exit 1; }
[[ -z "$SYMBOL_LIST" || -f "$SYMBOL_LIST" ]] || { log "ERROR: symbol list not found: ${SYMBOL_LIST}"; exit 1; }


#
# Build File List (all symbols, or subset from --symbol-list)
#

files=()
if [[ -n "$SYMBOL_LIST" ]]; then
	while IFS= read -r sym; do
		sym="$(echo "$sym" | tr -d '[:space:]')"
		[[ -z "$sym" ]] && continue
		f="${OPTIONS_DIR}/${sym}.parquet"
		if [[ -f "$f" ]]; then
			files+=("$f")
		else
			log "WARN: ${sym}.parquet not found locally, skipping"
		fi
	done < "$SYMBOL_LIST"
else
	files=("${OPTIONS_DIR}"/*.parquet)
fi

[[ ${#files[@]} -gt 0 ]] || { log "Nothing to export"; exit 0; }
log "Found ${#files[@]} symbol files to consider"


#
# Construct Date Filter (if passed --last-run-date, >= inclusive per convention)
#

DATE_FILTER=""
if [[ -n "$LAST_RUN_DATE" ]]; then
	DATE_FILTER="WHERE created_date::date >= DATE '${LAST_RUN_DATE}'"
	log "Applying date filter: ${DATE_FILTER}"
fi


#
# Export Loop
#

N_EXPORTED=0
N_SKIPPED=0

for f in "${files[@]}"; do
	sym="$(basename "$f" .parquet)"

	row_count="$(run_ddb_csv "SELECT COUNT(*) FROM read_parquet('${f}') ${DATE_FILTER};")"

	if [[ "$row_count" -eq 0 ]]; then
		N_SKIPPED=$((N_SKIPPED+1))
		continue
	fi

	run_ddb "
		COPY (
			SELECT *
			  FROM read_parquet('${f}')
			 ${DATE_FILTER}
		) TO '${S3_PREFIX}/${sym}.parquet' (FORMAT PARQUET);
	"

	N_EXPORTED=$((N_EXPORTED+1))
	log "Exported ${sym} (${row_count} rows)"
done

log "Done. exported=${N_EXPORTED}, skipped_empty=${N_SKIPPED}, prefix=${S3_PREFIX}"