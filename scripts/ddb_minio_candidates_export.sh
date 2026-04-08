#!/usr/bin/env bash

#
# FILE: `StockTrader/scripts/ddb_minio_candidates_export.sh`
#

# Minio s3 prefix: trading-candidates/<mart>/yyyymmdd.parquet

set -euo pipefail

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }



#
# Runtime Config
#

RT_DATE="${1:-$(date +%Y-%m-%d)}"
TMP_DIR="$(mktemp -d)"

trap "rm -rf ${TMP_DIR}" EXIT


#
# DDB Config
#

DDB_PATH="${STOCK_TRADER_DWH}/stocktrader_analytics_dev.duckdb"
MARTS=(
	"mart_vrp__high_ivp_candidates"
)


#
# HELPER FUNCTIONS - DDB EXEC, VERBOSE OUTPUT
#

run_ddb () { duckdb "$DDB_PATH" -c "$1"; }
run_ddb_csv () { duckdb "$DDB_PATH" --noheader -csv -c "$1"; }
ekko () { echo -e "$1"; echo "        -----------------------------------------------------"; }


#
# MinIO Config
#

MINIO_ENDPOINT="${MINIO_ENDPOINT:-127.0.0.1:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-${MINIO_ROOT_USER:-stocktrader}}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-${MINIO_ROOT_PASSWORD:-stocktrader}}"
S3_BUCKET="trading-candidates"

S3_CONFIG_SQL=$(cat << EOF
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
# Parse Arguments
#

log "Starting mart candidates export"

MODE="all"
TARGET=""

while [[ $# -gt 0 ]]; do
	case "$1" in
		--mart-name)
			MODE="single"
			TARGET="$2"
			shift 2
			;;
		--exclude)
			MODE="exclude"
			TARGET="$2"
			shift 2
			;;
		*)
			echo "Unknown arg $1"
			echo "Usage:"
			echo "  $0 						# export all marts"
			echo "  $0 --mart-name <mart> 	# export <mart>"
			echo "  $0 --exclude 			# export all but <mart>"
			exit 1
			;;
	esac
done




#
# Build list of marts to export
#

export_marts=()

case "${MODE}" in
	all)
		export_marts=("${MARTS[@]}")
		;;
	single)
		found=false
		for m in "${MARTS[@]}"; do
			if [[ "${m}" == "${TARGET}" ]]; then
				found=true
				break
			fi
		done
		if [[ "${found}" == false ]]; then
			echo "ERROR '${TARGET}' not real mart."
			echo "Registered:"; printf "   %s\n" "${MARTS[@]}"
			exit 1
		fi
		export_marts=("${TARGET}")
		;;
	exclude)
		for m in "${MARTS[@]}"; do
			if [[ "${m}" != "${TARGET}" ]]; then
				export_marts+=("${m}")
			fi
		done
		if [[ ${#export_marts[@]} -eq ${#MARTS[@]} ]]; then
			echo "WARN: '${TARGET}' not in MARTS. Exporting all."
		fi
		;;
esac

if [[ ${#export_marts[@]} -eq 0 ]]; then
	echo "Nothing to do"
	echo "Done."
	exit 0
fi



#
# Export Marts to Minio
#

for mart in "${export_marts[@]}"; do
	echo "[$(date +%H%M%S)] Exporting ${mart}, rt=${RT_DATE}"

	# row_count=$(ddb "${DDB_PATH}" -readonly -noheader -csv "SELECT COUNT(*) FROM ${mart};")
	row_count=$(run_ddb_csv "SELECT COUNT(*) FROM ${mart};")
	if [[ "${row_count}" -eq 0 ]]; then
		echo " 	WARN ${mart} returned 0 rows. Skip."
		continue
	fi

	s3_path="s3://${S3_BUCKET}/${mart}/${RT_DATE}.parquet"

	run_ddb "
		${S3_CONFIG_SQL}
		COPY(SELECT * FROM ${mart})
		TO '${s3_path}'
		(FORMAT PARQUET, COMPRESSION ZSTD)
	"

	echo "   ${s3_path}: n=${row_count}"
done


log "Done."









