#!/usr/bin/env bash

#
# FILE: `StockTrader/scripts/ddb_minio_batch_export.sh`
#

set -euo pipefail

#
# Configure Logging
#

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

#
# Batch/Run/Shard Settings
#

BATCH_ID="$(date +%Y%m%d_%H%M%S)"
N_SHARDS=4

#
# Parse Arguments
#

DRY_RUN=false
LAST_RUN_DATE=""
VERBOSE=false

while [[ $# -gt 0 ]]; do
	case $1 in
		--dry-run)
			DRY_RUN=true
			shift
			;;
		--last-run-date)
			LAST_RUN_DATE="$2"
			shift 2
			;;
		--verbose | -v)
			VERBOSE=true
			shift
			;;
		--batch-id)
			BATCH_ID="$2"
			shift 2
			;;
		*)
			echo "Unknown option: $1"
			echo "Usage: $0 [--dry-run] [--last-run-date YYYY-MM-DD] [--verbose]"
			exit 1
			;;
	esac
done


#
# DDB Config
#

DDB_PATH="${STOCK_TRADER_DWH}/stocktrader_analytics_dev.duckdb"
MART_NAME="mart_bopm__pays_dividends"

#
# MinIO Config
#

MINIO_ENDPOINT="${MINIO_ENDPOINT:-127.0.0.1:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-${MINIO_ROOT_USER:-stocktrader}}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-${MINIO_ROOT_PASSWORD:-stocktrader}}"
S3_BUCKET="pricing-inputs"
S3_BUCKET_ADDR="s3://$S3_BUCKET/batch_$BATCH_ID"


#
# Validate Resource Existence
#

log "Start export, batchid=${BATCH_ID}"

[[ -f "$DDB_PATH" ]] || { log "ERROR: duckdb missing (path=${DDB_PATH})"; exit 1; }
command -v duckdb >/dev/null 2>&1 || { log "ERROR: duckdb cli missing"; exit 1; }




#
# Install DuckDB HTTPFS
#

duckdb "$DDB_PATH" -c "INSTALL httpfs;" >/dev/null




###################################
# 1. Query Available Market Dates #
###################################

#
# Construct Date Filter (if passed --last-run-date)
#

DATE_FILTER=""
if [[ -n "$LAST_RUN_DATE" ]]; then
	DATE_FILTER="WHERE market_date >= '$LAST_RUN_DATE'"
	log "Applying date filter: ${DATE_FILTER}"
fi

#
# Retrieve Market Dates from Mart Model + Log Count Found
#

QUERY_MARKET_DATES="
	SELECT DISTINCT market_date::VARCHAR
	  FROM main.$MART_NAME
	 $DATE_FILTER
	 ORDER BY market_date DESC
	 ;
"

DDB_MARKET_DATES="$(duckdb "$DDB_PATH" -csv -noheader "$QUERY_MARKET_DATES" 2>/dev/null | tr -d '\r')"
[[ -z "$DDB_MARKET_DATES" ]] && { log "No market dates to export"; exit 0; }

N_DATES="$(printf '%s\n' "$DDB_MARKET_DATES" | sed '/^$/d' | wc -l | tr -d ' ')"
log "Found $N_DATES market dates to export"



##############################################
# 2. Configure DuckDB for S3 Export to MinIO #
##############################################

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

	SET partitioned_write_max_open_files=128;
EOF
)

##############################################
# 3. Incrementally Export Data ~ Market Date #
##############################################

N_CONTRACTS_TOTAL=0
N_EXPORT_MARKET_DATES=0
MANIFEST_ENTRIES=""

log "Export to $S3_BUCKET_ADDR"

while IFS= read -r market_date; do
	[[ -z "$market_date" ]] && continue
	log "Processing market date: $market_date"

	QUERY_N_MARKET_DATE_RECORDS="
		SELECT COUNT(*)
		  FROM main.$MART_NAME
		 WHERE market_date = '$market_date'
		 ;
	"

	DDB_N_MARKET_DATE_RECORDS="$(duckdb "$DDB_PATH" --csv --noheader "$QUERY_N_MARKET_DATE_RECORDS" 2>/dev/null | tr -d '\r' | xargs)"
	[[ "$DRY_RUN" == true ]] && { log "DRY: Export $market_date: $DDB_N_MARKET_DATE_RECORDS"; continue; }

	COPY_EXPORT_MARKET_DATE=$(cat <<- EOF
		$S3_CONFIG_SQL

		COPY (
			SELECT
				*,
				(hash(occ) % $N_SHARDS)::UTINYINT AS shard
			  FROM main.$MART_NAME
			 WHERE market_date='$market_date'
		)
		TO '${S3_BUCKET_ADDR}/market_date=${market_date}'
		(FORMAT PARQUET, PARTITION_BY (shard), COMPRESSION ZSTD)
		;
	EOF
	)

	log "Exporting $market_date"

	if printf '%s\n' "$COPY_EXPORT_MARKET_DATE" | duckdb "$DDB_PATH" >/dev/null 2>&1; then
		log "Exported $market_date: $DDB_N_MARKET_DATE_RECORDS"
		N_EXPORT_MARKET_DATES=$((N_EXPORT_MARKET_DATES+1))
		N_CONTRACTS_TOTAL=$((N_CONTRACTS_TOTAL+DDB_N_MARKET_DATE_RECORDS))
		MANIFEST_ENTRIES+=$(cat <<- EOF
			{
				"market_date": "$market_date",
				"n_contracts": 	$DDB_N_MARKET_DATE_RECORDS,
				"n_shards": 	$N_SHARDS
			},
		EOF
		)
	else
		log "ERROR: Failed export, market_date=$market_date"
	fi
done <<< "$DDB_MARKET_DATES"


##########################
# 4. Write Manifest File #
##########################

if [[ "$DRY_RUN" != true && "$N_EXPORT_MARKET_DATES" -gt 0 ]]; then
	log "Writing manifest"
	MANIFEST_ENTRIES="${MANIFEST_ENTRIES%,}"
	MANIFEST_JSON=$(cat <<- EOF
		{
			"batch_id": "$BATCH_ID",
			"export_ts": "$(date -Iseconds)",
			"n_market_dates": $N_EXPORT_MARKET_DATES,
			"n_contracts": $N_CONTRACTS_TOTAL,
			"n_shards": $N_SHARDS,
			"partitions": [$MANIFEST_ENTRIES]
		}
	EOF
	)

	COPY_EXPORT_MANIFEST_JSON=$(cat <<- EOF
		$S3_CONFIG_SQL

		COPY (SELECT '$MANIFEST_JSON' AS content)
		TO '${S3_BUCKET_ADDR}/manifest.json'
		(FORMAT CSV, HEADER false, QUOTE '')
		;
	EOF
	)

	printf '%s\n' "$COPY_EXPORT_MANIFEST_JSON" | duckdb "$DDB_PATH" >/dev/null
fi


#
# 5. Export Summary
#

log "Export Summary"
log "Batch ID: $BATCH_ID"
log "Market Dates Exported: $N_EXPORT_MARKET_DATES"
log "Contract Records Exported: $N_CONTRACTS_TOTAL"
log "Persisted To: $S3_BUCKET_ADDR"
log "Done."
