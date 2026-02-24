#!/usr/bin/env bash

#
# FILE: `StockTrader/scripts/minio_ddb_pricing_import.sh`
#

set -euo pipefail
log () { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }


#
# ARGS
#

BATCH_ID=""
DRY_RUN=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
	case $1 in
		--batch-id) 	BATCH_ID="$2"; shift 2 ;;
		--dry-run) 	DRY_RUN=true; shift ;;
		--verbose) 	VERBOSE=true; shift ;;
		*) echo "Unknown option: $1"; exit 1 ;;
	esac
done

[[ -z "$BATCH_ID" ]] && { log "ERROR: --batch-id required"; exit 1; }

#
# CONFIG
#

DDB_PATH="${STOCK_TRADER_DWH}/stocktrader_analytics_dev.duckdb"
MINIO_ENDPOINT="${MINIO_ENDPOINT}"
MINIO_ACCESS_KEY="${MINIO_ROOT_USER}"
MINIO_SECRET_KEY="${MINIO_ROOT_PASSWORD}"

S3_PREFIX_GLOB="s3://pricing-outputs/batch_${BATCH_ID}/**/results.parquet"
DDB_TARGET_SCHEMA="raw"
DDB_TARGET_TABLE="qlib_priced__bopm_dividends"

[[ -f "$DDB_PATH" ]] || { log "ERROR: ddb n/a, path=$DDB_PATH"; exit 1; }

#
# Install DuckDB HTTPFS / Ensure Schema Exists
#

duckdb "$DDB_PATH" -c "INSTALL httpfs;" >/dev/null
duckdb "$DDB_PATH" -c "CREATE SCHEMA IF NOT EXISTS raw;" > /dev/null


#
# DEFINE SQL STATEMENTS
#

log "Importing: minio prefix=${S3_PREFIX_GLOB}, batch=${BATCH_ID}, ddb=${DDB_TARGET_TABLE}"

[[ "$DRY_RUN" == true ]] && { log "DRY RUN - no mas"; exit 0; }


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

DDB_CREATE_TARGET_SQL=$(cat << EOF
	CREATE TABLE IF NOT EXISTS ${DDB_TARGET_SCHEMA}.${DDB_TARGET_TABLE} (
		market_date 	DATE,
		occ 			VARCHAR,
		npv 			DOUBLE,
		Δ 			DOUBLE,
		Γ 			DOUBLE,
		Θ 			DOUBLE,
		ν 			DOUBLE,
		ρ 			DOUBLE,
		σ_iv 			DOUBLE,
		npv_err 		VARCHAR,
		greek_err 		VARCHAR,
		σ_iv_err 		VARCHAR,
		model_name 		VARCHAR,
		n_steps 		INT,
		compute_ms 		DOUBLE,
		batch_id 		VARCHAR,
		shard			INT,
		ingest_ts 		TIMESTAMPTZ
	);
EOF
)

DDB_DUPLICATE_CHECK_SQL=$(cat << EOF
	SELECT
		CASE
			WHEN COUNT(*) > 0
			THEN 'SKIP: batch_id=' || '${BATCH_ID}' || ' exists with ' || COUNT(*)::VARCHAR || ' records.'
			ELSE 'OK: batch_id=' || '${BATCH_ID}' || ' up next.'
		END AS batch_duplicate_check
	FROM ${DDB_TARGET_SCHEMA}.${DDB_TARGET_TABLE}
	WHERE batch_id = '${BATCH_ID}'
	;
EOF
)

DDB_DUPLICATE_COUNT_SQL=$(cat << EOF
	SELECT COUNT(*)
	  FROM ${DDB_TARGET_SCHEMA}.${DDB_TARGET_TABLE}
	 WHERE batch_id = '${BATCH_ID}'
	;
EOF
)

DDB_SELECT_S3_SQL=$(cat << EOF
	SELECT
		market_date::DATE,
		occ::VARCHAR,
		npv::DOUBLE,
		Δ::DOUBLE,
		Γ::DOUBLE,
		Θ::DOUBLE,
		ν::DOUBLE,
		ρ::DOUBLE,
		σ_iv::DOUBLE,
		npv_err::VARCHAR,
		greek_err::VARCHAR,
		σ_iv_err::VARCHAR,
		model_name::VARCHAR,
		n_steps::INT,
		compute_ms::DOUBLE,
		'${BATCH_ID}' AS batch_id,
		shard::INT,
		CURRENT_TIMESTAMP::TIMESTAMPTZ AS ingest_ts
	FROM read_parquet('${S3_PREFIX_GLOB}')
	;
EOF
)

DDB_INSERT_SQL=$(cat << EOF
	INSERT INTO ${DDB_TARGET_SCHEMA}.${DDB_TARGET_TABLE}
	${DDB_SELECT_S3_SQL}

EOF
)

DDB_IMPORT_SUMMARY_SQL=$(cat << EOF
	SELECT
		batch_id,
		COUNT(*) AS n_records,
		COUNT(DISTINCT market_date) AS n_market_dates,
		COUNT(DISTINCT occ) AS n_occ,
		SUM(CASE WHEN npv_err IS NULL THEN 1 ELSE 0 END) AS n_priced,
		SUM(CASE WHEN npv_err IS NULL AND σ_iv_err IS NULL THEN 1 ELSE 0 END) AS n_iv_solved
	FROM ${DDB_TARGET_SCHEMA}.${DDB_TARGET_TABLE}
	WHERE batch_id = '${BATCH_ID}'
	GROUP BY batch_id
	;
EOF
)


#
# DDB EXECUTION HELPER FUNCTIONS
#

run_ddb () { duckdb "$DDB_PATH" -c "$1"; }
run_ddb_s3 () { duckdb "$DDB_PATH" -c "${S3_CONFIG_SQL} $1"; }
run_ddb_csv () { duckdb "$DDB_PATH" -noheader -csv -c "$1"; }



#
# VERBOSE DEBUGGING OUTPUT
#

if [[ "$VERBOSE" == true ]]; then
	echo "ddb: path=${DDB_PATH}, target=${DDB_TARGET_TABLE}"
	echo "minio: endpoint=${MINIO_ENDPOINT}, access=${MINIO_ACCESS_KEY}, secret=${MINIO_SECRET_KEY}"
	echo "s3 prefix glob: ${S3_PREFIX_GLOB}"

	echo -e "s3 config:\n${S3_CONFIG_SQL}";
	echo "-------"
	echo -e "create target:\n${DDB_CREATE_TARGET_SQL}";
	echo "-------"
	echo -e "dup check:\n${DDB_DUPLICATE_CHECK_SQL}";
	echo "-------"
	echo -e "select s3:\n${DDB_SELECT_S3_SQL}";
	echo "-------"
	echo -e "insert:\n${DDB_INSERT_SQL}";
	echo "-------"
	echo -e "import summary:\n${DDB_IMPORT_SUMMARY_SQL}";
fi


#
# RUN DDB COMMANDS TO IMPORT DATA FROM MINIO
#

log "Create target table in ddb..."
run_ddb "$DDB_CREATE_TARGET_SQL"

log "Verify non-duplicate batch, batch_id=${BATCH_ID}"
run_ddb "$DDB_DUPLICATE_CHECK_SQL"

EXISTING_COUNT=$(run_ddb_csv "$DDB_DUPLICATE_COUNT_SQL")
if [[ "$EXISTING_COUNT" -gt 0 ]]; then
	log "SKIP batch=${BATCH_ID}, existing=${EXISTING_COUNT}";
	exit 0
fi

log "Inserting batch=${BATCH_ID} from MinIO"
run_ddb_s3 "$DDB_INSERT_SQL"

log "Import summary"
run_ddb "$DDB_IMPORT_SUMMARY_SQL"


log "Done."

