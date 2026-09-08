#!/usr/bin/env bash

#
# FILE: `StockTrader/.github/scripts/dbt-load-raw-tables.sh`
#

#
# CI Stand-IN for MinIO Importers
#
# Sources declared with `schema: raw` have no `external_location`.
# Ergo we require table to exist before dbt can build stg view.
#

set -euo pipefail
log () { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DWH="${STOCK_TRADER_DWH:?ERROR: STDWH UNSET}"
DDB_PATH="${DWH}/stocktrader_analytics_dev.duckdb"

run_ddb () { duckdb "$DDB_PATH" -c "$1"; }
run_ddb_csv () { duckdb "$DDB_PATH" -noheader -csv -c "$1"; }

load_fixture () {
	local importer="$1"
	local fixture_glob="$2"

	source "${REPO_ROOT}/scripts/minio_ddb_import/importers/${importer}.sh"

	: "${TARGET_SCHEMA:=raw}"
	local target="${TARGET_SCHEMA}.${TARGET_TABLE}"

	if ! compgen -G "$fixture_glob" >/dev/null; then
		log "ERROR: no fixtures at ${fixture_glob}"
		exit 1
	fi

	log "Loading ${target} from ${fixture_glob}"

	run_ddb "CREATE SCHEMA IF NOT EXISTS ${TARGET_SCHEMA}; $(sql_ddl)"

	run_ddb "
		BEGIN TRANSACTION;
		DELETE FROM ${target};
		INSERT INTO ${target} BY NAME
		SELECT
			market_date::DATE          AS market_date,
			snapshot_ts::TIMESTAMP     AS snapshot_ts,
			symbol::VARCHAR            AS symbol,
			occ::VARCHAR               AS occ,
			option_type::VARCHAR       AS option_type,
			expiry_date::DATE          AS expiry_date,
			expiry_type::VARCHAR       AS expiry_type,
			n_contracts::BIGINT        AS n_contracts,
			strike_price::DOUBLE       AS strike_price,
			mid_price::DOUBLE          AS mid_price,
			bid_price::DOUBLE          AS bid_price,
			ask_price::DOUBLE          AS ask_price,
			volume::BIGINT             AS volume,
			open_interest::BIGINT      AS open_interest,
			bid_size::BIGINT           AS bid_size,
			ask_size::BIGINT           AS ask_size,
			quantity::DOUBLE           AS quantity,
			cost_basis::DOUBLE         AS cost_basis,
			market_value::DOUBLE       AS market_value,
			upl::DOUBLE                AS upl,
			upl_pct::DOUBLE            AS upl_pct,
			days_held::BIGINT          AS days_held,
			acq_date::DATE             AS acq_date,
			acq_time::TIMETZ           AS acq_time,
			tradier_id::BIGINT         AS tradier_id,
			CURRENT_TIMESTAMP::TIMESTAMPTZ AS ingest_ts
		FROM read_parquet('${fixture_glob}');
		COMMIT;
	"

	local n n_null
	n=$(run_ddb_csv "SELECT COUNT(*) FROM ${target};")
	n_null=$(run_ddb_csv "SELECT COUNT(*) FROM ${target} WHERE market_date IS NULL OR snapshot_ts IS NULL;")

	[[ "$n" -eq 0 ]] && { log "ERROR: ${target} loaded 0 rows"; exit 1; }
	[[ "$n_null" -gt 0 ]] && { log "ERROR: ${target} has ${n_null} rows with null keys"; exit 1; }

	log "${target}: n=${n}"
}

load_fixture portfolio_positions_snapshots "${DWH}/portfolio_positions_snapshots/*.parquet"

log "Done."
