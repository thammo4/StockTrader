#!/usr/bin/env bash

#
# FILE: `StockTrader/scripts/minio_ddb_import/importers/pricing_outputs.sh`
#
# Sourced by minio_ddb_import/run.sh. Not executable on its own.
# Import unit: one s3://pricing-outputs/batch_<id>/ prefix -> one batch_id partition.
# Mode is skip: a batch is closed by construction (manifest written after export); --override to repair.
#

TARGET_SCHEMA="raw"
TARGET_TABLE="qlib_priced__bopm_dividends"
PARTITION_COL="batch_id"
KEY_REGEX='^[0-9]{8}_[0-9]{6}$'
IMPORT_MODE="skip"
ALLOW_EMPTY=false

s3_glob ()       { echo "s3://pricing-outputs/batch_$1/**/results.parquet"; }
partition_val () { echo "'$1'"; }

sql_ddl () { cat << EOF
	CREATE TABLE IF NOT EXISTS ${TARGET_SCHEMA}.${TARGET_TABLE} (
		market_date 	DATE,
		occ 			VARCHAR,
		npv 			DOUBLE,
		delta 			DOUBLE,
		gamma 			DOUBLE,
		theta 			DOUBLE,
		vega 			DOUBLE,
		rho 			DOUBLE,
		iv 				DOUBLE,
		npv_err 		VARCHAR,
		greek_err 		VARCHAR,
		iv_err 			VARCHAR,
		model_name 		VARCHAR,
		n_steps 		INT,
		compute_ms 		DOUBLE,
		batch_id 		VARCHAR,
		shard			INT,
		ingest_ts 		TIMESTAMPTZ
	);
EOF
}

# $1 = s3 glob, $2 = import key (batch_id)
sql_select () { cat << EOF
	SELECT
		market_date::DATE           AS market_date,
		occ::VARCHAR                AS occ,
		npv::DOUBLE                 AS npv,
		Δ::DOUBLE                   AS delta,
		Γ::DOUBLE                   AS gamma,
		Θ::DOUBLE                   AS theta,
		ν::DOUBLE                   AS vega,
		ρ::DOUBLE                   AS rho,
		σ_iv::DOUBLE                AS iv,
		npv_err::VARCHAR            AS npv_err,
		greek_err::VARCHAR          AS greek_err,
		σ_iv_err::VARCHAR           AS iv_err,
		model_name::VARCHAR         AS model_name,
		n_steps::INT                AS n_steps,
		compute_ms::DOUBLE          AS compute_ms,
		'$2'                        AS batch_id,
		shard::INT                  AS shard,
		CURRENT_TIMESTAMP::TIMESTAMPTZ AS ingest_ts
	FROM read_parquet('$1')
EOF
}

# $1 = partition literal ('<batch_id>')
sql_summary () { cat << EOF
	SELECT
		batch_id,
		COUNT(*)                     AS n_records,
		COUNT(DISTINCT market_date)  AS n_market_dates,
		COUNT(DISTINCT occ)          AS n_occ,
		SUM(CASE WHEN npv_err IS NULL THEN 1 ELSE 0 END)                    AS n_priced,
		SUM(CASE WHEN npv_err IS NULL AND iv_err IS NULL THEN 1 ELSE 0 END) AS n_iv_solved
	FROM ${TARGET_SCHEMA}.${TARGET_TABLE}
	WHERE batch_id = $1
	GROUP BY batch_id
	;
EOF
}