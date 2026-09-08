#!/usr/bin/env bash

#
# FILE: `StockTrader/scripts/minio_ddb_import/importers/portfolio_positions_snapshots.sh`
#
#
# Sourced by minio_ddb_import/run.sh. Not executable on its own.
# Import unit: one s3://portfolio-snapshots/YYYYMMDD/ prefix -> one market_date partition.
# Mode is replace: a prefix is open until the monitor DAG's last tick, so re-runs must converge to S3 state.
#

TARGET_SCHEMA="raw"
TARGET_TABLE="portfolio__positions_snapshots"
PARTITION_COL="market_date"
KEY_REGEX='^[0-9]{8}$'
IMPORT_MODE="replace"
ALLOW_EMPTY=true          # no open positions -> monitor writes nothing -> valid no-op

default_key ()   { date +%Y%m%d; }
s3_glob ()       { echo "s3://portfolio-snapshots/$1/*.parquet"; }
partition_val () { echo "DATE '${1:0:4}-${1:4:2}-${1:6:2}'"; }

sql_ddl () { cat << EOF
	CREATE TABLE IF NOT EXISTS ${TARGET_SCHEMA}.${TARGET_TABLE} (
		market_date    DATE,
		snapshot_ts    TIMESTAMP,
		symbol         VARCHAR,
		occ            VARCHAR,
		option_type    VARCHAR,
		expiry_date    DATE,
		expiry_type    VARCHAR,
		n_contracts    BIGINT,
		strike_price   DOUBLE,
		mid_price      DOUBLE,
		bid_price      DOUBLE,
		ask_price      DOUBLE,
		volume         BIGINT,
		open_interest  BIGINT,
		bid_size       BIGINT,
		ask_size       BIGINT,
		quantity       DOUBLE,
		cost_basis     DOUBLE,
		market_value   DOUBLE,
		upl            DOUBLE,
		upl_pct        DOUBLE,
		days_held      BIGINT,
		acq_date       DATE,
		acq_time       TIME WITH TIME ZONE,
		tradier_id     BIGINT,
		ingest_ts      TIMESTAMPTZ
	);
EOF
}

# $1 = s3 glob, $2 = import key (unused; market_date and snapshot_ts come from the object key)
sql_select () { cat << EOF
	SELECT
		strptime(regexp_extract(filename, '/([0-9]{8})/[0-9]{6}[.]parquet', 1), '%Y%m%d')::DATE             AS market_date,
		strptime(regexp_extract(filename, '/([0-9]{8}/[0-9]{6})[.]parquet', 1), '%Y%m%d/%H%M%S')::TIMESTAMP AS snapshot_ts,
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
	FROM read_parquet('$1', filename = true)
EOF
}

# $1 = partition literal (DATE '...')
sql_summary () { cat << EOF
	SELECT
		market_date,
		COUNT(*)                    AS n_rows,
		COUNT(DISTINCT snapshot_ts) AS n_snapshots,
		COUNT(DISTINCT tradier_id)  AS n_positions,
		MIN(snapshot_ts)            AS first_ts,
		MAX(snapshot_ts)            AS last_ts
	FROM ${TARGET_SCHEMA}.${TARGET_TABLE}
	WHERE market_date = $1
	GROUP BY market_date
	;
EOF
}
