#!/usr/bin/env bash

#
# FILE: `StockTrader/scripts/dbt_refresh/int_options__calcs_vrp.sh`
#

set -euo pipefail
log () { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

#
# ARGS
#

VERBOSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --verbose) VERBOSE=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done


#
# CONFIGURE DDB PATH/SCHEMA/TABLE
#

DDB_PATH="${STOCK_TRADER_DWH}/stocktrader_analytics_dev.duckdb"
DDB_TARGET_SCHEMA="main_intermediate"
DDB_TARGET_TABLE="int_options__calcs_vrp"
DDB_TARGET_SCHEMA_DOT_TABLE="${DDB_TARGET_SCHEMA}.${DDB_TARGET_TABLE}"


#
# HELPER FUNCTIONS - DDB EXEC, VERBOSE OUTPUT
#

run_ddb () { duckdb "$DDB_PATH" -c "$1"; }
run_ddb_csv () { duckdb "$DDB_PATH" --noheader -csv -c "$1"; }
ekko () { echo -e "$1"; echo "        -----------------------------------------------------"; }


#
# VALIDATE DDB DWH EXISTS AT PATH
#

[[ -f "$DDB_PATH" ]] || { log "ERROR: ddb dwh n/a, path=$DDB_PATH"; exit 1; }


#
# RETRIEVE MARKET DATES FROM UPSTREAM-OF-TARGET TABLE
#

DDB_MARKET_DATES_SQL=$(cat << 'EOF'
    SELECT DISTINCT market_date::VARCHAR
      FROM main_intermediate.int_options__joins_qlib_priced
     ORDER BY 1
     ;
EOF
)


#
# PRIMARY QUERY (identical to the model's SQL but without the config block)
#

DDB_SELECT_SQL=$(cat << 'EOF'
    WITH options_base AS (
        SELECT
            market_date,
            symbol,
            occ,
            option_type,
            expiry_date,
            ttm_days,
            strike_price,
            spot_price,
            mid_price,
            bid_price,
            ask_price,
            intrinsic_price,
            time_value_mid_price,
            time_value_bid_price,
            time_value_ask_price,
            volume,
            open_interest,
            bid_size,
            ask_size,
            sigma,
            risk_free_rate,
            dividend_yield_annualized,
            moneyness_ratio,
            moneyness_ratio_log,
            moneyness_standardized,
            moneyness_category,
            npv,
            delta,
            gamma,
            theta,
            iv
        FROM main_intermediate.int_options__joins_qlib_priced
    ),
    dte_buckets AS (
        SELECT
            *,
            CASE
                WHEN ttm_days BETWEEN 1 AND 7 THEN 'weekly'
                WHEN ttm_days BETWEEN 8 AND 30 THEN 'monthly'
                WHEN ttm_days BETWEEN 31 AND 60 THEN 'intermediate'
                WHEN ttm_days BETWEEN 61 AND 120 THEN 'quarterly'
                ELSE 'leaps'
            END AS dte_bucket
        FROM options_base
    ),
    vrp_metrics AS (
        SELECT
            *,
            iv - sigma AS vrp_spread,
            iv / NULLIF(sigma, 0) AS vrp_ratio
        FROM dte_buckets
    )
    SELECT
        market_date,
        symbol,
        occ,
        option_type,
        expiry_date,
        ttm_days,
        strike_price,
        spot_price,
        mid_price,
        bid_price,
        ask_price,
        intrinsic_price,
        time_value_mid_price,
        time_value_bid_price,
        time_value_ask_price,
        volume,
        open_interest,
        bid_size,
        ask_size,
        sigma,
        risk_free_rate,
        dividend_yield_annualized,
        moneyness_ratio,
        moneyness_ratio_log,
        moneyness_standardized,
        moneyness_category,
        npv,
        delta,
        gamma,
        theta,
        iv,
        dte_bucket,
        vrp_spread,
        vrp_ratio
    FROM vrp_metrics
EOF
)


if [[ "$VERBOSE" == "true" ]]; then
    echo "        !!!!!!!!!!!!!!!!!! SQL STATEMENTS !!!!!!!!!!!!!!!!!!";
    ekko "DDB_MARKET_DATES_SQL\n${DDB_MARKET_DATES_SQL}"
    ekko "DDB_SELECT_SQL\n${DDB_SELECT_SQL}"
fi


#
# 1. FETCH MARKET DATES
#

log "1. Fetching market dates from main_intermediate.int_options__joins_qlib_priced"

MARKET_DATES="$(run_ddb_csv "$DDB_MARKET_DATES_SQL")"
N_TOTAL=$(echo "$MARKET_DATES" | grep -c '.' || true)

log "Found $N_TOTAL market dates"


#
# 2. DROP EXISTING TARGET TABLE
#

log "2. Dropping existing table if exists: ${DDB_TARGET_SCHEMA_DOT_TABLE}"

run_ddb "DROP TABLE IF EXISTS ${DDB_TARGET_SCHEMA_DOT_TABLE};"

log "Dropped."


#
# 3. RECONSTRUCT TARGET TABLE BY ITERATING MARKET DATES
#

log "3. Reconstructing table via CTAS + Inserts"

i=0
n_inserted=0

while IFS= read -r dt; do
    [[ -z "$dt" ]] && continue
    (( i++ )) || true

    log "Processing date $i / $N_TOTAL: $dt"

    DDB_WHERE_MARKET_DATE_SQL="WHERE market_date = '${dt}'::DATE"

    if (( n_inserted == 0 )); then
        run_ddb "
            CREATE TABLE ${DDB_TARGET_SCHEMA_DOT_TABLE}
                      AS ${DDB_SELECT_SQL} ${DDB_WHERE_MARKET_DATE_SQL}
            ;
        "
    else
        run_ddb "
            INSERT INTO ${DDB_TARGET_SCHEMA_DOT_TABLE}
                ${DDB_SELECT_SQL} ${DDB_WHERE_MARKET_DATE_SQL}
            ;
        "
    fi

    (( n_inserted++ )) || true

    if (( n_inserted % 10 == 0 )); then
        N_LOADED="$(run_ddb_csv "SELECT COUNT(*) FROM ${DDB_TARGET_SCHEMA_DOT_TABLE};")"
        log "Insert progress: dates=$n_inserted, records=$N_LOADED"
    fi
done <<< "$MARKET_DATES"


#
# 4. SUMMARIZE RESULTS
#

N_RECORDS_LOADED="$(run_ddb_csv "SELECT COUNT(*) FROM ${DDB_TARGET_SCHEMA_DOT_TABLE};")"
N_DATES_LOADED="$(run_ddb_csv "SELECT COUNT(DISTINCT market_date) FROM ${DDB_TARGET_SCHEMA_DOT_TABLE};")"

log "Insert results: inserted=$n_inserted"
log "Table ${DDB_TARGET_SCHEMA_DOT_TABLE}: n=${N_RECORDS_LOADED}, dates=${N_DATES_LOADED}"

log "Done."
