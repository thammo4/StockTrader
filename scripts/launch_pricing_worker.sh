#!/usr/bin/env bash

#
# FILE: `StockTrader/scripts/launch_pricing_worker.sh`
#

set -euo pipefail

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$(hostname)] $*"; }

#
# ARGS
#

N_WORKERS=${1:-4}
N_STEPS=${2:-225}
MODEL="crr_bopm_amr_divs"
MAX_JOBS=${3:-0}

#
# DIRECTORY STRUCTURE
#

APP_ROOT="$HOME/Desktop/StockTrader"
VENV_ACTIVATE="$APP_ROOT/venv12/bin/activate"


#
# ENV VALIDATION
#

[[ -f "$VENV_ACTIVATE" ]] || { log "ERROR: no venv $VENV_ACTIVATE"; exit 1; }
[[ -n "${REDIS_URL:-}" ]] || { log "ERROR: REDIS_URL n/a"; exit 1; }
[[ -n "${MINIO_ENDPOINT:-}" ]] || { log "ERROR: MINIO_ENDPOINT n/a"; exit 1; }
[[ -n "${MINIO_ROOT_USER:-}" ]] || { log "ERROR: MINIO_ROOT_USER n/a"; exit 1; }
[[ -n "${MINIO_ROOT_PASSWORD:-}" ]] || { log "ERROR: MINIO_ROOT_PASSWORD n/a"; exit 1; }



#
# ENV CONFIG
#

cd $STOCK_TRADER_HOME

source "$VENV_ACTIVATE"
export PYTHONPATH="$APP_ROOT/src:$APP_ROOT"

#
# LAUNCH WORKERS
#

log "Launching $N_WORKERS workers, model=$MODEL"
log "redis=$REDIS_URL, minio=$MINIO_ENDPOINT"


# PIDS=()
# for i in $(seq 1 "$N_WORKERS"); do
# 	python3.12 -m infrastructure.redis.job_worker --model "$MODEL" --max-jobs "$MAX_JOBS" --timeout 10 & PIDS+=($!)
# 	log "Worker $i started (pid=${PIDS[-1]})"
# done

PIDS=()
for i in $(seq 1 "$N_WORKERS"); do
	python3.12 -m infrastructure.redis.job_worker --model "$MODEL" --max-jobs "$MAX_JOBS" --timeout 10 & pid=$!
	PIDS+=("$pid")
	log "Worker $i started, pid=$pid"
done

log "All $N_WORKERS workers running..."


#
# AWAIT WORKERS
#

N_FAILED=0
for pid in "${PIDS[@]}"; do
	if ! wait "$pid"; then
		log "WARNING: worker pid=$pid exit, non-zero status"
		N_FAILED=$((N_FAILED+1))
	fi
done

log "Workers done, failed=$N_FAILED/$N_WORKERS"

log "Done."

echo "go fuck yourself"
