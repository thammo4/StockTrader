#!/usr/bin/env bash

#
# FILE: `StockTrader/scripts/launch_pricing_worker.sh`
#

#
# Launches background pricing-worker processes.
# 	- Validates runtime environment + activates Python venv
# 	- Spawns N_WORKERS instances of `infrastructure.redis.job_worker`
# 	- Awaits completion of worker processes
# 	- Prints/outputs report of results/failures
#

set -euo pipefail

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$(hostname)] $*"; }

#
# ARGS
#

N_WORKERS=4
N_STEPS=225
MODEL="crr_bopm_amr_divs"
MAX_JOBS=0
TIMEOUT=10

#
# PARSE COMMAND LINE ARGS
#

usage() {
	cat <<EOF
		Usage:
			$0 [--n-workers N] [--n-steps M] [--model NAME] [--max-jobs K]

		Options:
			--n-workers 	Number of worker processes to launch, default=$N_WORKERS
			--n-steps 		Number of BOPM tree steps in pricing, default=$N_STEPS
			--model 		Pricing model name, default=$MODEL
			--max-jobs 		Max jobs per worker. 0 indicates inf. default=$MAX_JOBS
			--timeout		Redis job queue polling timeout seconds, default=$TIMEOUT
EOF
}



while [[ $# -gt 0 ]]; do
	case "$1" in
		--n-workers) N_WORKERS="$2"; shift 2 ;;
		--n-steps) N_STEPS="$2"; shift 2 ;;
		--model) MODEL="$2"; shift 2 ;;
		--max-jobs) MAX_JOBS="$2"; shift 2 ;;
		--timeout) TIMEOUT="$2"; shift 2 ;;
		*) log "Unknown option: $1"; usage; exit 1 ;;
	esac
done

#
# DIRECTORY STRUCTURE
#

APP_ROOT="$STOCK_TRADER_HOME"
VENV_ACTIVATE="$APP_ROOT/venv12/bin/activate"

[[ -d "$APP_ROOT" ]] || { log "ERROR: APP_ROOT dir $APP_ROOT for StockTrader no esta aqui"; exit 1; }


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

source "$VENV_ACTIVATE"
export PYTHONPATH="$APP_ROOT/src:$APP_ROOT"

#
# LAUNCH WORKERS
#

log "Launching: n-workers=$N_WORKERS, max-jobs=$MAX_JOBS, timeout=$TIMEOUT; model=$MODEL, n-steps=$N_STEPS"
log "redis=$REDIS_URL, minio=$MINIO_ENDPOINT"


PIDS=()
for i in $(seq 1 "$N_WORKERS"); do
	python3.12 -m infrastructure.redis.job_worker \
		--model "$MODEL" \
		--n-steps "$N_STEPS" \
		--max-jobs "$MAX_JOBS" \
		--timeout "$TIMEOUT" \
		& pid=$!
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
