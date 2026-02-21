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


echo "go fuck yourself"
echo "Done." 
