#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCK_FILE="/tmp/etl_rrhh_bot.lock"

mkdir -p "${PROJECT_DIR}/logs"

# Evita ejecuciones solapadas si una corrida tarda más de 24h.
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "[$(date '+%F %T')] ETL ya está en ejecución. Se omite esta corrida." >> "${PROJECT_DIR}/logs/cron_runner.log"
  exit 0
fi

cd "${PROJECT_DIR}"
echo "[$(date '+%F %T')] Iniciando corrida ETL..." >> "${PROJECT_DIR}/logs/cron_runner.log"

docker compose -f compose.yml run --rm etl-app >> "${PROJECT_DIR}/logs/cron_runner.log" 2>&1

echo "[$(date '+%F %T')] Corrida ETL finalizada." >> "${PROJECT_DIR}/logs/cron_runner.log"
