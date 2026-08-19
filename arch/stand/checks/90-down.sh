#!/usr/bin/env bash
# Снос стенда (контейнеры + тома).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose down -v --remove-orphans
