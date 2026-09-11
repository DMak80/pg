#!/usr/bin/env bash
# Зеркалирует ВСЕ внешние образы из images.txt в локальный registry (см. mirror-image.sh).
# Идемпотентен: образы с готовым манифест-листом в registry пропускает (можно
# перезапускать, например при rate-limit апстрима — докачает недостающее).
# Ретраи при исчерпании квоты апстрима делаются снаружи (sleep + повторный запуск),
# либо вручную: ./mirror-image.sh <образ>.
set -euo pipefail
REG="${PGW_REGISTRY:-192.168.0.1:5000}"
cd "$(dirname "$0")"
while read -r img; do
  case "$img" in ''|'#'*) continue;; esac
  if docker manifest inspect --insecure "$REG/$img" >/dev/null 2>&1; then
    echo ">>> $img (уже в registry — пропуск)"
    continue
  fi
  echo ">>> $img"
  ./mirror-image.sh "$img"
done < images.txt
