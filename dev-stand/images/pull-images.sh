#!/usr/bin/env bash
# Пре-пулл внешних образов проекта перед подъёмом стенда/тестов — доставляет
# ТОЛЬКО ОТСУТСТВУЮЩЕЕ: образ, уже лежащий локально под каноническим именем,
# не тянется (local: skipped) — в сеть скрипт лезет только за недостающим.
# Отсутствующий образ: сперва локальный registry ($REG/<образ>), при его
# недоступности — апстрим (Docker Hub / quay.io / ghcr.io / mcr.microsoft.com).
# После работы образы лежат локально под каноническими именами (postgres:18,
# quay.io/coreos/etcd:v3.5.21, ...) — compose-файлы и тесты не меняются.
# Обновление уже имеющегося образа (плавающий тег) — осознанная процедура:
# docker rmi <образ> + повтор пре-пулла, либо зеркало (mirror-all.sh) на хосте
# registry (арх/правила — arch/operations/02-registry-images.md).
# Список образов — images.txt (единый источник вместе с зеркалом в registry).
set -uo pipefail   # без -e: недоступность registry — не ошибка, fallback продолжается
REG="${PGW_REGISTRY:-192.168.0.1:5000}"
cd "$(dirname "$0")"
ok_reg=0; ok_up=0; ok_local=0; fail=0
while read -r img; do
  case "$img" in ''|'#'*) continue;; esac
  # Локально уже есть — в сеть не лезем (канон 02-registry-images: пре-пулл
  # доставляет отсутствующее, обновление имеющегося — отдельная процедура).
  if docker image inspect "$img" >/dev/null 2>&1; then
    echo "local:    $img (already present, skipped)"; ok_local=$((ok_local+1)); continue
  fi
  if docker pull "$REG/$img" >/dev/null 2>&1; then
    docker tag "$REG/$img" "$img" >/dev/null 2>&1 && { echo "registry: $img"; ok_reg=$((ok_reg+1)); }
  elif docker pull "$img" >/dev/null 2>&1; then
    echo "upstream: $img (registry unavailable)"; ok_up=$((ok_up+1))
  else
    echo "FAIL:     $img"; fail=$((fail+1))
  fi
done < images.txt
echo "summary: local=$ok_local registry=$ok_reg upstream=$ok_up fail=$fail"
[ "$fail" -eq 0 ]
