#!/usr/bin/env bash
# Пре-пулл внешних образов проекта перед подъёмом стенда/тестов:
# сперва локальный registry ($REG/<образ>), при его недоступности — апстрим
# (Docker Hub / quay.io / ghcr.io / mcr.microsoft.com).
# После работы образы лежат локально под каноническими именами (postgres:18,
# quay.io/coreos/etcd:v3.5.21, ...) — compose-файлы и тесты не меняются.
# Список образов — images.txt (единый источник вместе с зеркалом в registry).
set -uo pipefail   # без -e: недоступность registry — не ошибка, fallback продолжается
REG="${PGW_REGISTRY:-192.168.0.1:5000}"
cd "$(dirname "$0")"
ok_reg=0; ok_up=0; fail=0
while read -r img; do
  case "$img" in ''|'#'*) continue;; esac
  if docker pull "$REG/$img" >/dev/null 2>&1; then
    docker tag "$REG/$img" "$img" >/dev/null 2>&1 && { echo "registry: $img"; ok_reg=$((ok_reg+1)); }
  elif docker pull "$img" >/dev/null 2>&1; then
    echo "upstream: $img (registry недоступен)"; ok_up=$((ok_up+1))
  else
    echo "FAIL:     $img"; fail=$((fail+1))
  fi
done < images.txt
echo "итог: registry=$ok_reg upstream=$ok_up fail=$fail"
[ "$fail" -eq 0 ]
