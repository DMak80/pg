#!/usr/bin/env bash
# Зеркалирует внешний образ в локальный registry с сохранением мульти-архитектурности
# (linux/amd64 + linux/arm64). Имя в registry: $REG/<образ> — краткое имя как есть
# для Docker Hub, с хостом для чужих реестров (quay.io/..., ghcr.io/..., mcr...).
#
# Использование: mirror-image.sh <образ>   (например: postgres:18)
#
# ⚠️ Только ВНЕШНИЕ образы (images.txt). Локально собираемые (pgworker*:dev,
# adminpanel:dev, pgworker-node:* и т.п.) зеркалить КАТЕГОРИЧЕСКИ запрещено —
# см. docs/runbook.md.
#
# Технически: pull по digest'ам из апстрим-манифест-листа (устойчиво к локальному
# состоянию тегов) → per-arch push ($DST-amd64 / $DST-arm64) → сборка манифест-листа
# `docker manifest create` из апстрим-digest'ов → push списка (--insecure: самоподписанный
# TLS registry Go-1.24-клиенты отвергают как «not standards compliant», daemon при этом
# работает — подробности в docs/runbook.md).
set -euo pipefail

REG="${PGW_REGISTRY:-192.168.0.1:5000}"
ARCHES="${PGW_MIRROR_ARCHES:-amd64 arm64}"   # фильтр платформ (через пробел)
SRC="$1"
DST="$REG/$SRC"
repo="${SRC%%:*}"

# Digest'ы linux-манифестов нужных архитектур из апстрим-манифест-листа
# (без mapfile — bash 3.2 на macOS из коробки). Ошибка inspect (например,
# rate-limit Docker Hub) — сразу отказ, НЕ проваливается в single-arch ветку.
entries=()
list="$(docker manifest inspect "$SRC" 2>/dev/null)" || { echo "!! $SRC: docker manifest inspect упал (rate-limit апстрима?)"; exit 1; }
while read -r a dg; do
  entries+=("$a $dg")
done < <(echo "$list" | python3 -c '
import json, sys
wanted = set(sys.argv[1].split())
d = json.load(sys.stdin)
if "manifests" in d:
    for m in d["manifests"]:
        p = m.get("platform", {})
        if p.get("os") == "linux" and p.get("architecture") in wanted:
            print(p["architecture"], m["digest"])
' "$ARCHES")

if [ "${#entries[@]}" -eq 0 ]; then
  echo "!! $SRC: нет linux-манифестов для [$ARCHES] — зеркалирую как есть (одна платформа)"
  docker pull "$SRC" >/dev/null
  docker tag "$SRC" "$DST"
  docker push "$DST" >/dev/null
  echo "   $SRC -> $DST (single-arch)"
  exit 0
fi

refs=()
for e in "${entries[@]}"; do
  a="${e%% *}"; dg="${e##* }"
  docker pull "$repo@$dg" >/dev/null
  docker tag "$(docker image inspect -f '{{.Id}}' "$repo@$dg")" "$DST-$a"
  docker push "$DST-$a" >/dev/null
  refs+=("$DST-$a")
  echo "   $SRC [$a] -> $DST-$a"
done

# manifest create берёт источники только из целевого registry — per-arch теги уже там
docker manifest rm "$DST" >/dev/null 2>&1 || true
docker manifest create --insecure "$DST" "${refs[@]}" >/dev/null
docker manifest push --insecure "$DST" >/dev/null
echo "   $SRC -> $DST (multi-arch: ${#entries[@]})"
