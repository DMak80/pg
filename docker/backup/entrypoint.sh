#!/bin/sh
# Джоб полного бэкапа (arch/19 §2, t02; t36 — пул S3): pg_basebackup → mc в
# ПЕРВУЮ ДОСТУПНУЮ ноду пула. Протокол — stdout-маркеры JSON (воркер парсирует
# свои строки), итог — result-JSON и exit-код; etcd контейнеру неизвестен.
# Все параметры — env (PGW_BK_*); список пула — PGW_BK_S3_ENDPOINTS (запятая,
# порядок = приоритет записи по роли источника, arch/19 §4).
set -u
# Доверие CA https-endpoint (t42, arch/19 §7): PEM из env — в файл + SSL_CERT_FILE (mc, Go).
if [ -n "${S3_CA_PEM:-}" ]; then
  printf '%s\n' "$S3_CA_PEM" > /tmp/s3-ca.pem
  export SSL_CERT_FILE=/tmp/s3-ca.pem
fi
STAGING="${PGW_BK_STAGING_DIR:-/backup-staging}"

LOG() { printf '%s\n' "$1"; }
FAIL() {
  # однострочная причина без кавычек (JSON-безопасность)
  LOG "{\"ok\":false,\"error\":\"$(printf '%s' "$1" | tr '\n' ' ' | tr -d '"')\"}"
  exit 1
}

LOG '{"phase":"basebackup"}'
rm -rf "$STAGING/full"
pg_basebackup -d "$PGW_BK_DSN" -D "$STAGING/full" -X stream --checkpoint=spread --manifest-checksums=SHA256 \
  || FAIL "pg_basebackup failed"

# wal_start_segment из backup_label: "START WAL LOCATION: ... (file <seg>)"
WAL_START="$(sed -n 's/^START WAL LOCATION: .*(file \(.*\)).*/\1/p' "$STAGING/full/backup_label" | head -n 1)"
[ -n "$WAL_START" ] || FAIL "backup_label: START WAL LOCATION not found"
SIZE_BYTES="$(du -sb "$STAGING/full" | cut -f1)"
[ -n "$SIZE_BYTES" ] || FAIL "du staging failed"

LOG "{\"phase\":\"uploading\",\"wal_start_segment\":\"$WAL_START\"}"

# t36: список endpoints пула (роль источника, порядок = приоритет записи).
# MC_HOST_pgw<i> — алиас per-нода; per-попыточные таймауты (coreutils timeout):
# проба 5 с, загрузка 300 с — висящий endpoint НЕ вешает попытку до внешних
# бюджетов («быстрый отказ/успех, не молчание» — R3, arch/23 §7).
S3_ALIASES=""
MC_N=0
for ep in $(echo "$PGW_BK_S3_ENDPOINTS" | tr ',' ' '); do
  S3_SCHEME="${ep%%://*}"
  S3_REST="${ep#*://}"
  [ -n "$S3_REST" ] && [ "$S3_REST" != "$ep" ] || FAIL "PGW_BK_S3_ENDPOINTS: item without scheme: $ep"
  export MC_HOST_pgw$MC_N="$S3_SCHEME://$PGW_BK_S3_ACCESS_KEY:$PGW_BK_S3_SECRET_KEY@$S3_REST"
  S3_ALIASES="$S3_ALIASES pgw$MC_N"; MC_N=$((MC_N+1))
done
[ "$MC_N" -gt 0 ] || FAIL "PGW_BK_S3_ENDPOINTS: empty pool list"

# До загрузки — выбор первого доступного (короткая mc-проба, таймаут per-попытки).
alias_alive() { timeout 5 mc ls "$1/$PGW_BK_S3_BUCKET" >/dev/null 2>&1; }
FIRST_ALIVE=""
for a in $S3_ALIASES; do
  alias_alive "$a" && { FIRST_ALIVE="$a"; break; }
done
[ -n "$FIRST_ALIVE" ] || FAIL "s3-pool-exhausted: mc ls unavailable on all pool nodes"

# Загрузка: первая живая нода; падение загрузки → следующая по списку
# (переливка идемпотентна: имена объектов детерминированы; маркер исчерпания —
# s3-pool-exhausted → FAILED в статусе воркера, алерт backup-pool-unreachable).
# Источник — локальный staging (одинаков для всех алиасов), цель per-алиас.
upload() { # $1 — локальный источник; $2 — ключ цели в bucket
  for a in $S3_ALIASES; do
    timeout 300 mc cp --recursive "$1" "$a/$PGW_BK_S3_BUCKET/$2" \
      && return 0
  done
  return 1
}

# файлы pg_basebackup 1:1 (вкл. backup_manifest и pg_wal/) — arch/19 §5.
# mc: источник с хвостовым "/" кладёт СОДЕРЖИМОЕ каталога в цель (вариант
# "/." дал бы лишний вложенный уровень <id>/full/… — ломает канон-лэйаут).
upload "$STAGING/full/" "$PGW_BK_PREFIX/full/$PGW_BK_ID/" \
  || FAIL "s3-pool-exhausted: upload full failed on all pool endpoints"

# закрытые сегменты набора -X stream → общий wal/-префикс (дублирование
# канона §5; идемпотентная перезапись; .partial/.history — домен t03)
for seg in "$STAGING"/full/pg_wal/*; do
  [ -f "$seg" ] || continue
  case "$(basename "$seg")" in *.partial|*.history) continue ;; esac
  upload "$seg" "$PGW_BK_PREFIX/wal/$(basename "$seg")" \
    || FAIL "s3-pool-exhausted: upload wal failed on all pool endpoints"
done

LOG "{\"ok\":true,\"wal_start_segment\":\"$WAL_START\",\"size_bytes\":$SIZE_BYTES}"
