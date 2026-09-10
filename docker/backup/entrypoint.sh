#!/bin/sh
# Джоб полного бэкапа (arch/19 §2, t02): pg_basebackup → mc в S3. Протокол —
# stdout-маркеры JSON (воркер парсирует свои строки), итог — result-JSON и
# exit-код; etcd контейнеру неизвестен. Все параметры — env (PGW_BK_*).
set -u
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
# MC_HOST_<alias> требует схему ПЕРЕД кредами: scheme://ak:sk@endpoint
# (endpoint из PGW_BK_S3_ENDPOINT несёт схему — переставляем).
S3_SCHEME="${PGW_BK_S3_ENDPOINT%%://*}"
S3_REST="${PGW_BK_S3_ENDPOINT#*://}"
[ -n "$S3_REST" ] && [ "$S3_REST" != "$PGW_BK_S3_ENDPOINT" ] \
  || FAIL "PGW_BK_S3_ENDPOINT: схема (http://|https://) обязательна"
export MC_HOST_pgw="$S3_SCHEME://$PGW_BK_S3_ACCESS_KEY:$PGW_BK_S3_SECRET_KEY@$S3_REST"

# файлы pg_basebackup 1:1 (вкл. backup_manifest и pg_wal/) — arch/19 §5.
# mc: источник с хвостовым "/" кладёт СОДЕРЖИМОЕ каталога в цель (вариант
# "/." дал бы лишний вложенный уровень <id>/full/… — ломает канон-лэйаут).
mc cp --recursive "$STAGING/full/" "pgw/$PGW_BK_S3_BUCKET/$PGW_BK_PREFIX/full/$PGW_BK_ID/" \
  || FAIL "upload full failed"

# закрытые сегменты набора -X stream → общий wal/-префикс (дублирование
# канона §5; идемпотентная перезапись; .partial/.history — домен t03)
for seg in "$STAGING"/full/pg_wal/*; do
  [ -f "$seg" ] || continue
  case "$(basename "$seg")" in *.partial|*.history) continue ;; esac
  mc cp "$seg" "pgw/$PGW_BK_S3_BUCKET/$PGW_BK_PREFIX/wal/$(basename "$seg")" \
    || FAIL "upload wal failed"
done

LOG "{\"ok\":true,\"wal_start_segment\":\"$WAL_START\",\"size_bytes\":$SIZE_BYTES}"
