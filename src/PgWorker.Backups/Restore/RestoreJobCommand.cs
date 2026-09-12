namespace PgWorker.Backups.Restore;

// Билдер inline-команды restore-джоба (t05, arch/19 §3.5): единственное место
// restore-механики. Скрипт (bash): download полного из S3 (mc cp --recursive,
// хвостовой / у источника кладёт содержимое каталога в цель — прецедент
// entrypoint t02), restore_command через mc, targeted recovery средствами PG
// (recovery.signal + recovery_target_*), pg_ctl под uid:gid 101 Spilo-postgres
// (в postgres:18 локальный postgres — uid 999, PGDATA принадлежит 101 —
// численное chown и запуск от 101 обязательны), поллинг выхода из recovery
// с бюджетом, затем очистка recovery-остатков. Цель latest — БЕЗ
// recovery_target_*: конец WAL (restore_command exit != 0) завершает targeted
// recovery; recovery.signal (в отличие от standby.signal) не ждёт новые
// сегменты вечно. Пути каталогов — env-контракт джоба с дефолтами
// Spilo-layout (§3.3): точка монтирования volume и PGDATA конфигурируются env.
public static class RestoreJobCommand
{
    public const string EnvMcHost = "MC_HOST_pgwbkp";
    public const string EnvBucket = "S3_BUCKET";
    public const string EnvSrcPrefix = "SRC_PREFIX";
    public const string EnvBackupId = "BACKUP_ID";
    public const string EnvTargetTime = "TARGET_TIME";
    public const string EnvRecoveryTimeoutSec = "PGW_RECOVERY_TIMEOUT_SEC";
    public const string EnvDataDir = "PGW_RESTORE_DATA_DIR";
    public const string EnvPgdata = "PGW_RESTORE_PGDATA";

    public static IReadOnlyList<string> Build() => ["bash", "-c", Script];

    private const string Script = """
        set -euo pipefail
        DATA_DIR="${PGW_RESTORE_DATA_DIR:-/restore}"
        PGDATA="${PGW_RESTORE_PGDATA:-$DATA_DIR/pgdata/pgroot/data}"
        LOG() { printf '%s\n' "$1"; }
        FAIL() { LOG "{\"ok\":false,\"error\":\"$(printf '%s' "$1" | tr '\n' ' ' | tr -d '"')\"}"; exit 1; }

        LOG '{"phase":"downloading"}'
        mc cp --recursive "pgwbkp/$S3_BUCKET/$SRC_PREFIX/full/$BACKUP_ID/" "$PGDATA/" \
          || FAIL "download full/$BACKUP_ID failed"
        [ -f "$PGDATA/backup_label" ] || FAIL "full/$BACKUP_ID: no backup_label"
        # Пустые runtime-каталоги PGDATA не переживают S3 (mc не хранит пустые
        # каталоги, pg_basebackup их не архивирует) — восстанавливаем канонический
        # набор initdb (инцидент E2E: FATAL could not open directory pg_notify).
        mkdir -p "$PGDATA"/pg_tblspc "$PGDATA"/pg_replslot "$PGDATA"/pg_commit_ts \
                 "$PGDATA"/pg_snapshots "$PGDATA"/pg_serial "$PGDATA"/pg_twophase \
                 "$PGDATA"/pg_notify "$PGDATA"/pg_stat "$PGDATA"/pg_stat_tmp \
                 "$PGDATA"/pg_wal/archive_status

        # restore_command: mc качает сегмент/.history из wal/-префикса прямо в %p;
        # объекта нет → mc exit != 0 → конец WAL (канон §3.5)
        cat > "$PGDATA/restore-wal.sh" <<'WALSH'
        #!/bin/bash
        set -o pipefail
        exec mc cp "pgwbkp/$S3_BUCKET/$SRC_PREFIX/wal/$1" "$2"
        WALSH
        chmod 755 "$PGDATA/restore-wal.sh"

        AUTO="$PGDATA/postgresql.auto.conf"
        printf "restore_command = '/bin/bash %s/restore-wal.sh %%f %%p'\n" "$PGDATA" >> "$AUTO"
        printf "recovery_target_action = 'promote'\n" >> "$AUTO"
        if [ -n "$TARGET_TIME" ]; then
          printf "recovery_target_time = '%s'\n" "$TARGET_TIME" >> "$AUTO"
        fi
        # Spilo-наследие исходной ноды (pg_basebackup копирует её конфигурацию):
        # preload-библиотек (bg_mon, …) в образе джоба postgres:18 нет, ssl-сертификаты
        # лежат вне PGDATA — для ephemeral-старта recovery отключаем (инцидент E2E:
        # FATAL could not access file bg_mon). Patroni на rejoin перепишет конфиг ноды.
        printf "shared_preload_libraries = ''\n" >> "$AUTO"
        printf "ssl = off\n" >> "$AUTO"
        : > "$PGDATA/recovery.signal"
        # временный локальный trust для поллинга (сокет-only; после rejoin Patroni
        # перепишет pg_hba своим конфигом)
        sed -i '1i local all all trust' "$PGDATA/pg_hba.conf"

        # Владелец и права — ПОСЛЕ всех модификаций: mc/sed/printf создают файлы
        # под root, а postgres (101) обязан владеть PGDATA и переписывать
        # auto.conf на promote. chmod 700 обязателен: mc не сохраняет unix-права
        # (S3 их не хранит) — PGDATA приходил 0755 → FATAL «invalid permissions»
        # на pg_ctl start (инцидент E2E-гейта t05).
        chown -R 101:101 "${PGDATA%%/pgdata/pgroot/data}" || FAIL "chown 101:101 failed"
        chmod 700 "$PGDATA" || FAIL "chmod 700 PGDATA failed"

        LOG '{"phase":"recovering"}'
        PGCTL() { setpriv --reuid=101 --regid=101 --clear-groups pg_ctl "$@"; }
        PGCTL -D "$PGDATA" -l /tmp/restore-pg.log -w -t 60 \
          -o "-c listen_addresses='' -c unix_socket_directories='/tmp'" start \
          || FAIL "pg_ctl start failed: $(tail -n 3 /tmp/restore-pg.log | tr '\n' ' ')"

        DEADLINE=$(( $(date +%s) + PGW_RECOVERY_TIMEOUT_SEC ))
        while :; do
          IN_REC=$(psql -h /tmp -U postgres -tAc "SELECT pg_is_in_recovery()" 2>/dev/null || echo err)
          [ "$IN_REC" = "f" ] && break
          [ "$IN_REC" = "t" ] || FAIL "psql probe failed: $IN_REC"
          [ "$(date +%s)" -lt "$DEADLINE" ] || { PGCTL -D "$PGDATA" -m fast stop || true; FAIL "recovery budget exceeded ($PGW_RECOVERY_TIMEOUT_SEC s)"; }
          sleep 5
        done
        LSN=$(psql -h /tmp -U postgres -tAc "SELECT pg_current_wal_lsn()" | tr -d ' ')
        PGCTL -D "$PGDATA" -m fast stop || FAIL "pg_ctl stop failed"

        # убрать recovery-остатки: сигнал, restore/recovery-строки, trust-строку
        rm -f "$PGDATA/recovery.signal"
        sed -i -e '/restore-wal\.sh/d' -e '/recovery_target/d' "$AUTO"
        sed -i '/^local all all trust$/d' "$PGDATA/pg_hba.conf"

        LOG "{\"ok\":true,\"restored_to_lsn\":\"$LSN\"}"
        """;
}
