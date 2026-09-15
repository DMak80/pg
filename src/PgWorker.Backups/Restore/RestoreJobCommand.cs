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
        # Дефолт Spilo-layout: volume-корень узла /home/postgres/pgdata, данные
        # узла — pgroot/data (arch/14 §2.1)
        PGDATA="${PGW_RESTORE_PGDATA:-$DATA_DIR/pgroot/data}"
        LOG() { printf '%s\n' "$1"; }
        FAIL() { LOG "{\"ok\":false,\"error\":\"$(printf '%s' "$1" | tr '\n' ' ' | tr -d '"')\"}"; exit 1; }

        LOG '{"phase":"downloading"}'
        mc cp --recursive "pgwbkp/$S3_BUCKET/$SRC_PREFIX/full/$BACKUP_ID/" "$PGDATA/" \
          || FAIL "download full/$BACKUP_ID failed"
        [ -f "$PGDATA/backup_label" ] || FAIL "full/$BACKUP_ID: no backup_label"
        # Пустые/транзиентные каталоги PGDATA не переживают S3: mc не хранит пустые
        # каталоги, а pg_basebackup ИСКЛЮЧАЕТ транзиентные SLRU-каталоги (pg_subtrans,
        # pg_serial, pg_snapshots, pg_notify, pg_stat_tmp, pg_dynshmem, pg_replslot).
        # Восстанавливаем канонический набор initdb — без них падает старт/чекпойнт:
        # pg_notify (FATAL could not open directory), pg_subtrans (end-of-recovery
        # checkpoint: ERROR could not access status of transaction 0 — DETAIL Could
        # not open file "pg_subtrans/0000"; инциденты E2E-гейта t05).
        mkdir -p "$PGDATA"/pg_tblspc "$PGDATA"/pg_replslot "$PGDATA"/pg_commit_ts \
                 "$PGDATA"/pg_snapshots "$PGDATA"/pg_serial "$PGDATA"/pg_twophase \
                 "$PGDATA"/pg_subtrans "$PGDATA"/pg_dynshmem \
                 "$PGDATA"/pg_notify "$PGDATA"/pg_stat "$PGDATA"/pg_stat_tmp \
                 "$PGDATA"/pg_wal/archive_status \
                 "$PGDATA"/pg_logical/snapshots "$PGDATA"/pg_logical/mappings

        # restore_command: mc качает сегмент/.history из wal/-префикса прямо в %p;
        # объекта нет → mc exit != 0 → конец WAL (канон §3.5)
        cat > "$PGDATA/restore-wal.sh" <<'WALSH'
        #!/bin/bash
        set -o pipefail
        exec mc cp "pgwbkp/$S3_BUCKET/$SRC_PREFIX/wal/$1" "$2"
        WALSH
        chmod 755 "$PGDATA/restore-wal.sh"

        AUTO="$PGDATA/postgresql.auto.conf"
        # Прибережём pristine auto.conf: наши recovery-override'ы пишем только в
        # auto.conf, а после promote возвращаем исходный — на rejoin Patroni/PG
        # обязаны видеть нодовые пути (data_directory/hba_file), а не /restore
        # (инцидент E2E-гейта t05: нода не поднималась после restore).
        cp "$AUTO" "$AUTO.orig" || FAIL "cp auto.conf failed"
        # Patroni в postgresql.conf ноды пишет АБСОЛЮТНЫЕ пути нодового layout —
        # data_directory/hba_file/ident_file (/home/postgres/pgdata/pgroot/data/…).
        # В джобе volume смонтирован в dataDir: postmaster, послушавшись их, ищет
        # pg_hba.conf не там (ENOENT, инцидент E2E-гейта t05). auto.conf читается
        # последним и возвращает пути на фактический PGDATA джоба.
        printf "data_directory = '%s'\n" "$PGDATA" >> "$AUTO"
        printf "hba_file = '%s/pg_hba.conf'\n" "$PGDATA" >> "$AUTO"
        printf "ident_file = '%s/pg_ident.conf'\n" "$PGDATA" >> "$AUTO"
        # wal_level=logical ноды тянет на ephemeral-старте logical-механику
        # (standby-снапшоты, rebuild логических слотов) — end-of-recovery
        # checkpoint падал: ERROR could not access status of transaction 0
        # (инцидент E2E-гейта t05). Для restore логический уровень не нужен:
        # Patroni на rejoin перепишет конфиг ноды.
        printf "wal_level = 'replica'\n" >> "$AUTO"
        # PG17+: WAL-суммаризация на ephemeral-старте не нужна, а end-of-recovery
        # checkpoint с ней падал (ERROR could not access status of transaction 0,
        # инцидент E2E-гейта t05); Patroni на rejoin перепишет конфиг ноды.
        printf "summarize_wal = 'off'\n" >> "$AUTO"
        printf "restore_command = '/bin/bash %s/restore-wal.sh %%f %%p'\n" "$PGDATA" >> "$AUTO"
        printf "recovery_target_action = 'promote'\n" >> "$AUTO"
        if [ -n "$TARGET_TIME" ]; then
          printf "recovery_target_time = '%s'\n" "$TARGET_TIME" >> "$AUTO"
        fi
        # Spilo-наследие исходной ноды (pg_basebackup копирует её конфигурацию):
        # preload-библиотек (bg_mon, …) в образе джоба postgres:18 нет, ssl-сертификаты
        # лежат вне PGDATA, logging_collector пишет в ../pg_log — каталог вне PGDATA,
        # которого в ephemeral-окружении джобы нет (FATAL could not open log file,
        # инцидент E2E-гейта t05). Patroni на rejoin перепишет конфиг ноды.
        printf "shared_preload_libraries = ''\n" >> "$AUTO"
        printf "ssl = off\n" >> "$AUTO"
        printf "logging_collector = off\n" >> "$AUTO"
        : > "$PGDATA/recovery.signal"
        # временный локальный trust для поллинга (сокет-only; после rejoin Patroni
        # перепишет pg_hba своим конфигом)
        sed -i '1i local all all trust' "$PGDATA/pg_hba.conf"

        # Владелец и права — ПОСЛЕ всех модификаций: mc/sed/printf создают файлы
        # под root, а postgres (101) обязан владеть PGDATA и переписывать
        # auto.conf на promote. chmod 700 обязателен: mc не сохраняет unix-права
        # (S3 их не хранит) — PGDATA приходил 0755 → FATAL «invalid permissions»
        # на pg_ctl start (инцидент E2E-гейта t05).
        chown -R 101:101 "${PGDATA%%/pgroot/data}" || FAIL "chown 101:101 failed"
        chmod 700 "$PGDATA" || FAIL "chmod 700 PGDATA failed"

        LOG '{"phase":"recovering"}'
        # HOME рута под uid 101 недоступен: mc из restore_command обязан писать
        # ~/.mc, иначе каждый вызов падает (mkdir /root/.mc: permission denied)
        # и конец WAL наступает раньше цели (инцидент E2E-гейта t05).
        export HOME=/tmp
        PGCTL() { setpriv --reuid=101 --regid=101 --clear-groups pg_ctl "$@"; }
        PGCTL -D "$PGDATA" -l /tmp/restore-pg.log -w -t 60 \
          -o "-c listen_addresses='' -c unix_socket_directories='/tmp'" start \
          || FAIL "pg_ctl start failed: $(tail -n 3 /tmp/restore-pg.log | tr '\n' ' ')"

        DEADLINE=$(( $(date +%s) + PGW_RECOVERY_TIMEOUT_SEC ))
        while :; do
          # promote после конца WAL перезапускает postmaster: соединение
          # временно недоступно — err ретраится до бюджета, это нормальный
          # переход (инцидент E2E-гейта t05: немедленный FAIL на окне рестарта)
          if IN_REC=$(psql -h /tmp -U postgres -tAc "SELECT pg_is_in_recovery()" 2>/dev/null); then
            if [ "$IN_REC" = "f" ]; then break; fi
          fi
          [ "$(date +%s)" -lt "$DEADLINE" ] || { PGCTL -D "$PGDATA" -m fast stop || true; FAIL "recovery budget exceeded ($PGW_RECOVERY_TIMEOUT_SEC s): $(tail -n 10 /tmp/restore-pg.log | tr '\n' ' ')"; }
          sleep 2
        done
        LSN=$(psql -h /tmp -U postgres -tAc "SELECT pg_current_wal_lsn()" | tr -d ' ')
        PGCTL -D "$PGDATA" -m fast stop || FAIL "pg_ctl stop failed"

        # убрать recovery-остатки: сигнал, вернуть pristine auto.conf (без
        # /restore-путей и recovery-строк), убрать trust-строку
        rm -f "$PGDATA/recovery.signal"
        mv "$AUTO.orig" "$AUTO"
        # Вычистка управляющих Patroni параметров архивации (инцидент t10,
        # 2026-09-15): pristine auto.conf снят pg_basebackup-ом с ноды-источника
        # и несёт её archive_mode=on; Patroni нового HA-scope навязывает свой
        # archive_mode=None → reload для archive_mode недостаточен → «Pending
        # restart» → отложенный рестарт postmaster рвёт соединения клиентов
        # сразу после restore. WAL-архивация в системе — внешний pg_receivewal
        # t03, archive_mode постгреса не используется: вычистка бэкап-контур
        # не ломает. sed -i пересоздаёт файл под root — chown обязателен
        # (блок chown -R выше уже прошёл).
        sed -i '/^archive_mode[[:space:]=]/d;/^archive_command[[:space:]=]/d' "$AUTO"
        chown 101:101 "$AUTO"
        sed -i '/^local all all trust$/d' "$PGDATA/pg_hba.conf"

        # System id восстановленного PGDATA (pg_controldata работает по
        # остановленному кластеру): воркер ставит его в initialize HA-scope ДО
        # подъёма нод — без щита любая пустая нода, поднятая параллельно с
        # rejoin'ом (супервиз/гонка), успевает initdb'нуться и стать лидером
        # НОВОГО пустого кластера, а восстановленная навечно получает
        # «system ID mismatch» (инцидент E2E-гейта t05, 2026-09-13).
        SYSID=$(pg_controldata "$PGDATA" | sed -n 's/^Database system identifier: *//p' | tr -d ' ')
        [ -n "$SYSID" ] || FAIL "pg_controldata: system identifier not found"

        LOG "{\"ok\":true,\"restored_to_lsn\":\"$LSN\",\"system_id\":\"$SYSID\"}"
        """;
}
