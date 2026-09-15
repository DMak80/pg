using PgWorker.Backups.Restore;

namespace PgWorker.UnitTests.Backups;

// Билдер inline-скрипта restore-джоба (t05 §3.3): фазы протокола t02,
// download из source-префикса, recovery-цели по env, очистка auto.conf,
// запуск pg_ctl под uid 101 Spilo, пути — env-контракт с дефолтами.
public class RestoreJobCommandTests
{
    // AAA: скрипт содержит фазы протокола t02 и download из source-префикса.
    [Fact]
    public void Build_PhaseMarkersAndDownloadFromSourcePrefix()
    {
        // Arrange / Act
        var cmd = RestoreJobCommand.Build();

        // Assert
        cmd.Should().Equal("bash", "-c", cmd[2]);
        cmd[2].Should().Contain("""{"phase":"downloading"}""");
        cmd[2].Should().Contain("""{"phase":"recovering"}""");
        cmd[2].Should().Contain(@"mc cp --recursive ""pgwbkp/$S3_BUCKET/$SRC_PREFIX/full/$BACKUP_ID/"" ""$PGDATA/""");
        cmd[2].Should().Contain("chown -R 101:101");
        // пустые/транзиентные каталоги PGDATA восстанавливаются (S3 не хранит
        // пустые каталоги, pg_basebackup исключает SLRU-каталоги — pg_notify,
        // pg_subtrans, pg_dynshmem; регрессии E2E-гейта t05)
        cmd[2].Should().Contain("pg_notify");
        cmd[2].Should().Contain("pg_subtrans");
        cmd[2].Should().Contain("pg_dynshmem");
        cmd[2].Should().Contain("pg_wal/archive_status");
        // владелец/права — после всех модификаций: PGDATA 0700 (проверка pg_ctl),
        // mc не сохраняет unix-права (S3 их не хранит) — регрессия E2E-гейта t05
        cmd[2].Should().Contain("chmod 700 \"$PGDATA\"");
        // chown — ПОСЛЕ trust-строки pg_hba (sed -i пересоздаёт файл под root)
        cmd[2].IndexOf("chown -R 101:101", StringComparison.Ordinal)
            .Should().BeGreaterThan(
                cmd[2].IndexOf("local all all trust", StringComparison.Ordinal),
                "все созданные под root файлы обязаны получить владельца 101");
    }

    // AAA: цель latest — без recovery_target_time; восстановление auto.conf после stop.
    [Fact]
    public void Build_RecoveryTargetsAndCleanup()
    {
        // Arrange / Act
        var script = RestoreJobCommand.Build()[2];

        // Assert — target только через env: строка конфига пишется под if
        // (printf с '%s' из $TARGET_TIME; latest → TARGET_TIME пуст → строки нет).
        script.Should().Contain("recovery_target_time = '%s'");
        script.Should().Contain("$TARGET_TIME");
        script.Should().Contain("""if [ -n "$TARGET_TIME" ]; then""");
        // Spilo-наследие копии конфигурации ноды отключается для ephemeral-старта
        // (bg_mon и preload-библиотеки в образе джоба postgres:18 отсутствуют)
        script.Should().Contain("""shared_preload_libraries = ''""");
        script.Should().Contain("ssl = off");
        // logging_collector Spilo пишет в ../pg_log — каталога вне PGDATA джобы нет
        script.Should().Contain("logging_collector = off");
        // data_directory/hba_file/ident_file Patroni зашит абсолютными нодовыми
        // путями — возвращаем на фактический PGDATA джоба (иначе postmaster
        // ищет pg_hba.conf не там)
        script.Should().Contain("data_directory = '%s'");
        script.Should().Contain("hba_file = '%s/pg_hba.conf'");
        script.Should().Contain("ident_file = '%s/pg_ident.conf'");
        script.Should().Contain("recovery_target_action = 'promote'");
        script.Should().Contain("recovery.signal");
        // pristine auto.conf возвращается после promote — на rejoin нода обязана
        // видеть свои нодовые пути, а не /restore (регрессия E2E-гейта t05)
        script.Should().Contain("cp \"$AUTO\" \"$AUTO.orig\"");
        script.Should().Contain("mv \"$AUTO.orig\" \"$AUTO\"");
        script.Should().Contain("setpriv --reuid=101 --regid=101");
        // result-строка в скрипте — внутри bash double-quotes: кавычки экранированы
        script.Should().Contain("{\\\"ok\\\":true,\\\"restored_to_lsn\\\":\\\"");
        // system_id в result (pg_controldata) — щит re-bootstrap в rejoin'е
        script.Should().Contain("\\\"system_id\\\":\\\"$SYSID\\\"");
    }

    // AAA: pristine auto.conf после возврата (mv) вычищается от управляющих
    // Patroni параметров архивации (инцидент t10: archive_mode источника →
    // «Pending restart» нового HA-scope → рестарт postmaster рвёт соединения);
    // sed -i пересоздаёт файл под root — chown 101:101 после sed обязателен
    // (блок chown -R выше по скрипту уже прошёл).
    [Fact]
    public void Build_PristineAutoConf_CleansPatroniArchiveParams()
    {
        // Arrange / Act
        var script = RestoreJobCommand.Build()[2];

        // Assert — вычистка archive_mode/archive_command из возвращённого pristine
        script.Should().Contain(
            @"sed -i '/^archive_mode[[:space:]=]/d;/^archive_command[[:space:]=]/d' ""$AUTO""");
        // вычистка — ПОСЛЕ возврата pristine: применяется к итоговому auto.conf
        script.IndexOf("sed -i '/^archive_mode", StringComparison.Ordinal)
            .Should().BeGreaterThan(
                script.IndexOf("mv \"$AUTO.orig\" \"$AUTO\"", StringComparison.Ordinal),
                "вычистка применяется к возвращённому pristine auto.conf");
        // chown — ПОСЛЕ sed: sed -i пересоздаёт файл под root
        script.IndexOf("chown 101:101 \"$AUTO\"", StringComparison.Ordinal)
            .Should().BeGreaterThan(
                script.IndexOf("sed -i '/^archive_mode", StringComparison.Ordinal),
                "sed -i пересоздаёт файл под root — владелец 101 возвращается после");
    }

    // AAA: пути каталогов — из env-контракта джоба с дефолтами Spilo-layout (§3.3).
    [Fact]
    public void Build_DataDirPgdata_ThroughEnv()
    {
        // Arrange / Act
        var script = RestoreJobCommand.Build()[2];

        // Assert — внутри тома pgroot/data (volume-корень узла /home/postgres/pgdata)
        script.Should().Contain("DATA_DIR=\"${PGW_RESTORE_DATA_DIR:-/restore}\"");
        script.Should().Contain("PGDATA=\"${PGW_RESTORE_PGDATA:-$DATA_DIR/pgroot/data}\"");
    }

    // AAA: restore_command — mc-скрипт, качающий сегмент в %p (конец WAL = exit != 0).
    [Fact]
    public void Build_RestoreCommand_WalScript()
    {
        // Arrange / Act
        var script = RestoreJobCommand.Build()[2];

        // Assert
        script.Should().Contain("restore-wal.sh");
        script.Should().Contain(@"exec mc cp ""pgwbkp/$S3_BUCKET/$SRC_PREFIX/wal/$1"" ""$2""");
        script.Should().Contain("restore_command = '/bin/bash");
        script.Should().Contain("""recovery.signal""");
    }

    // AAA: env-имена контракта джоба (§3.3) — константы для спеки (Task 5).
    [Fact]
    public void Env_NamesContract()
    {
        // Act / Assert
        RestoreJobCommand.EnvMcHost.Should().Be("MC_HOST_pgwbkp");
        RestoreJobCommand.EnvBucket.Should().Be("S3_BUCKET");
        RestoreJobCommand.EnvSrcPrefix.Should().Be("SRC_PREFIX");
        RestoreJobCommand.EnvBackupId.Should().Be("BACKUP_ID");
        RestoreJobCommand.EnvTargetTime.Should().Be("TARGET_TIME");
        RestoreJobCommand.EnvRecoveryTimeoutSec.Should().Be("PGW_RECOVERY_TIMEOUT_SEC");
        RestoreJobCommand.EnvDataDir.Should().Be("PGW_RESTORE_DATA_DIR");
        RestoreJobCommand.EnvPgdata.Should().Be("PGW_RESTORE_PGDATA");
    }
}
