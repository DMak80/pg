using System.Diagnostics;
using System.Text.RegularExpressions;
using PgWorker.Backups;
using PgWorker.Etcd.S3;

namespace PgWorker.WalReceiver.Operations;

/// <summary>Операция restore/drill tool-контейнера (arch/19 §3.5/§3.6, spec
/// t26 §3.4 — механика 1:1 из bash-механики restore, Р15): скачивание
/// full/&lt;id&gt;/ 1:1 в PGDATA (Spilo-layout) → канонические каталоги initdb →
/// override-строки postgresql.auto.conf (restore_command = fetch-wal sub-режим
/// того же бинаря; pristine auto.conf в .orig) → standby.signal →
/// recovery.signal → trust-строка pg_hba → chown 101:101/chmod 700 ПОСЛЕ
/// модификаций → pg_ctl start под uid/gid 101 (обёртка setpriv, HOME=/tmp) →
/// поллинг pg_is_in_recovery() с бюджетом (promote-окно ретраится) → LSN →
/// stop → возврат pristine auto.conf с вычисткой archive_mode/archive_command
/// и снятие trust-строки → system_id из pg_controldata. Дрилл — тот же класс:
/// команда без target_time = latest. Резюмов нет — FAILED → новая заявка.</summary>
public sealed class RestoreOperation : IReceiverOperation
{
    // Spilo-postgres (образ pgworker-node): численные uid:gid владельца PGDATA.
    private const string PgUidGid = "101:101";

    // Бюджет одной пробы поллинга: рестарт postmaster (promote-окно) может
    // держать сокет — проба гасится kill'ом, исход err → ретрай до бюджета.
    private static readonly TimeSpan PollAttemptTimeout = TimeSpan.FromSeconds(10);

    private readonly StartRestoreRequest _request;
    private readonly IProcessRunner _runner;
    private readonly OperationRegistry _registry;
    private readonly string _pgdata;
    private readonly Func<string, IBackupS3>? _targetFactory; // тестовый шов (шаг 9)
    private readonly CancellationTokenSource _cts = new();

    public RestoreOperation(
        StartRestoreRequest request,
        IProcessRunner runner,
        OperationRegistry registry,
        string? pgdata = null,
        Func<string, IBackupS3>? targetFactory = null)
    {
        _request = request;
        _runner = runner;
        _registry = registry;
        _pgdata = pgdata ?? ToolPaths.Pgdata;
        _targetFactory = targetFactory;
    }

    public OperationSnapshot? Snapshot => _registry.Current;

    public async Task RunAsync(CancellationToken ct)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
        var token = linked.Token;

        var pgdata = _pgdata;
        // volume-корень для chown -R (bash: ${PGDATA%%/pgroot/data}).
        var volumeRoot = pgdata.EndsWith("/pgroot/data", StringComparison.Ordinal)
            ? pgdata[..^"/pgroot/data".Length]
            : pgdata;
        var autoConf = Path.Combine(pgdata, "postgresql.auto.conf");
        var autoOrig = autoConf + ".orig";
        var hbaConf = Path.Combine(pgdata, "pg_hba.conf");

        var prefix = _request.S3.Prefix.Trim('/');
        var slash = prefix.IndexOf('/');
        var cluster = slash > 0 ? prefix[..slash] : prefix;
        var shard = slash > 0 ? prefix[(slash + 1)..] : string.Empty;
        var s3Base = $"{prefix}/full/{_request.BackupId}/";

        // Тип — PoolBackupS3 (не IBackupS3): union-листинг набора — метод пула.
        PoolBackupS3? s3 = null;
        string? caBundlePath = null;
        var phase = OperationPhases.Downloading;
        try
        {
            // CA https (PEM в POST-команде, §7): материализация в temp, 0600.
            if (_request.S3.CaPem is { Length: > 0 } caPem)
            {
                caBundlePath = Path.Combine(Path.GetTempPath(), $"s3-ca-{Guid.NewGuid():N}.pem");
                var caOptions = OperatingSystem.IsWindows()
                    ? new FileStreamOptions { Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.None }
                    : new FileStreamOptions
                    {
                        Mode = FileMode.Create,
                        Access = FileAccess.Write,
                        Share = FileShare.None,
                        UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite,
                    };
                await using (var caStream = new FileStream(caBundlePath, caOptions))
                using (var writer = new StreamWriter(caStream))
                {
                    await writer.WriteAsync(caPem + "\n");
                }
            }

            // Пул-клиент (write=read=endpoints команды, симметрично verify):
            // скачивание ключа — перебор read-списка до найденного.
            s3 = new PoolBackupS3(
                _request.S3.Endpoints, _request.S3.Endpoints,
                _request.S3.AccessKey, _request.S3.SecretKey, _request.S3.Bucket,
                caBundlePath, requestTimeout: WalReceiverCore.FileAttemptTimeout,
                targetFactory: _targetFactory);

            // 2. Фаза downloading: union-лист full/<id>/ по ВСЕМ endpoints с
            //    дедупом (симметрично VerifyOperation: соло-лист другой ноды —
            //    отказ посреди заливки/лаг mesh — неполон, недокачанный набор
            //    дал бы ложный permanent-FAILED) и скачивание 1:1 в PGDATA.
            _registry.Update(new OperationSnapshot(
                _request.BackupId, OperationStates.Running, phase));
            var listed = await s3.ListPrefixUnionAsync(s3Base, ct: token);
            if (!listed.IsSuccess)
            {
                Fail(phase, OpText.OneLine(listed.Error!.Message));
                return;
            }
            Directory.CreateDirectory(pgdata);
            foreach (var obj in listed.Value)
            {
                // rel — путь под PGDATA (относительно s3Base); key — ключ
                // скачивания ОТНОСИТЕЛЬНО {cluster}/{shard}/ (контракт
                // GetObjectToFileAsync, симметрично VerifyOperation).
                var rel = obj.Key[s3Base.Length..];
                var key = obj.Key[(_request.S3.Prefix.Length + 1)..];
                var dest = Path.Combine(pgdata, rel.Replace('/', Path.DirectorySeparatorChar));
                var destDir = Path.GetDirectoryName(dest);
                if (!string.IsNullOrEmpty(destDir))
                    Directory.CreateDirectory(destDir);
                var got = await s3.GetObjectToFileAsync(cluster, shard, key, dest, token);
                if (!got.IsSuccess)
                {
                    Fail(phase, OpText.OneLine(got.Error!.Message));
                    return;
                }
            }
            // Проверка backup_label набора — нет → failed/downloading.
            if (!File.Exists(Path.Combine(pgdata, "backup_label")))
            {
                Fail(phase, $"full/{_request.BackupId}: no backup_label");
                return;
            }

            // 3. Канонические каталоги initdb: пустые/транзиентные каталоги не
            //    переживают S3, pg_basebackup исключает транзиентные SLRU-
            //    каталоги — без них падает старт/чекпойнт (pg_notify FATAL,
            //    pg_subtrans/0000 end-of-recovery checkpoint).
            foreach (var dir in new[]
                     {
                         "pg_tblspc", "pg_replslot", "pg_commit_ts", "pg_snapshots",
                         "pg_serial", "pg_twophase", "pg_subtrans", "pg_dynshmem",
                         "pg_notify", "pg_stat", "pg_stat_tmp", "pg_wal/archive_status",
                         "pg_logical/snapshots", "pg_logical/mappings",
                     })
                Directory.CreateDirectory(Path.Combine(pgdata, dir));

            // 4. postgresql.auto.conf: pristine в .orig, затем override-строки
            //    (наши recovery-пути — только в auto.conf; после promote
            //    возвращаем исходный: Patroni/PG обязаны видеть нодовые пути).
            File.Copy(autoConf, autoOrig, overwrite: true);
            var overrides = new List<string>
            {
                $"data_directory = '{pgdata}'",
                $"hba_file = '{pgdata}/pg_hba.conf'",
                $"ident_file = '{pgdata}/pg_ident.conf'",
                // wal_level=logical тянет logical-механику на ephemeral-старте
                // (end-of-recovery checkpoint падал) — restore'у не нужен.
                "wal_level = 'replica'",
                // PG17+: WAL-суммаризация на ephemeral-старте не нужна.
                "summarize_wal = 'off'",
                // restore_command — sub-режим того же бинаря (S3-контекст —
                // env процесса, секреты не в argv, Р13).
                "restore_command = '/opt/pgw-wal/walreceiver fetch-wal %f %p'",
                "recovery_target_action = 'promote'",
            };
            if (!string.IsNullOrEmpty(_request.TargetTime))
                overrides.Add($"recovery_target_time = '{_request.TargetTime}'");
            // Spilo-наследие исходной ноды: preload-библиотек в образе нет,
            // ssl-сертификаты вне PGDATA, logging_collector пишет вне PGDATA.
            overrides.Add("shared_preload_libraries = ''");
            overrides.Add("ssl = off");
            overrides.Add("logging_collector = off");
            await File.AppendAllLinesAsync(autoConf, overrides, token);

            // 5. Сигналы: standby.signal снятого с реплики набора удаляется
            //    (иначе STANDBY-старт: бесконечное ожидание WAL без recovery);
            //    режим целевого восстановления задаёт recovery.signal.
            File.Delete(Path.Combine(pgdata, "standby.signal"));
            File.Create(Path.Combine(pgdata, "recovery.signal")).Dispose();

            // 6. Временный trust для поллинга (сокет-only; после rejoin Patroni
            //    перепишет pg_hba своим конфигом).
            var hba = await File.ReadAllLinesAsync(hbaConf, token);
            await File.WriteAllLinesAsync(
                hbaConf, new[] { "local all all trust" }.Concat(hba), token);

            // 7. Владелец/права ПОСЛЕ всех модификаций: файлы .NET создал под
            //    root; chmod 700 обязателен — S3 не хранит unix-права, PGDATA
            //    иначе 0755 → FATAL «invalid permissions» на pg_ctl start.
            var (chownExit, _, chownErr) = await _runner.RunAsync(
                Sys("chown", ["-R", PgUidGid, volumeRoot]), token);
            if (chownExit != 0)
            {
                Fail(phase, OpText.FirstLine(chownErr, "chown 101:101 failed"));
                return;
            }
            var (chmodExit, _, chmodErr) = await _runner.RunAsync(
                Sys("chmod", ["700", pgdata]), token);
            if (chmodExit != 0)
            {
                Fail(phase, OpText.FirstLine(chmodErr, "chmod 700 PGDATA failed"));
                return;
            }

            // 8. Фаза recovering: pg_ctl start под uid/gid 101 (обёртка
            //    setpriv), unix-socket, сеть не слушает; env HOME=/tmp +
            //    S3-контекст fetch-wal (postgres наследует до
            //    restore_command; секреты не в argv, Р13).
            phase = OperationPhases.Recovering;
            _registry.Update(new OperationSnapshot(
                _request.BackupId, OperationStates.Running, phase));
            var startPsi = PgCtl(pgdata, "start");
            startPsi.ArgumentList.Add("-l");
            startPsi.ArgumentList.Add("/tmp/restore-pg.log");
            startPsi.ArgumentList.Add("-w");
            startPsi.ArgumentList.Add("-t");
            startPsi.ArgumentList.Add("60");
            startPsi.ArgumentList.Add("-o");
            startPsi.ArgumentList.Add("-c listen_addresses='' -c unix_socket_directories='/tmp'");
            var (startExit, _, startErr) = await _runner.RunAsync(startPsi, token);
            if (startExit != 0)
            {
                var log = await TailAsync("/tmp/restore-pg.log", 3);
                Fail(phase, OpText.FirstLine($"{startErr}\n{log}", "pg_ctl start failed"));
                return;
            }

            // 9. Поллинг pg_is_in_recovery() с бюджетом (сек): promote после
            //    конца WAL перезапускает postmaster — err ретраится до бюджета
            //    (нормальный переход), 'f' — выход.
            var budgetSec = _request.RecoveryTimeoutSec;
            var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(budgetSec);
            while (true)
            {
                var inRecovery = await ProbeAsync("SELECT pg_is_in_recovery()", token);
                if (inRecovery == "f")
                    break;
                if (DateTimeOffset.UtcNow >= deadline)
                {
                    await StopBestEffortAsync(pgdata);
                    Fail(phase, $"recovery budget exceeded ({budgetSec} s)");
                    return;
                }
                await Task.Delay(TimeSpan.FromSeconds(2), token);
            }

            // 10. restored_to_lsn + pg_ctl stop -m fast.
            var lsn = await ProbeAsync("SELECT pg_current_wal_lsn()", token);
            if (string.IsNullOrWhiteSpace(lsn))
            {
                await StopBestEffortAsync(pgdata);
                Fail(phase, "pg_current_wal_lsn: empty result");
                return;
            }
            var stopPsi = PgCtl(pgdata, "stop");
            stopPsi.ArgumentList.Add("-m");
            stopPsi.ArgumentList.Add("fast");
            var (stopExit, _, stopErr) = await _runner.RunAsync(stopPsi, token);
            if (stopExit != 0)
            {
                Fail(phase, OpText.FirstLine(stopErr, "pg_ctl stop failed"));
                return;
            }

            // 11. Чистка recovery-остатков: сигнал; возврат pristine auto.conf
            //     с вычисткой archive_mode/archive_command (pristine снят с
            //     ноды-источника и несёт её archive_mode — reload недостаточен,
            //     «Pending restart» рвёт соединения после restore); chown
            //     (запись пересоздаёт файл под root); снятие trust-строки.
            File.Delete(Path.Combine(pgdata, "recovery.signal"));
            File.Move(autoOrig, autoConf, overwrite: true);
            var pristine = await File.ReadAllLinesAsync(autoConf, token);
            var cleaned = pristine
                .Where(l => !Regex.IsMatch(l, @"^archive_mode\s*=")
                            && !Regex.IsMatch(l, @"^archive_command\s*="))
                .ToArray();
            await File.WriteAllLinesAsync(autoConf, cleaned, token);
            await _runner.RunAsync(Sys("chown", [PgUidGid, autoConf]), token);
            var hbaNow = await File.ReadAllLinesAsync(hbaConf, token);
            await File.WriteAllLinesAsync(hbaConf,
                hbaNow.Where(l => l.TrimEnd('\r') != "local all all trust"), token);

            // 12. system_id восстановленного PGDATA (pg_controldata по
            //     остановленному кластеру): щит initialize HA-scope воркера.
            var (cdExit, cdOut, cdErr) = await _runner.RunAsync(
                Sys("pg_controldata", [pgdata]), token);
            var sysId = cdExit == 0
                ? Regex.Match(cdOut, @"^Database system identifier:\s*(\S+)",
                    RegexOptions.Multiline) is { Success: true } m ? m.Groups[1].Value : null
                : null;
            if (string.IsNullOrWhiteSpace(sysId))
            {
                Fail(phase, OpText.FirstLine(cdErr, "pg_controldata: system identifier not found"));
                return;
            }

            // Итог succeeded (+RestoredToLsn, SystemId).
            _registry.Update(new OperationSnapshot(
                _request.BackupId, OperationStates.Succeeded, phase,
                RestoredToLsn: lsn, SystemId: sysId));
        }
        catch (OperationCanceledException)
        {
            // Cancel API: kill дерева уже сделан runner'ом; pg_ctl stop
            // best-effort; чистку сигналов НЕ делаем — контейнер снесёт воркер.
            await StopBestEffortAsync(_pgdata);
            Fail(phase, "cancelled");
        }
        catch (Exception e)
        {
            Fail(phase, $"{e.GetType().Name}: {e.Message.Split('\n')[0]}");
        }
        finally
        {
            if (caBundlePath is not null)
                try { File.Delete(caBundlePath); } catch { /* уже нет */ }
            if (s3 is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
        }
    }

    /// <summary>Best-effort отмена (cancel API): гасит внутренний CTS — runner
    /// убивает дерево текущего child-процесса (psql/pg_ctl/скачивание); плюс
    /// pg_ctl stop -m fast best-effort (чистку сигналов НЕ делает — контейнер
    /// снесёт воркер).</summary>
    public async Task CancelAsync()
    {
        _cts.Cancel();
        await StopBestEffortAsync(_pgdata);
    }

    // Системная утилита child-процессом (chown/chmod/pg_controldata — root).
    private static ProcessStartInfo Sys(string fileName, string[] args)
    {
        var psi = new ProcessStartInfo { FileName = fileName };
        foreach (var a in args)
            psi.ArgumentList.Add(a);
        return psi;
    }

    // pg_ctl под setpriv: --reuid/--regid/--clear-groups + утилита и аргументы;
    // env HOME=/tmp + S3-контекст fetch-wal (наследуется postgres'ом).
    private ProcessStartInfo PgCtl(string pgdata, string command)
    {
        var psi = new ProcessStartInfo { FileName = "setpriv" };
        psi.ArgumentList.Add("--reuid=101");
        psi.ArgumentList.Add("--regid=101");
        psi.ArgumentList.Add("--clear-groups");
        psi.ArgumentList.Add("pg_ctl");
        psi.ArgumentList.Add("-D");
        psi.ArgumentList.Add(pgdata);
        psi.ArgumentList.Add(command);
        // HOME рута под uid 101 недоступен (наследуется postgres'ом и
        // fetch-wal'ом — рабочие файлы пишутся в /tmp).
        psi.EnvironmentVariables["HOME"] = "/tmp";
        psi.EnvironmentVariables["PGW_S3_ENDPOINTS"] = string.Join(",", _request.S3.Endpoints);
        psi.EnvironmentVariables["PGW_S3_BUCKET"] = _request.S3.Bucket;
        psi.EnvironmentVariables["PGW_S3_PREFIX"] = _request.S3.Prefix;
        psi.EnvironmentVariables["PGW_S3_ACCESS_KEY"] = _request.S3.AccessKey;
        psi.EnvironmentVariables["PGW_S3_SECRET_KEY"] = _request.S3.SecretKey;
        if (!string.IsNullOrEmpty(_request.S3.CaPem))
            psi.EnvironmentVariables["PGW_S3_CA_PEM"] = _request.S3.CaPem;
        return psi;
    }

    // psql — root-клиент по trust (без setpriv, как bash); проба с собственным
    // таймаутом: рестарт postmaster может держать сокет — исход null (err),
    // ретрай до бюджета. Успех — trimmed stdout.
    private async Task<string?> ProbeAsync(string query, CancellationToken token)
    {
        var psi = new ProcessStartInfo { FileName = "psql" };
        psi.ArgumentList.Add("-h");
        psi.ArgumentList.Add("/tmp");
        psi.ArgumentList.Add("-U");
        psi.ArgumentList.Add("postgres");
        psi.ArgumentList.Add("-tAc");
        psi.ArgumentList.Add(query);
        try
        {
            using var attempt = CancellationTokenSource.CreateLinkedTokenSource(token);
            attempt.CancelAfter(PollAttemptTimeout);
            var (exitCode, stdout, _) = await _runner.RunAsync(psi, attempt.Token);
            return exitCode == 0 ? stdout.Trim() : null;
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            return null; // проба исчерпала таймаут — ретрай до бюджета
        }
    }

    // pg_ctl stop -m fast best-effort (короткий бюджет; ошибки глотаются).
    private async Task StopBestEffortAsync(string pgdata)
    {
        try
        {
            var stopPsi = PgCtl(pgdata, "stop");
            stopPsi.ArgumentList.Add("-m");
            stopPsi.ArgumentList.Add("fast");
            using var attempt = CancellationTokenSource.CreateLinkedTokenSource(
                CancellationToken.None);
            attempt.CancelAfter(TimeSpan.FromSeconds(10));
            await _runner.RunAsync(stopPsi, attempt.Token);
        }
        catch
        {
            // best-effort: контейнер в любом случае снесёт воркер
        }
    }

    private static async Task<string> TailAsync(string path, int lines)
    {
        try
        {
            var all = await File.ReadAllLinesAsync(path);
            return string.Join("\n", all.TakeLast(lines));
        }
        catch
        {
            return string.Empty;
        }
    }

    private void Fail(string phase, string error)
        => _registry.Update(new OperationSnapshot(
            _request.BackupId, OperationStates.Failed, phase, Error: error));
}
