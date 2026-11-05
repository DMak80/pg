using System.Diagnostics;
using System.Text.RegularExpressions;
using PgWorker.Backups;
using PgWorker.Backups.Tar;
using PgWorker.Etcd.S3;

namespace PgWorker.PgNode.Operations;

/// <summary>Restore/drill-операция процесса приёмника ноды (arch/19
/// §3.5/§3.6): фиксация endpoint'а (HEAD) → потоковая распаковка ЕДИНОГО
/// объекта full/&lt;id&gt;.tar в целевой PGDATA (restore — PGDATA ноды, volume
/// пуст после демонтажа; drill — scratch-каталог backup-drill/&lt;id&gt;/pgdata,
/// прод-данные недостижимы; файла архива на диске нет, spec t01 §3.6) →
/// канонические каталоги initdb →
/// override-строки postgresql.auto.conf (restore_command = fetch-wal sub-режим
/// того же бинаря; pristine auto.conf в .orig) → standby.signal →
/// recovery.signal → trust-строка pg_hba → chmod 700 ПОСЛЕ модификаций
/// (процесс работает под postgres uid 101 — файлы скачиваются сразу нужным
/// владельцем) → pg_ctl start (напрямую, без setpriv) → HOME=/tmp →
/// поллинг pg_is_in_recovery() с бюджетом (promote-окно ретраится) → LSN →
/// stop → возврат pristine auto.conf с вычисткой archive_mode/archive_command
/// и снятие trust-строки → system_id из pg_controldata. Дрилл — тот же класс
/// (id в запросе): чистка scratch — при любом исходе, вкл. отмену. Резюмов
/// нет — FAILED → новая заявка.
/// Операторская отмена (cancel API) — снимок state=cancelled (НЕ failed —
/// «операции не было», arch/19 §3.5; воркер удаляет запись заявки / ключ
/// дрилла; scratch дрилла чистится при этом здесь).</summary>
public sealed class RestoreOperation : IReceiverOperation
{
    // Бюджет одной пробы поллинга: рестарт postmaster (promote-окно) может
    // держать сокет — проба гасится kill'ом, исход err → ретрай до бюджета.
    private static readonly TimeSpan PollAttemptTimeout = TimeSpan.FromSeconds(10);

    private readonly StartRestoreRequest _request;
    private readonly IProcessRunner _runner;
    private readonly OperationRegistry _registry;
    private readonly string _pgdata;
    private readonly bool _isDrill; // id в запросе — drill: scratch + чистка при любом исходе
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
        _isDrill = request.Id is { Length: > 0 };
        _pgdata = pgdata
                  ?? (_isDrill
                      ? Path.Combine(NodePaths.DrillRoot, request.Id!, "pgdata")
                      : NodePaths.Pgdata);
        _targetFactory = targetFactory;
    }

    public OperationSnapshot? Snapshot => _registry.Current;

    public async Task RunAsync(CancellationToken ct)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
        var token = linked.Token;

        var pgdata = _pgdata;
        var autoConf = Path.Combine(pgdata, "postgresql.auto.conf");
        var autoOrig = autoConf + ".orig";
        var hbaConf = Path.Combine(pgdata, "pg_hba.conf");

        var prefix = _request.S3.Prefix.Trim('/');
        var slash = prefix.IndexOf('/');
        var cluster = slash > 0 ? prefix[..slash] : prefix;
        var shard = slash > 0 ? prefix[(slash + 1)..] : string.Empty;

        // Patroni ноды — ВНИЗ на время RESTORE (arch/19 §3.5): живой Patroni
        // вмешивается в заполняемую PGDATA (стартует постгрес как secondary на
        // полуданных, воссоздаёт standby.signal, убивает restore-постгрес).
        // ТОЛЬКО restore: дрилл идёт на ЖИВОЙ ноде — stop spilo там гасит
        // прод-постгрес (runit внутри spilo) — 57P01 клиентам. supervisorctl —
        // через локальный сокет контейнера (chmod 0770 root:postgres,
        // supervisord.conf образа); отказ — операция не начинается: честный
        // failed-снимок вместо тихой гонки с Patroni. finally вернёт Patroni.
        if (!_isDrill)
            if (!await StopPatroniAsync(token))
                return;

        // Клиент единичного endpoint'а (endpoint чтения фиксируется HEAD'ом:
        // переключение ноды посреди потока недопустимо, spec §6.9).
        IBackupS3? s3 = null;
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

            // 2. Фаза downloading: фиксация endpoint'а чтения (HEAD до первого
            //    найденного) и потоковая распаковка ЕДИНОГО объекта
            //    full/<id>.tar в PGDATA — без файла архива на диске (spec t01
            //    §3.6, §6.9). Переключение ноды посреди потока недопустимо
            //    (частичный объект): транспортный отказ = transient-отказ
            //    операции (failed/downloading), повтор с нуля.
            Update(OperationStates.Running, phase);
            var objectKey = $"full/{_request.BackupId}.tar";
            string? endpoint = null;
            foreach (var candidateEndpoint in _request.S3.Endpoints)
            {
                var candidate = CreateClient(candidateEndpoint, caBundlePath);
                var head = await ((IStreamObjectS3)candidate).HeadObjectAsync(
                    cluster, shard, objectKey, token);
                if (head.IsSuccess && head.Value.Exists)
                {
                    endpoint = candidateEndpoint;
                    s3 = candidate;
                    break;
                }
                if (candidate is IAsyncDisposable disposable)
                    await disposable.DisposeAsync();
            }
            if (endpoint is null || s3 is null)
            {
                Fail(phase, $"{objectKey}: object not found");
                return;
            }

            var streamSw = System.Diagnostics.Stopwatch.StartNew();
            var opened = await ((IStreamObjectS3)s3).GetObjectStreamAsync(
                cluster, shard, objectKey, token);
            if (!opened.IsSuccess)
            {
                Fail(phase, OpText.OneLine(opened.Error!.Message));
                return;
            }

            var (body, totalSize) = opened.Value;
            Console.WriteLine($"[restore] tar stream {totalSize} bytes from {endpoint}");
            Directory.CreateDirectory(pgdata);
            var (extracted, objectSha) = await ExtractTarAsync(body, pgdata, token);
            Console.WriteLine(
                $"[restore] extracted {extracted} bytes in {streamSw.Elapsed.TotalSeconds:F1}s sha256={objectSha}");
            // Сквозной sha256 объекта — против etcd-поля выбранного полного
            // (задано у наших полных; у DR-орфана поля нет). Расхождение —
            // вердикт о данных (permanent), pg_ctl не стартует (06 §2).
            if (_request.Sha256 is { Length: > 0 } expectedSha
                && !string.Equals(expectedSha.ToLowerInvariant(), objectSha, StringComparison.Ordinal))
            {
                Fail(OperationPhases.Recovering,
                    $"object sha256 mismatch: expected {expectedSha.ToLowerInvariant()}, got {objectSha}");
                return;
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
                $"data_directory = '{SqlLiteral(pgdata)}'",
                $"hba_file = '{SqlLiteral(pgdata)}/pg_hba.conf'",
                $"ident_file = '{SqlLiteral(pgdata)}/pg_ident.conf'",
                // wal_level=logical тянет logical-механику на ephemeral-старте
                // (end-of-recovery checkpoint падал) — restore'у не нужен.
                "wal_level = 'replica'",
                // PG17+: WAL-суммаризация на ephemeral-старте не нужна.
                "summarize_wal = 'off'",
                // restore_command — sub-режим того же бинаря (S3-контекст —
                // env процесса, секреты не в argv, Р13).
                "restore_command = '/opt/pgw-node/pgnode fetch-wal %f %p'",
                "recovery_target_action = 'promote'",
            };
            if (!string.IsNullOrEmpty(_request.TargetTime))
            {
                // target_time в конфиг — только после точного разбора
                // PG-совместимого формата и повторного рендера (06 §2):
                // перевод строки/кавычка в значении инжектировали бы чужой
                // параметр в postgresql.auto.conf (исполнение под postgres).
                if (!TryNormalizeTargetTime(_request.TargetTime, out var targetTime))
                {
                    Fail(OperationPhases.Recovering,
                        $"invalid target_time '{OpText.OneLine(_request.TargetTime)}' " +
                        "(expected yyyy-MM-dd HH:mm:sszzz)");
                    return;
                }

                overrides.Add($"recovery_target_time = '{targetTime}'");
            }
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

            // 7. Права ПОСЛЕ модификаций: файлы созданы под postgres (uid 101 в
            //    ноде — supervisord user=postgres, владельца менять не нужно);
            //    chmod 700 обязателен — S3 не хранит unix-права, PGDATA
            //    иначе 0755 → FATAL «invalid permissions» на pg_ctl start.
            var (chmodExit, _, chmodErr) = await _runner.RunAsync(
                Sys("chmod", ["700", pgdata]), token);
            if (chmodExit != 0)
            {
                Fail(phase, OpText.FirstLine(chmodErr, "chmod 700 PGDATA failed"));
                return;
            }

            // 8. Фаза recovering: pg_ctl start (процесс уже postgres, uid 101 —
            //    напрямую), unix-socket, сеть не слушает; env HOME=/tmp +
            //    S3-контекст fetch-wal (postgres наследует до
            //    restore_command; секреты не в argv, Р13). Старт-ожидание
            //    pg_ctl -t = бюджет восстановления: до консистентной точки
            //    postgres качает сегменты fetch-wal-ом — под нагрузкой это
            //    минуты, короткий литерал рвал операцию до поллинга
            //    (arch/19 §3.5). standby.signal снимается повторно ПЕРЕД
            //    стартом: живой Patroni ноды может воссоздать файл в окне
            //    между правками и стартом — postgres уйдёт в standby-ожидание
            //    WAL вместо recovery и промоута (факт прогона).
            phase = OperationPhases.Recovering;
            Update(OperationStates.Running, phase);
            const int recoveryStartAttempts = 3;
            for (var attempt = 1; ; attempt++)
            {
                // standby.signal снимается повторно ПЕРЕД каждым стартом: живой
                // Patroni ноды может воссоздать файл в окне между правками и
                // стартом — postgres уйдёт в standby-ожидание WAL вместо
                // recovery и промоута (факт прогона).
                File.Delete(Path.Combine(pgdata, "standby.signal"));
                var startPsi = PgCtl(pgdata, "start");
                startPsi.ArgumentList.Add("-l");
                startPsi.ArgumentList.Add("/tmp/restore-pg.log");
                startPsi.ArgumentList.Add("-w");
                startPsi.ArgumentList.Add("-t");
                startPsi.ArgumentList.Add(
                    Math.Max(_request.RecoveryTimeoutSec, 60).ToString(
                        System.Globalization.CultureInfo.InvariantCulture));
                startPsi.ArgumentList.Add("-o");
                startPsi.ArgumentList.Add("-c listen_addresses='' -c unix_socket_directories='/tmp'");
                var (startExit, _, startErr) = await _runner.RunAsync(startPsi, token);
                if (startExit == 0)
                    break;

                // Ретрай старта: «recovery ended before configured recovery
                // target was reached» — WAL-объекты цели ещё не отданы
                // restore_command'ом (гонка добивки/доступности сегментов S3-пула
                // против старта recovery; факт прогона PitR: последний коммит
                // архива отставал от target на секунды). Повторный старт
                // продолжает recovery с места — arch/19 §3.5: моргания
                // покрываются повторами; ограничение — 3 попытки, пауза 5 с
                // (интервал анти-дребезга, не бюджет фаз). Иные причины старта-
                // провала — фейл как раньше.
                var log = await TailAsync("/tmp/restore-pg.log", 25);
                if (attempt < recoveryStartAttempts
                    && log.Contains("recovery ended before configured recovery target was reached",
                        StringComparison.Ordinal))
                {
                    await StopBestEffortAsync(pgdata);
                    await Task.Delay(TimeSpan.FromSeconds(5), token);
                    continue;
                }

                // Диагностика фейла старта — ПОЛНЫЙ хвост PG-лога в last_error
                // (pg_ctl «could not start server» без деталей нечитаем: контейнер
                // джобы с /tmp/restore-pg.log удаляется воркером после завершения).
                Fail(phase, $"pg_ctl start failed: {startErr.Split('\n')[0]}\n--- restore-pg.log tail ---\n{log}");
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
            //     «Pending restart» рвёт соединения после restore); снятие
            //     trust-строки.
            File.Delete(Path.Combine(pgdata, "recovery.signal"));
            File.Move(autoOrig, autoConf, overwrite: true);
            var pristine = await File.ReadAllLinesAsync(autoConf, token);
            var cleaned = pristine
                .Where(l => !Regex.IsMatch(l, @"^archive_mode\s*=")
                            && !Regex.IsMatch(l, @"^archive_command\s*="))
                .ToArray();
            await File.WriteAllLinesAsync(autoConf, cleaned, token);
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
            Update(OperationStates.Succeeded, phase, restoredToLsn: lsn, systemId: sysId);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Операторская отмена (cancel API, внешний канал): kill дерева уже
            // сделан runner'ом; pg_ctl stop best-effort; снимок state=cancelled —
            // НЕ failed (отмена = «операции не было», arch/19 §3.5; воркер
            // отличает её от vanished: запись заявки/ключ дрилла удаляются).
            await StopBestEffortAsync(_pgdata);
            Update(OperationStates.Cancelled, phase, error: "cancelled by operator");
        }
        catch (Exception e) when (IsTransport(e))
        {
            // Транспортный сбой чтения S3/сети (вкл. таймаут HttpClient) —
            // transient: маркер downloading; воркер перезапустит операцию.
            Fail(OperationPhases.Downloading, $"{e.GetType().Name}: {e.Message.Split('\n')[0]}");
        }
        catch (TarFormatException e)
        {
            // Недоверенный tar (битый формат/tar-slip/symlink-escape) — вердикт
            // о данных: permanent (фаза recovering — не transient-маркер
            // downloading), pg_ctl не стартует (06 §2).
            Fail(OperationPhases.Recovering, $"invalid tar object: {e.Message}");
        }
        catch (Exception e)
        {
            Fail(phase, $"{e.GetType().Name}: {e.Message.Split('\n')[0]}");
        }
        finally
        {
            // Patroni ноды — обратно при ЛЮБОМ исходе RESTORE (там он был
            // остановлен; поднимет постгрес уже с валидным initialize и
            // данными — REJOINING-рестарта не нужно). Дрилл Patroni не трогал.
            if (!_isDrill)
            {
                try
                {
                    await RunnerSys("supervisorctl", ["start", "spilo"], CancellationToken.None);
                }
                catch { /* best-effort */ }
            }
            // Scratch дрилла чистится при ЛЮБОМ исходе (arch/19 §3.6) —
            // терминальный ключ дрилла сразу чистый; PGDATA restore НЕ трогаем
            // (там восстановленные данные до REJOINING).
            if (_isDrill)
            {
                try
                {
                    if (Directory.Exists(_pgdata))
                        Directory.Delete(_pgdata, recursive: true);
                }
                catch { /* best-effort */ }
            }
            if (caBundlePath is not null)
                try { File.Delete(caBundlePath); } catch { /* уже нет */ }
            if (s3 is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
        }
    }

    /// <summary>Best-effort отмена (cancel API): гасит внутренний CTS — runner
    /// убивает дерево текущего child-процесса (psql/pg_ctl/скачивание); плюс
    /// pg_ctl stop -m fast best-effort (чистку PGDATA restore НЕ делает —
    /// там восстановленные данные до REJOINING; scratch дрилла чистит
    /// finally RunAsync).</summary>
    public async Task CancelAsync()
    {
        _cts.Cancel();
        await StopBestEffortAsync(_pgdata);
    }

    // Обновление снимка с переносом started_unix TryBegin (Update заменяет
    // снимок целиком) и опциональными результатами restore.
    private void Update(string state, string phase,
        string? restoredToLsn = null, string? systemId = null, string? error = null)
        => _registry.Update(new OperationSnapshot(
            _request.Id ?? _request.BackupId, state, phase,
            RestoredToLsn: restoredToLsn, SystemId: systemId,
            StartedUnix: _registry.Current?.StartedUnix, Error: error));

    // Клиент одного endpoint'а команды (шов _targetFactory — фабрика от
    // endpoint): MaxErrorRetry=0 — per-попыточный таймаут вместо ретраев SDK.
    private IBackupS3 CreateClient(string endpoint, string? caBundlePath)
        => _targetFactory is { } factory
            ? factory(endpoint)
            : new BackupS3(
                new S3ClientOptions(
                    endpoint, _request.S3.Bucket, _request.S3.AccessKey, _request.S3.SecretKey,
                    CaBundlePath: caBundlePath, MaxErrorRetry: 0),
                WalReceiverCore.FileAttemptTimeout);

    // Потоковая распаковка tar в PGDATA (spec t01 §3.6, §6.22): каталоги — из
    // записей typeflag '5' И лениво по пути файла (идемпотентно); symlinks —
    // File.CreateSymbolicLink; права — chmod-эквивалент из заголовка ПОСЛЕ
    // записи. Объект S3 — недоверенный вход: каждая запись валидируется ДО
    // файловой операции (arch/pgnode/06 §2 «Границы распаковки»): имя только
    // относительное, без `.`/`..`, канонический путь под корнем + разделитель;
    // symlink — цель относительная и резолюция внутри корня; запись сквозь
    // symlink запрещена; рекурсивного удаления по имени из tar нет; файлы
    // создаются 0600 и получают mode заголовка (без group/other-write) после
    // записи. Возврат — распаковано байт и сквозной sha256 потока (сверка с
    // etcd-полем команды до pg_ctl start, §2).
    private static async Task<(long Extracted, string Sha256)> ExtractTarAsync(
        Stream body, string pgdata, CancellationToken ct)
    {
        await using (body)
        {
            var root = Path.GetFullPath(pgdata);
            using var objectHash = System.Security.Cryptography.IncrementalHash.CreateHash(
                System.Security.Cryptography.HashAlgorithmName.SHA256);
            await using var hashing = new HashingReadStream(body, objectHash);
            var reader = new TarStreamReader(hashing);
            long extracted = 0;
            long logged = 0;
            while (await reader.ReadHeaderAsync(ct) is { } entry)
            {
                var dest = ResolveEntryPath(root, entry.Name);
                switch (entry.TypeFlag)
                {
                    case TarTypeFlags.Dir:
                        EnsureNoSymlinkParents(root, dest);
                        EnsureParentDirectory(dest);
                        if (File.Exists(dest) && !IsSymlink(dest))
                            throw new TarFormatException(
                                $"tar entry '{entry.Name}': file exists where a directory is expected");
                        Directory.CreateDirectory(dest);
                        ApplyMode(dest, entry.Mode, directory: true);
                        await reader.SkipDataAsync(ct);
                        break;
                    case TarTypeFlags.Symlink:
                        EnsureNoSymlinkParents(root, dest);
                        EnsureParentDirectory(dest);
                        EnsureLinkTargetInside(root, dest, entry.LinkName);
                        DeleteExistingNode(dest, entry.Name);
                        File.CreateSymbolicLink(dest, entry.LinkName);
                        await reader.SkipDataAsync(ct);
                        break;
                    default:
                    {
                        // Файл (типы '0'/NUL): ленивое создание каталога пути —
                        // порядок записей tar не значим (каталоговая запись
                        // может идти после файла). Родители проверяются на
                        // symlink: запись сквозь ссылку запрещена.
                        EnsureNoSymlinkParents(root, dest);
                        EnsureParentDirectory(dest);
                        DeleteExistingNode(dest, entry.Name);
                        var fileOptions = OperatingSystem.IsWindows()
                            ? new FileStreamOptions
                            {
                                Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.None,
                            }
                            : new FileStreamOptions
                            {
                                Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.None,
                                UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite,
                            };
                        await using (var file = new FileStream(dest, fileOptions))
                        {
                            await reader.OpenData().CopyToAsync(file, 64 * 1024, ct);
                        }

                        ApplyMode(dest, entry.Mode, directory: false);
                        extracted += entry.Size;
                        if (extracted - logged >= 512L * 1024 * 1024)
                        {
                            logged = extracted;
                            Console.WriteLine($"[restore] extracting {extracted} bytes");
                        }

                        break;
                    }
                }
            }

            // Хвост tar-добивки — в сквозной sha256 объекта.
            await hashing.DrainAsync(ct);
            var sha = Convert.ToHexString(objectHash.GetHashAndReset()).ToLowerInvariant();
            return (extracted, sha);
        }
    }

    // Канонический путь записи внутри корня: абсолютные имена/`..`/`.` —
    // нарушение формата (данные плохие); вхождение — строго с разделителем
    // (иначе «data2» прошёл бы проверку префикса «data»).
    internal static string ResolveEntryPath(string root, string name)
    {
        if (name.Length == 0 || Path.IsPathRooted(name)
            || name.Contains('\0', StringComparison.Ordinal))
            throw new TarFormatException($"tar entry name is not relative: '{name}'");
        var normalized = name.Replace('\\', '/');
        var parts = normalized.Split('/');
        if (parts.Any(p => p is "." or ".."))
            throw new TarFormatException($"tar entry name contains a dot segment: '{name}'");
        var dest = Path.GetFullPath(Path.Combine(
            root, normalized.Replace('/', Path.DirectorySeparatorChar)));
        if (!dest.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            throw new TarFormatException($"tar entry escapes the restore root: '{name}'");
        return dest;
    }

    // Ни один РОДИТЕЛЬСКИЙ компонент пути не является symlink (запись сквозь
    // ссылку увела бы файл за пределы корня). Несуществующие компоненты —
    // норма (ленивое создание каталогов).
    private static void EnsureNoSymlinkParents(string root, string dest)
    {
        var relative = Path.GetRelativePath(root, dest);
        var current = root;
        var parts = relative.Split(Path.DirectorySeparatorChar);
        for (var i = 0; i < parts.Length - 1; i++)
        {
            current = Path.Combine(current, parts[i]);
            if (IsSymlink(current))
                throw new TarFormatException(
                    $"tar entry writes through a symlink: '{relative}'");
            if (File.Exists(current) && !Directory.Exists(current))
                throw new TarFormatException(
                    $"tar entry parent is a file: '{relative}'");
        }
    }

    // Цель symlink: относительная и каноническая резолюция внутри корня.
    private static void EnsureLinkTargetInside(string root, string dest, string linkName)
    {
        if (linkName.Length == 0 || Path.IsPathRooted(linkName))
            throw new TarFormatException($"symlink target is not relative: '{linkName}'");
        var destDir = Path.GetDirectoryName(dest) ?? root;
        var resolved = Path.GetFullPath(Path.Combine(
            destDir, linkName.Replace('\\', '/')));
        if (!resolved.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.Ordinal)
            && !string.Equals(resolved, root, StringComparison.Ordinal))
            throw new TarFormatException($"symlink target escapes the restore root: '{linkName}'");
    }

    // Родительский каталог записи: проверен на symlink (выше) — создаётся.
    private static void EnsureParentDirectory(string dest)
    {
        var parent = Path.GetDirectoryName(dest);
        if (!string.IsNullOrEmpty(parent))
            Directory.CreateDirectory(parent);
    }

    // Существующий узел: ссылка/файл удаляются, ПУСТОЙ каталог — тоже;
    // непустой каталог — отказ (рекурсивного удаления по имени из tar нет).
    private static void DeleteExistingNode(string dest, string entryName)
    {
        if (IsSymlink(dest))
        {
            File.Delete(dest);
            return;
        }

        if (Directory.Exists(dest))
        {
            try
            {
                Directory.Delete(dest, recursive: false); // только пустой
            }
            catch (IOException)
            {
                throw new TarFormatException(
                    $"tar entry '{entryName}': non-empty directory exists at the destination");
            }

            return;
        }

        if (File.Exists(dest))
            File.Delete(dest);
    }

    private static bool IsSymlink(string path)
    {
        try
        {
            return new FileInfo(path).LinkTarget is not null;
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            return false;
        }
    }

    // Права из заголовка: только owner rwx + group/other rx (запись для
    // group/other вырезается — подделанный tar не даст world-writable).
    private static void ApplyMode(string path, int mode, bool directory)
    {
        if (OperatingSystem.IsWindows())
            return;
        var headerMode = mode == 0
            ? (directory ? 0x1C0 /* 0700 */ : 0x180 /* 0600 */)
            : mode;
        File.SetUnixFileMode(path, (UnixFileMode)(headerMode & 0x1ED));
    }

    // Литерал значения в postgresql.auto.conf: удвоение одинарной кавычки
    // (значения с переводом строки отклоняются до вызова).
    private static string SqlLiteral(string value)
        => value.Replace("'", "''", StringComparison.Ordinal);

    // target_time: точный PG-совместимый формат, повторный рендер (без
    // переводов строк/кавычек — в конфиг попадает только нормализованное
    // значение, arch/pgnode/06 §2).
    internal static bool TryNormalizeTargetTime(string value, out string normalized)
    {
        normalized = "";
        if (value.Contains('\n') || value.Contains('\r') || value.Contains('\''))
            return false;
        if (!DateTimeOffset.TryParseExact(
                value, "yyyy-MM-dd HH:mm:sszzz", System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None, out var parsed))
            return false;
        normalized = parsed.ToString(
            "yyyy-MM-dd HH:mm:sszzz", System.Globalization.CultureInfo.InvariantCulture);
        return true;
    }

    // Транспортные исключения чтения S3/сети — transient (маркер downloading).
    private static bool IsTransport(Exception e)
        => e is IOException
            or System.Net.Http.HttpRequestException
            or System.Net.Sockets.SocketException
            or Amazon.S3.AmazonS3Exception
            or TaskCanceledException or TimeoutException;

    /// <summary>Patroni ноды вниз перед восстановлением + подтверждение
    /// STOPPED (короткий бюджет): false — операция не начинается (честный
    /// failed-снимок вместо тихой гонки с Patroni за PGDATA).</summary>
    private async Task<bool> StopPatroniAsync(CancellationToken token)
    {
        try
        {
            var psi = new ProcessStartInfo { FileName = "supervisorctl" };
            psi.ArgumentList.Add("stop");
            psi.ArgumentList.Add("spilo");
            using var attempt = CancellationTokenSource.CreateLinkedTokenSource(token);
            attempt.CancelAfter(TimeSpan.FromSeconds(10));
            var (exit, _, err) = await _runner.RunAsync(psi, attempt.Token);
            if (exit != 0)
            {
                Fail(OperationPhases.Downloading,
                    OpText.OneLine($"supervisorctl stop spilo: exit {exit} {err}"));
                return false;
            }
        }
        catch (Exception e)
        {
            Fail(OperationPhases.Downloading,
                OpText.OneLine($"supervisorctl stop spilo: {e.Message.Split('\n')[0]}"));
            return false;
        }

        return true;
    }

    // Системная утилита child-процессом (supervisorctl) — best-effort: ошибка
    // запуска (нет бинарника — unit-контур) проглатывается.
    private async Task RunnerSys(string fileName, string[] args, CancellationToken token)
    {
        try
        {
            var psi = new ProcessStartInfo { FileName = fileName };
            foreach (var a in args)
                psi.ArgumentList.Add(a);
            await _runner.RunAsync(psi, token);
        }
        catch
        {
            // best-effort
        }
    }
    private static ProcessStartInfo Sys(string fileName, string[] args)
    {
        var psi = new ProcessStartInfo { FileName = fileName };
        foreach (var a in args)
            psi.ArgumentList.Add(a);
        return psi;
    }

    // pg_ctl — процесс WalReceiver в ноде уже postgres (supervisord
    // user=postgres): запуск напрямую, без setpriv/детекта uid; env
    // HOME=/tmp + S3-контекст fetch-wal (наследуется postgres'ом).
    private ProcessStartInfo PgCtl(string pgdata, string command)
    {
        var psi = new ProcessStartInfo { FileName = "pg_ctl" };
        psi.ArgumentList.Add("-D");
        psi.ArgumentList.Add(pgdata);
        psi.ArgumentList.Add(command);
        // HOME под postgres недоступен для записи так же, как под root
        // (наследуется postgres'ом и fetch-wal'ом — рабочие файлы в /tmp).
        psi.EnvironmentVariables["HOME"] = "/tmp";
        psi.EnvironmentVariables["PGW_S3_ENDPOINTS"] = string.Join(",", _request.S3.Endpoints);
        psi.EnvironmentVariables["PGW_S3_BUCKET"] = _request.S3.Bucket;
        psi.EnvironmentVariables["PGW_S3_PREFIX"] = _request.S3.Prefix;
        psi.EnvironmentVariables["PGW_S3_ACCESS_KEY"] = _request.S3.AccessKey;
        psi.EnvironmentVariables["PGW_S3_SECRET_KEY"] = _request.S3.SecretKey;
        if (!string.IsNullOrEmpty(_request.S3.CaPem))
            psi.EnvironmentVariables["PGW_S3_CA_PEM"] = _request.S3.CaPem;
        // t15 (arch/pgnode/06 §1): номер жизни выбранного полного — fetch-wal
        // собирает хвост только этой жизни; отсутствие — DR-orphan (фильтр off).
        if (_request.ChainLife is { } chainLife)
            psi.EnvironmentVariables["PGW_WAL_CHAIN_LIFE"] = chainLife.ToString();
        return psi;
    }

    // psql — локальный клиент по trust; проба с собственным
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
        => Update(OperationStates.Failed, phase, error: error);
}
