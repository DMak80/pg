using System.Diagnostics;
using PgWorker.Backups;
using PgWorker.Backups.Restore;
using PgWorker.Etcd.S3;
using Shared.Etcd.Client;

namespace PgWorker.WalReceiver.Operations;

/// <summary>Операция полного бэкапа приёмника ноды (arch/19 §2, spec t26 §3.4):
/// фоновая Task процесса приёмника, параллельная стриму. Пайплайн: резолв
/// кредов/пула из etcd (как у стрима, Р4) → роль (REST-проба; write-список
/// ФИКСИРУЕТСЯ на старте — смена роли посреди полного её не рвёт, Р10) →
/// чистка staging → pg_basebackup локально 127.0.0.1:5432 под backup_exec
/// (пароль — env PGPASSWORD child-процесса, в argv/DSN НЕ передаётся, Р13) →
/// wal_start_segment из backup_label → upload 1:1 .NET PoolBackupS3 (вкл.
/// backup_manifest/pg_wal/ + дублирование закрытых сегментов набора в wal/).
/// Резюмов нет: любой отказ → failed с однострочной причиной, переснятие —
/// новым id. Статус — IMMUTABLE-снимки в OperationRegistry (атомарная замена
/// ссылки); etcd-контракта операция не знает (пишет только воркер, arch/19 §4).</summary>
public sealed class FullBackupOperation : IReceiverOperation
{
    // DSN-параметры pg_basebackup — как у стримного подключения приёмника
    // (WalReceiverOptions: 127.0.0.1:5432, роль backup_exec, БД postgres).
    private const string PgHost = "127.0.0.1";
    private const int PgPort = 5432;

    private readonly string _id;
    private readonly string _stagingRoot;
    private readonly WalReceiverIdentity _identity;
    private readonly IEtcdGateway _etcd;
    private readonly string[] _etcdEndpoints;
    private readonly IS3PoolStore _pools;
    private readonly Func<CancellationToken, Task<WalReceiverRole>> _roleProbe;
    private readonly IProcessRunner _runner;
    private readonly OperationRegistry _registry;
    private readonly CancellationTokenSource _cts = new();

    public FullBackupOperation(
        string id,
        string stagingDir,
        WalReceiverIdentity identity,
        IEtcdGateway etcd,
        string[] etcdEndpoints,
        IS3PoolStore pools,
        Func<CancellationToken, Task<WalReceiverRole>> roleProbe,
        IProcessRunner runner,
        OperationRegistry registry,
        Func<string, IBackupS3>? targetFactory = null)
    {
        _id = id;
        _stagingRoot = stagingDir;
        _identity = identity;
        _etcd = etcd;
        _etcdEndpoints = etcdEndpoints;
        _pools = pools;
        _roleProbe = roleProbe;
        _runner = runner;
        _registry = registry;
        _targetFactory = targetFactory; // тестовый шов: фейк IBackupS3 (шаг 9)
    }

    private readonly Func<string, IBackupS3>? _targetFactory;

    /// <summary>Текущий снимок операции (чтение реестра).</summary>
    public OperationSnapshot? Snapshot => _registry.Current;

    public async Task RunAsync(CancellationToken ct)
    {
        // Связка отмен cancel API и хоста: любой из них гасит операцию
        // (runner убивает дерево текущего child-процесса).
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
        var token = linked.Token;
        var opRoot = Path.Combine(_stagingRoot, _id);
        IBackupS3? s3 = null;
        string? caBundlePath = null;
        try
        {
            // 1. Резолв кредов/пула из etcd (как у стрима, Р4): ожидания-условия
            //    (нет пароля/policy) — честный transient-отказ операции; гвард
            //    G1/G3 воркера обычно исключает. Переснятие — новым id.
            var targets = await new WalReceiverTargets(_etcd, _etcdEndpoints, _pools)
                .ResolveAsync(_identity.Cluster, _identity.Shard, token);
            if (targets.WaitingPassword || targets.WaitingPolicy)
            {
                Fail(targets.Error is { Length: > 0 } e
                    ? e
                    : "backup password/policy is not resolved");
                return;
            }
            // Bucket per-install (Р4): пусто — инконсистентность инсталляции.
            if (_identity.S3Bucket is not { Length: > 0 } bucket)
            {
                Fail("env PGW_BACKUP_S3_BUCKET is not set — backups disabled on this install");
                return;
            }

            // 2. Роль + write-список: ФИКСАЦИЯ на старте операции (Р10) —
            //    смена роли посреди полного её не рвёт.
            var role = await _roleProbe(token);
            var pool = targets.Pool!;
            var writeTargets = role == WalReceiverRole.Master
                ? pool.WriteMaster
                : pool.WriteReplica;
            // Прогресс в журнал ноды (диагностика «почему долго»: приёмник не
            // молчит между 202 и терминальным статусом).
            Prog($"basebackup start (role={role.ToString().ToLowerInvariant()})");

            // CA https S3 (per-install, env ноды): материализация в temp —
            // один раз на операцию (образец WalReceiverLoop), 0600.
            if (_identity.S3CaPem is { Length: > 0 } caPem)
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

            s3 = new PoolBackupS3(
                writeTargets, pool.ReadAll, pool.AccessKey, pool.SecretKey,
                bucket, caBundlePath, requestTimeout: WalReceiverCore.FileAttemptTimeout,
                targetFactory: _targetFactory);

            // 3. Чистка staging: прошлые <id>-каталоги + создание свежего (Р5).
            // Каталог создаётся ПЕРЕД чисткой: на первой операции ноды его ещё
            // нет (каталог — не volume-маунт, а обычный путь в data-volume).
            // Список материализуется ДО удаления: снос во время ленивой
            // EnumerateFileSystemEntries по тому же каталогу — неопределённое
            // поведение перечисления (пропуски/IOException).
            Directory.CreateDirectory(_stagingRoot);
            foreach (var stale in Directory.EnumerateFileSystemEntries(_stagingRoot).ToList())
                TryDelete(stale);
            Directory.CreateDirectory(Path.Combine(opRoot, "full"));

            // 4. Фаза basebackup: pg_basebackup ЛОКАЛЬНО (127.0.0.1:5432),
            //    plain-формат -X stream (arch/19 §2). Пароль — ТОЛЬКО env
            //    PGPASSWORD child-процесса (не argv/DSN, Р13).
            _registry.Update(Snap(OperationStates.Running, OperationPhases.Basebackup));
            var psi = new ProcessStartInfo { FileName = "pg_basebackup" };
            psi.ArgumentList.Add("-d");
            psi.ArgumentList.Add($"host={PgHost} port={PgPort} user=backup_exec dbname=postgres sslmode=require");
            psi.ArgumentList.Add("-D");
            psi.ArgumentList.Add(Path.Combine(opRoot, "full"));
            psi.ArgumentList.Add("-X");
            psi.ArgumentList.Add("stream");
            psi.ArgumentList.Add("--checkpoint=fast");
            psi.ArgumentList.Add("--manifest-checksums=SHA256");
            // HOME=/tmp: walreceiver работает под postgres (supervisord
            // user=postgres), а HOME наследует от supervisord (/root) —
            // в образе Spilo там лежит postgresql.crt, недоступный uid 101
            // (Permission denied); с HOME=/tmp libpq клиентский сертификат
            // не ищет (sslmode=require его не требует). Приём arch/19 §3.5
            // (pg_ctl restore) — тот же паттерн, RestoreOperation.PgCtl.
            psi.EnvironmentVariables["HOME"] = "/tmp";
            psi.EnvironmentVariables["PGPASSWORD"] = targets.Password!;
            var (exitCode, _, stderr) = await _runner.RunAsync(psi, token);
            if (exitCode != 0)
            {
                Fail(FirstLine(stderr, "pg_basebackup failed"));
                return;
            }

            // 5. wal_start_segment из backup_label; size_bytes — сумма файлов.
            var labelText = await File.ReadAllTextAsync(
                Path.Combine(opRoot, "full", "backup_label"), token);
            var walStart = BackupLabel.WalStartSegment(labelText);
            if (walStart is null)
            {
                Fail("backup_label: START WAL LOCATION not found");
                return;
            }
            var sizeBytes = DirectorySize(Path.Combine(opRoot, "full"));
            Prog($"basebackup done: wal_start={walStart} size={sizeBytes} bytes — uploading");

            // 6. Фаза uploading: upload 1:1 (вкл. backup_manifest, pg_wal/) —
            //    ключ = <C>/<X>/full/<id>/<отн. путь>; ЗАТЕМ закрытые сегменты
            //    набора (pg_wal/* без *.partial/*.history) дублируются в
            //    общий префикс wal/ (идемпотентная перезапись, arch/19 §2).
            _registry.Update(new OperationSnapshot(
                _id, OperationStates.Uploading, OperationPhases.Uploading,
                WalStartSegment: walStart));
            var fullDir = Path.Combine(opRoot, "full");
            var uploadedFiles = 0;
            foreach (var file in Directory.EnumerateFiles(fullDir, "*", SearchOption.AllDirectories))
            {
                var rel = Path.GetRelativePath(fullDir, file).Replace('\\', '/');
                var put = await s3.PutFileAsync(
                    $"{_identity.Cluster}/{_identity.Shard}/full/{_id}/{rel}", file, token);
                if (!put.IsSuccess)
                {
                    Fail(put.Error!.Message.Split('\n')[0]);
                    return;
                }
                uploadedFiles++;
                if (uploadedFiles % UploadProgressEveryFiles == 0)
                    Prog($"upload progress: {uploadedFiles} files, {DirectorySize(fullDir)} bytes");
            }
            foreach (var file in Directory.EnumerateFiles(Path.Combine(fullDir, "pg_wal")))
            {
                var name = Path.GetFileName(file);
                if (name.EndsWith(".partial", StringComparison.Ordinal)
                    || name.EndsWith(".history", StringComparison.Ordinal))
                    continue;
                // Имя дубля — НОРМАЛИЗОВАННОЕ (строчное): pg_basebackup кладёт
                // сегменты набора ВЕРХНИМ hex, а канон ключей wal/ — строчные
                // (симметрия стриму приёмников и fetch-wal) — дубль сливается
                // со стримом, дублирования объектов нет (arch/19 §2).
                var put = await s3.PutFileAsync(
                    $"{_identity.Cluster}/{_identity.Shard}/wal/{FetchWalCommand.NormalizeSegment(name)}", file, token);
                if (!put.IsSuccess)
                {
                    Fail(put.Error!.Message.Split('\n')[0]);
                    return;
                }
            }

            // 7. Итог succeeded (чистка staging — в finally, при любом исходе).
            _registry.Update(new OperationSnapshot(
                _id, OperationStates.Succeeded, OperationPhases.Uploading,
                WalStartSegment: walStart, SizeBytes: sizeBytes));
            Prog($"COMPLETED: wal_start={walStart} size={sizeBytes} bytes, files={uploadedFiles}");
        }
        catch (OperationCanceledException)
        {
            // Cancel API / останов хоста: failed `cancelled` (переснятие новым id).
            Fail("cancelled");
        }
        catch (Exception e)
        {
            // Операция не роняет процесс: failed с однострочной причиной.
            Fail($"{e.GetType().Name}: {e.Message.Split('\n')[0]}");
        }
        finally
        {
            TryDelete(opRoot);
            if (caBundlePath is not null)
                try { File.Delete(caBundlePath); } catch { /* уже нет */ }
            if (s3 is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
        }
    }

    // Прогресс-строка в stderr (журнал ноды): шаг прогресс-диагностики —
    // приёмник не молчит в логах между 202 и терминальным статусом.
    private const int UploadProgressEveryFiles = 500;

    private void Prog(string message)
        => Console.Error.WriteLine($"walreceiver: full {_id} {message}");

    /// <summary>Best-effort отмена (cancel API): гасит внутренний CTS — runner
    /// убивает дерево текущего child-процесса; операция завершится failed
    /// `cancelled` с чисткой staging.</summary>
    public Task CancelAsync()
    {
        _cts.Cancel();
        return Task.CompletedTask;
    }

    private OperationSnapshot Snap(string state, string phase)
        => new(_id, state, phase);

    /// <summary>Публикация терминального failed-снимка (фаза — текущая);
    /// причина — в stdout контейнера (диагностика: etcd-статус живёт до
    /// переснятия, журнал остаётся после).</summary>
    private void Fail(string error)
    {
        var phase = _registry.Current?.Phase ?? OperationPhases.Basebackup;
        _registry.Update(new OperationSnapshot(
            _id, OperationStates.Failed, phase, Error: error));
        Console.Error.WriteLine(
            $"walreceiver: full {_id} FAILED at {phase}: {error}");
    }

    private static string FirstLine(string text, string fallback)
    {
        var line = text.Split('\n').Select(l => l.Trim('"', ' ').Trim()).FirstOrDefault(l => l.Length > 0);
        return string.IsNullOrEmpty(line) ? fallback : line;
    }

    private static long DirectorySize(string dir)
        => Directory.EnumerateFiles(dir, "*", SearchOption.AllDirectories)
            .Sum(f => new FileInfo(f).Length);

    private static void TryDelete(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
            else if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Чистка best-effort: ENOSPC/гонки не роняют процесс — следующий
            // старт операции повторит чистку (идемпотентно).
        }
    }
}
