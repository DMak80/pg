using System.Text;
using PgWorker.Backups;
using PgWorker.Backups.Restore;
using PgWorker.Backups.Tar;
using PgWorker.Etcd.S3;
using PgWorker.PgNode.BaseBackup;
using Shared.Etcd.Client;

namespace PgWorker.PgNode.Operations;

/// <summary>Операция полного бэкапа приёмника ноды (arch/pgnode/04 §3,
/// spec t01 §3.4): фоновая Task процесса приёмника, параллельная стриму.
/// Пайплайн: резолв кредов/пула из etcd → роль (REST-проба; write-список
/// ФИКСИРУЕТСЯ на старте) → попытки по endpoint'ам write-списка: multipart-
/// сессия ОДНОГО объекта `full/&lt;id&gt;.tar` → BASE_BACKUP (свой wire-клиент) →
/// перепаковка входного tar-потока ЕДИНЫМ tar-writer'ом прямо в части
/// multipart (диска не касаемся) → Complete → фаза uploading: IDENTIFY_SYSTEM
/// и дубль закрытых сегментов набора в `wal/` (Range-GET из своего объекта) →
/// succeeded с `wal_start_segment`, `size_bytes`, `sha256`, прогрессом.
/// Резюмов нет: любой отказ → failed с однострочной причиной, переснятие —
/// новым id. Статус — IMMUTABLE-снимки в OperationRegistry; etcd-контракта
/// операция не знает (пишет только воркер, arch/19 §4).</summary>
public sealed class FullBackupOperation(
    string id,
    PgNodeIdentity identity,
    IEtcdGateway etcd,
    string[] etcdEndpoints,
    IS3PoolStore pools,
    Func<CancellationToken, Task<WalReceiverRole>> roleProbe,
    OperationRegistry registry,
    Func<string, IBackupS3>? targetFactory = null,              // тестовый шов (endpoint → клиент)
    Func<WalReceiverOptions, IXLogReplicationSource>? sourceFactory = null, // IDENTIFY_SYSTEM шов
    Func<string, string, IBaseBackupSource>? wireFactory = null) // (host, password) → BASE_BACKUP-источник
    : IReceiverOperation
{
    // DSN-параметры BASE_BACKUP — как у стримного подключения приёмника
    // (WalReceiverOptions: 127.0.0.1:5432, роль backup_exec, БД postgres).
    private const string PgHost = "127.0.0.1";
    private const int PgPort = 5432;

    // Параллелизм дубля закрытых сегментов набора в wal/ (как у прежней
    // заливки набора; порядок put не значим).
    private const int DuplicateParallelism = 8;

    // Строка прогресса в журнал ноды — каждые 64 МиБ потока.
    private const long ProgressLogEveryBytes = 64L * 1024 * 1024;

    // Потолок объекта: 10000 частей × 16 МиБ (лимиты OwnS3, spec §5);
    // превышение — FAILED с диагностикой (набор не влезает в multipart).
    private const long MaxObjectBytes = 10_000L * 16 * 1024 * 1024;

    // Имя записи манифеста в едином tar (verify читает его хвостом объекта).
    private const string ManifestEntryName = "backup_manifest";

    private readonly CancellationTokenSource _cts = new();

    // Состояние текущей попытки (BASE_BACKUP → единый tar → части).
    private TarStreamWriter? _tar;
    private MultipartPartBufferStream? _parts;
    private byte[]? _manifest;
    private string? _walStart;
    private readonly List<(string Name, long Offset, long Size)> _walEntries = [];
    private long _progressBucket = -1;

    // Итог успешной попытки.
    private string? _sha256;
    private long _sizeBytes;

    /// <summary>Текущий снимок операции (чтение реестра).</summary>
    public OperationSnapshot? Snapshot => registry.Current;

    public async Task RunAsync(CancellationToken ct)
    {
        // Связка отмен cancel API и хоста: любой из них гасит операцию
        // (сокет BASE_BACKUP и multipart-сессия закрываются обработчиками).
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
        var token = linked.Token;
        string? caBundlePath = null;
        IBackupS3? uploadedClient = null;
        try
        {
            // 1. Резолв кредов/пула из etcd (как у стрима): ожидания-условия
            //    (нет пароля/policy) — честный transient-отказ операции.
            var targets = await new WalReceiverTargets(etcd, etcdEndpoints, pools)
                .ResolveAsync(identity.Cluster, identity.Shard, token);
            if (targets.WaitingPassword || targets.WaitingPolicy)
            {
                Fail(targets.Error is { Length: > 0 } e
                    ? e
                    : "backup password/policy is not resolved");
                return;
            }

            // Bucket per-install: пусто — инконсистентность инсталляции.
            if (identity.S3Bucket is not { Length: > 0 } bucket)
            {
                Fail("env PGW_BACKUP_S3_BUCKET is not set — backups disabled on this install");
                return;
            }

            // Роль + write-список: ФИКСАЦИЯ на старте операции (смена роли
            // посреди полного её не рвёт).
            var role = await roleProbe(token);
            var pool = targets.Pool!;
            var writeTargets = role == WalReceiverRole.Master
                ? pool.WriteMaster
                : pool.WriteReplica;
            Prog($"basebackup start (role={role.ToString().ToLowerInvariant()})");

            // CA https S3 (per-install, env ноды): материализация в temp —
            // один раз на операцию (образец WalReceiverLoop), 0600.
            caBundlePath = await MaterializeCaAsync();

            // 2. Попытки по endpoint'ам write-списка: полный рестарт чтения на
            //    каждой (источник не перематывается); сессия — на ОДНОМ endpoint.
            var objectKey = $"{identity.Cluster}/{identity.Shard}/full/{id}.tar";
            var errors = new List<string>();
            (long Bytes, long Parts)? progress = null;
            var uploaded = false;
            foreach (var endpoint in writeTargets)
            {
                token.ThrowIfCancellationRequested();
                var client = CreateClient(endpoint, bucket, pool, caBundlePath);
                var keepClient = false;
                try
                {
                    var created = await ((IMultipartS3)client)
                        .CreateMultipartUploadAsync(objectKey, token);
                    if (!created.IsSuccess)
                    {
                        errors.Add(OneLine(created.Error!.Message));
                        continue;
                    }

                    var parts = new MultipartPartBufferStream(created.Value);
                    var tar = new TarStreamWriter(parts);
                    BeginAttempt(tar, parts);
                    registry.Update(new OperationSnapshot(
                        id, OperationStates.Running, OperationPhases.Basebackup));
                    try
                    {
                        await using var source = await OpenSourceAsync(targets.Password!, token);
                        await new BaseBackupCommand(source).RunAsync(id, CopyStreamAsync, token);
                        if (_walStart is null)
                        {
                            errors.Add("backup_label: START WAL LOCATION not found");
                            await parts.AbortAsync();
                            continue;
                        }

                        // Манифест — ПОСЛЕДНЕЙ записью единого tar (verify
                        // читает его хвостом объекта, spec §6.7).
                        if (_manifest is { } manifest)
                        {
                            await tar.WriteEntryAsync(
                                new TarEntry(
                                    ManifestEntryName, TarTypeFlags.File, manifest.Length, 0x1A4, ""),
                                manifest, token);
                        }

                        await tar.FinishAsync(token);
                        var completed = await parts.CompleteAsync(token);
                        if (!completed.IsSuccess)
                        {
                            errors.Add(OneLine(completed.Error!.Message));
                            await parts.AbortAsync();
                            continue;
                        }

                        _sha256 = Convert.ToHexString(tar.Sha256Hash()).ToLowerInvariant();
                        _sizeBytes = tar.TotalBytes;
                        progress = (parts.BytesDone, parts.PartsDone);
                        uploaded = true;
                        // Объект залит на ЭТОТ endpoint: дубль WAL читает его же.
                        uploadedClient = client;
                        keepClient = true;
                        break;
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        // Отмена гасит и BASE_BACKUP-соединение, и multipart-
                        // сессию; цикл попыток прерывается (отмена — не отказ
                        // endpoint'а; деградация в s3-pool-exhausted недопустима).
                        await parts.AbortAsync();
                        throw;
                    }
                    catch (Exception e)
                    {
                        errors.Add(OneLine($"{e.GetType().Name}: {e.Message}"));
                        await parts.AbortAsync();
                        continue;
                    }
                    finally
                    {
                        tar.Dispose();
                        _tar = null;
                        _parts = null;
                    }
                }
                finally
                {
                    if (!keepClient && client is IAsyncDisposable disposable)
                        await disposable.DisposeAsync();
                }
            }

            if (!uploaded)
            {
                Fail($"{S3PoolMarkers.Exhausted}: " +
                     $"{errors.FirstOrDefault() ?? "write target list is empty"}");
                return;
            }

            Prog($"basebackup done: size={_sizeBytes} bytes sha256={_sha256}");

            // 3. Фаза uploading: снимок + IDENTIFY_SYSTEM (позиция источника)
            //    + дубль закрытых сегментов набора в общий wal/.
            registry.Update(new OperationSnapshot(
                id, OperationStates.Uploading, OperationPhases.Uploading,
                WalStartSegment: _walStart));

            var identifyOptions = new WalReceiverOptions(
                PgHost, PgPort, "backup_exec", targets.Password!, "postgres",
                targets.Slot!, identity.Cluster, identity.Shard,
                writeTargets, pool.ReadAll, bucket, pool.AccessKey, pool.SecretKey);
            var identifySource = sourceFactory is { } factory
                ? factory(identifyOptions)
                : new NpgsqlXLogSource(identifyOptions);
            await using (identifySource)
            {
                var opened = await identifySource.OpenAsync(token);
                if (!opened.IsSuccess)
                {
                    Fail($"IDENTIFY_SYSTEM open failed: {OneLine(opened.Error!.Message)}");
                    return;
                }

                var identified = await identifySource.IdentifySystemAsync(token);
                if (!identified.IsSuccess)
                {
                    Fail($"IDENTIFY_SYSTEM failed: {OneLine(identified.Error!.Message)}");
                    return;
                }

                var activeSegId = identified.Value.XLogPos / (ulong)WalFileName.SegmentBytes;
                var duplicateError = await DuplicateWalAsync(
                    (IStreamObjectS3)uploadedClient!, uploadedClient as IWalSegmentGuard,
                    activeSegId, token);
                if (duplicateError is not null)
                {
                    Fail(duplicateError);
                    return;
                }
            }

            // 4. Итог succeeded: wal_start, размер объекта, сквозной sha256,
            //    финализированный прогресс (терминальные снимки — без потерь).
            registry.Update(new OperationSnapshot(
                id, OperationStates.Succeeded, OperationPhases.Uploading,
                WalStartSegment: _walStart,
                SizeBytes: _sizeBytes,
                Sha256: _sha256,
                BytesDone: progress?.Bytes,
                PartsDone: progress?.Parts));
            Prog($"COMPLETED: wal_start={_walStart} size={_sizeBytes} bytes sha256={_sha256}");
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
            if (caBundlePath is not null)
                try { File.Delete(caBundlePath); } catch { /* уже нет */ }
            if (uploadedClient is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
        }
    }

    /// <summary>Best-effort отмена (cancel API): гасит внутренний CTS — чтение
    /// сокета и заливка частей прерываются; операция завершится failed
    /// `cancelled` (multipart-сессия гасится Abort'ом в обработчике).</summary>
    public Task CancelAsync()
    {
        _cts.Cancel();
        return Task.CompletedTask;
    }

    /// <summary>Перепаковка одной секции COPY-потока BASE_BACKUP в единый tar:
    /// index 0 — base.tar (WAL-сегменты уже внутри с префиксом pg_wal/),
    /// index 1 — отдельный WAL-архив (защита для серверов без WAL-in-tar),
    /// <see cref="BaseBackupCommand.ManifestIndex"/> — манифест (сырые байты,
    /// пишется последней записью).</summary>
    private async Task CopyStreamAsync(int index, Stream stream, CancellationToken ct)
    {
        var tar = _tar ?? throw new InvalidOperationException("tar writer is not initialized");
        var parts = _parts ?? throw new InvalidOperationException("multipart stream is not initialized");

        if (index == BaseBackupCommand.ManifestIndex)
        {
            using var manifest = new MemoryStream();
            await stream.CopyToAsync(manifest, ct);
            _manifest = manifest.ToArray();
            Prog($"manifest received: {_manifest.Length} bytes");
            return;
        }

        var reader = new TarStreamReader(stream);
        while (await reader.ReadHeaderAsync(ct) is { } entry)
        {
            // Отдельный WAL-архив сервера — записи с префиксом pg_wal/.
            var name = index == 0 ? entry.Name : "pg_wal/" + entry.Name;
            if (name == "backup_label")
            {
                var label = await reader.ReadAllDataAsync(ct);
                _walStart = BackupLabel.WalStartSegment(Encoding.ASCII.GetString(label));
                await tar.WriteEntryAsync(entry with { Name = name }, label, ct);
            }
            else if (entry.TypeFlag is TarTypeFlags.Dir or TarTypeFlags.Symlink)
            {
                // Каталоги/симлинки — только заголовок (данных нет).
                await tar.WriteEntryAsync(entry with { Name = name }, Stream.Null, ct);
            }
            else
            {
                // Смещение ДАННЫХ записи в выходном tar — фактическое, после всех
                // заголовков (служебные longname/PAX тоже сдвигают данные): из
                // писателя, не «Position + 512» (Range-GET дубля).
                var dataOffset = await tar.WriteEntryWithDataOffsetAsync(
                    entry with { Name = name }, reader.OpenData(), ct);
                if (name.StartsWith("pg_wal/", StringComparison.Ordinal))
                    _walEntries.Add((name, dataOffset, entry.Size));
            }

            if (tar.TotalBytes > MaxObjectBytes)
                throw new ApplicationException("backup exceeds 160 GiB multipart limit");
            PublishProgress(tar.TotalBytes, parts.PartsDone);
            var bucket = tar.TotalBytes / ProgressLogEveryBytes;
            if (bucket != _progressBucket)
            {
                _progressBucket = bucket;
                Prog($"upload progress: bytes={tar.TotalBytes} parts={parts.PartsDone}");
            }
        }
    }

    // Дубль закрытых сегментов набора в wal/ (arch/pgnode/04 §3): Range-GET
    // байтов сегмента из СВОЕГО tar-объекта → гвард чужой цепочки; последний
    // живой сегмент набора не дублируется. Возврат — первая ошибка или null.
    private async Task<string?> DuplicateWalAsync(
        IStreamObjectS3 reader, IWalSegmentGuard? guard, ulong activeSegId, CancellationToken ct)
    {
        if (guard is null)
            return "wal duplicate failed: S3 client does not implement the WAL chain guard";
        string? error = null;
        using var gate = new SemaphoreSlim(DuplicateParallelism);
        await Task.WhenAll(_walEntries.Select(async entry =>
        {
            var fileName = entry.Name[(entry.Name.LastIndexOf('/') + 1)..];
            var normalized = FetchWalCommand.NormalizeSegment(fileName);
            if (WalFileName.TryParse(normalized) is not { } segment)
                return; // не сегмент (archive_status/*.done, *.history) — пропуск
            var setPos = (long)segment.Log * WalFileName.SegsPerLog + segment.Seg;
            if (setPos >= (long)activeSegId)
            {
                Prog($"wal duplicate skipped: {normalized} is not closed in the source yet");
                return;
            }

            await gate.WaitAsync(ct);
            try
            {
                if (Volatile.Read(ref error) is not null)
                    return; // первая ошибка — остальные Range/put не начинаем
                var range = await reader.GetObjectRangeAsync(
                    identity.Cluster, identity.Shard, $"full/{id}.tar",
                    entry.Offset, entry.Offset + entry.Size - 1, ct);
                if (!range.IsSuccess)
                {
                    Interlocked.CompareExchange(ref error, OneLine(range.Error!.Message), null);
                    return;
                }

                var put = await guard.PutWalSegmentAsync(
                    $"{identity.Cluster}/{identity.Shard}/wal/{normalized}", range.Value, null, ct);
                if (!put.IsSuccess)
                    Interlocked.CompareExchange(ref error, OneLine(put.Error!.Message), null);
            }
            finally
            {
                gate.Release();
            }
        }));
        return error;
    }

    private void BeginAttempt(TarStreamWriter tar, MultipartPartBufferStream parts)
    {
        _tar = tar;
        _parts = parts;
        _manifest = null;
        _walStart = null;
        _progressBucket = -1;
        _walEntries.Clear();
    }

    // Прогресс в снимке (дешёвая замена ссылки): bytes_done/parts_done при
    // живом прогрессе — штатная частота обновления реестра.
    private void PublishProgress(long bytesDone, long partsDone)
    {
        var current = registry.Current;
        if (current is not { State: OperationStates.Running })
            return;
        registry.Update(current with { BytesDone = bytesDone, PartsDone = partsDone });
    }

    private IBackupS3 CreateClient(
        string endpoint, string bucket, BackupPoolContext pool, string? caBundlePath)
        => targetFactory is { } factory
            ? factory(endpoint)
            // MaxErrorRetry: 0 — своя политика попыток (перебор endpoint'ов).
            : new BackupS3(
                new S3ClientOptions(endpoint, bucket, pool.AccessKey, pool.SecretKey,
                    CaBundlePath: caBundlePath, MaxErrorRetry: 0),
                WalReceiverCore.FileAttemptTimeout);

    // Источник BASE_BACKUP: прод — свой wire-клиент (подключение+SCRAM), тест —
    // готовый фейк через wireFactory.
    private async Task<IBaseBackupSource> OpenSourceAsync(string password, CancellationToken ct)
    {
        if (wireFactory is { } factory)
            return factory(PgHost, password);
        var client = new PgWireReplicationClient(PgHost, PgPort, "backup_exec", password);
        await client.ConnectAsync(ct);
        return client;
    }

    // CA https S3 в temp-файл (образец WalReceiverLoop): 0600, путь — в S3-клиент.
    private async Task<string?> MaterializeCaAsync()
    {
        if (identity.S3CaPem is not { Length: > 0 } caPem)
            return null;
        var path = Path.Combine(Path.GetTempPath(), $"s3-ca-{Guid.NewGuid():N}.pem");
        var options = OperatingSystem.IsWindows()
            ? new FileStreamOptions { Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.None }
            : new FileStreamOptions
            {
                Mode = FileMode.Create,
                Access = FileAccess.Write,
                Share = FileShare.None,
                UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite,
            };
        await using (var stream = new FileStream(path, options))
        using (var writer = new StreamWriter(stream))
        {
            await writer.WriteAsync(caPem + "\n");
        }

        return path;
    }

    private void Prog(string message)
        => Console.Error.WriteLine($"walreceiver: full {id} {message}");

    /// <summary>Публикация терминального failed-снимка (фаза — текущая);
    /// причина — в stdout контейнера (диагностика: etcd-статус живёт до
    /// переснятия, журнал остаётся после).</summary>
    private void Fail(string error)
    {
        var phase = registry.Current?.Phase ?? OperationPhases.Basebackup;
        registry.Update(new OperationSnapshot(
            id, OperationStates.Failed, phase, Error: error));
        Console.Error.WriteLine(
            $"walreceiver: full {id} FAILED at {phase}: {error}");
    }

    private static string OneLine(string text)
        => text.Split('\n')[0];
}
