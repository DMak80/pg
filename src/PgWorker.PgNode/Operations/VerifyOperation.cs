using System.Security.Cryptography;
using System.Text;
using PgWorker.Backups;
using PgWorker.Backups.Tar;
using PgWorker.PgNode.BaseBackup;

namespace PgWorker.PgNode.Operations;

/// <summary>Verify-операция процесса приёмника ноды (spec t01 §3.5, arch/pgnode/05):
/// одна активная ops-операция на процесс (общий OpsOperationRegistry; полный —
/// свой реестр и не мешает). Фаза downloading — добывание манифеста: фиксация
/// endpoint'а чтения (HEAD до первого найденного — даёт и размер объекта),
/// Range-чтение хвоста tar-объекта (tail = 8 МиБ, не найден — удвоение, ≤4
/// попыток; объект меньше tail — диапазон от 0), разбор backup_manifest.
/// Транспортный сбой — failed/downloading (МАРКЕР ТРАНЗИЕНТА: воркер держит
/// PENDING и ретраит тиком). Фаза verifying — единственный проход
/// GetObjectStreamAsync: sha256 каждой tar-записи на лету против манифеста
/// (размер/hex), лишние/отсутствующие файлы — failed/verifying (PERMANENT —
/// данные плохие); сквозной sha256 объекта — против etcd-поля `sha256`
/// (задано у наших полных; у орфанов может отсутствовать). Операторская
/// отмена — state=cancelled (НЕ failed). Распаковки на диск нет.</summary>
public sealed class VerifyOperation : IReceiverOperation
{
    // Хвост объекта под манифест: 8 МиБ покрывает ~50K файлов, удвоение
    // ≤4 попыток — для очень больших наборов (spec §6.7).
    private const long InitialTailBytes = 8L << 20;
    private const int MaxTailAttempts = 4;

    // Записи, которых манифест заведомо не содержит: он сам и WAL-сегменты
    // набора (WAL покрыт WAL-Ranges, а не пофайловыми checksum'ами).
    private const string ManifestEntryName = "backup_manifest";
    private const string WalPrefix = "pg_wal/";

    private readonly StartVerifyRequest _request;
    private readonly OperationRegistry _registry;
    private readonly Func<string, IBackupS3>? _targetFactory; // тестовый шов (endpoint → клиент)
    private readonly CancellationTokenSource _cts = new();
    private volatile bool _shutdown; // гашение хоста ≠ операторская отмена

    public VerifyOperation(
        StartVerifyRequest request,
        OperationRegistry registry,
        Func<string, IBackupS3>? targetFactory = null)
    {
        _request = request;
        _registry = registry;
        _targetFactory = targetFactory;
    }

    public OperationSnapshot? Snapshot => _registry.Current;

    public async Task RunAsync(CancellationToken ct)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
        var token = linked.Token;

        // prefix команды — <C>/<X>: cluster/shard — разбор по первому '/'.
        var prefix = _request.S3.Prefix.Trim('/');
        var slash = prefix.IndexOf('/');
        var cluster = slash > 0 ? prefix[..slash] : prefix;
        var shard = slash > 0 ? prefix[(slash + 1)..] : string.Empty;
        var objectKey = $"full/{_request.BackupId}.tar";
        IBackupS3? client = null;
        string? caBundlePath = null;
        try
        {
            // CA https (PEM в POST-команде): материализация в temp, 0600.
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

            // Фаза downloading: фиксация endpoint'а (HEAD до первого найденного)
            // + манифест из хвоста объекта.
            Update(OperationStates.Running, OperationPhases.Downloading);
            long size = 0;
            IStreamObjectS3? streamer = null;
            foreach (var endpoint in _request.S3.Endpoints)
            {
                var candidate = CreateClient(endpoint, caBundlePath);
                var head = await ((IStreamObjectS3)candidate).HeadObjectAsync(
                    cluster, shard, objectKey, token);
                if (head.IsSuccess && head.Value.Exists)
                {
                    size = head.Value.SizeBytes;
                    streamer = (IStreamObjectS3)candidate;
                    client = candidate;
                    break;
                }

                if (candidate is IAsyncDisposable disposable)
                    await disposable.DisposeAsync();
            }

            if (streamer is null)
            {
                // Отсутствие объекта у COMPLETE-полного — битые данные, но фазу
                // оставляем downloading/transient (воркер ретраит; вечный цикл
                // исключён VerifyTimeoutSec-бюджетом супервизора, spec §6.7).
                Fail(OperationPhases.Downloading, $"{objectKey}: not found");
                return;
            }

            if (size <= 0)
            {
                // Пустой объект у COMPLETED-полного — вердикт о данных
                // (permanent, arch/pgnode/05 §2). Проверка ДО Range-чтения
                // хвоста: диапазон [0, -1] сервер отвергает 416, и ошибка
                // ушла бы в transient-ветку вместо вердикта.
                Fail(OperationPhases.Verifying, $"{objectKey}: object is empty");
                return;
            }

            var manifestJson = await ReadManifestAsync(
                streamer, cluster, shard, objectKey, size, token);

            if (manifestJson is null)
            {
                // Исчерпание попыток хвоста при СУЩЕСТВУЮЩЕМ объекте —
                // permanent (arch/pgnode/05 §2): данные плохие; иначе воркер
                // ретраил бы вечно (квоты на transient-цикл нет).
                Fail(OperationPhases.Verifying,
                    "backup_manifest not found in the object tail after all attempts");
                return;
            }

            IReadOnlyDictionary<string, ManifestFile> files;
            IReadOnlyList<ManifestWalRange> walRanges;
            try
            {
                files = BackupManifest.ParseFiles(manifestJson);
                walRanges = BackupManifest.ParseWalRanges(manifestJson);
            }
            catch (System.Text.Json.JsonException e)
            {
                Fail(OperationPhases.Verifying, OpText.OneLine(e.Message));
                return;
            }

            if (BackupManifest.VerifyChecksum(manifestJson) is false)
            {
                Fail(OperationPhases.Verifying, "backup manifest checksum mismatch");
                return;
            }

            // Фаза verifying: один проход потока — sha256 записей + сквозной
            // sha256 объекта.
            Update(OperationStates.Running, OperationPhases.Verifying);
            var opened = await streamer.GetObjectStreamAsync(cluster, shard, objectKey, token);
            if (!opened.IsSuccess)
            {
                Fail(OperationPhases.Downloading, OpText.OneLine(opened.Error!.Message));
                return;
            }

            var (body, _) = opened.Value;
            using var objectHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            var walSegments = new HashSet<string>(StringComparer.Ordinal);
            await using (var hashing = new HashingReadStream(body, objectHash))
            {
                var error = await VerifyPassAsync(hashing, files, walSegments, token);
                if (error is not null)
                {
                    Fail(OperationPhases.Verifying, error);
                    return;
                }

                // Сквозной хэш — по ВСЕМУ объекту: tar-reader останавливается
                // на первом нулевом заголовке, второй блок добивки (1024 нуля
                // пишет writer) остаётся непрочитанным — дочитываем до EOF.
                var drain = new byte[64 * 1024];
                while (await hashing.ReadAsync(drain, token) > 0)
                {
                    // Хвост объекта уже уходит в сквозной хэш обёрткой.
                }
            }

            // WAL-Ranges: сегменты, заявленные диапазонами манифеста, обязаны
            // лежать в tar (pg_wal/<segment>) — перечень, не побайтовая сверка
            // (arch/pgnode/05 §2).
            var walError = CheckWalRanges(walRanges, walSegments);
            if (walError is not null)
            {
                Fail(OperationPhases.Verifying, walError);
                return;
            }

            var objectSha = Convert.ToHexString(objectHash.GetHashAndReset()).ToLowerInvariant();
            if (_request.Sha256 is { Length: > 0 } expected
                && !string.Equals(expected.ToLowerInvariant(), objectSha, StringComparison.Ordinal))
            {
                Fail(OperationPhases.Verifying,
                    $"object sha256 mismatch: expected {expected.ToLowerInvariant()}, got {objectSha}");
                return;
            }

            Update(OperationStates.Succeeded, OperationPhases.Verifying);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Операторская отмена: снимок cancelled — НЕ failed (отмена не
            // суждение о данных); воркер вернёт кандидата в PENDING.
            // Гашение при остановке хоста (_shutdown) снимок НЕ перезаписывает.
            if (!_shutdown)
                Update(OperationStates.Cancelled, CurrentPhase(), "cancelled by operator");
        }
        catch (Exception e)
        {
            // Транспортный сбой чтения на любой фазе — transient (маркер
            // downloading, arch/pgnode/05 §2): permanent — только вердикт о
            // данных (формат/хэши/перечень), он ставится выше явно.
            var phase = IsTransport(e) ? OperationPhases.Downloading : CurrentPhase();
            Fail(phase, $"{e.GetType().Name}: {e.Message.Split('\n')[0]}");
        }
        finally
        {
            if (caBundlePath is not null)
                try { File.Delete(caBundlePath); } catch { /* уже нет */ }
            if (client is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
        }
    }

    /// <summary>Best-effort отмена (cancel API): гасит внутренний CTS — чтение
    /// потока/сокета прерывается.</summary>
    public Task CancelAsync()
    {
        _cts.Cancel();
        return Task.CompletedTask;
    }

    /// <summary>Гашение при остановке хоста приёмника (arch/pgnode/
    /// 06-restore-drill.md §2): прерывает чтение потока/сокета; снимок НЕ
    /// перезаписывается cancelled — операторская семантика только у
    /// cancel API.</summary>
    public Task ShutdownAsync()
    {
        _shutdown = true;
        _cts.Cancel();
        return Task.CompletedTask;
    }

    // Манифест из хвоста объекта: Range [max(0, size−tail), size−1]; не найден
    // — удвоение tail (≤4 попыток); объект меньше tail — диапазон от 0.
    private async Task<string?> ReadManifestAsync(
        IStreamObjectS3 reader, string cluster, string shard, string key, long size,
        CancellationToken ct)
    {
        var tail = InitialTailBytes;
        for (var attempt = 1; attempt <= MaxTailAttempts; attempt++)
        {
            var start = Math.Max(0, size - tail);
            var range = await reader.GetObjectRangeAsync(cluster, shard, key, start, size - 1, ct);
            if (!range.IsSuccess)
                throw new ApplicationException(OpText.OneLine(range.Error!.Message));
            if (TryFindManifest(range.Value, start, out var manifest))
                return manifest;
            tail <<= 1;
        }

        return null;
    }

    // Поиск записи backup_manifest в буфере хвоста: сканирование по границам
    // 512 от выровненного в объекте смещения; запись должна лежать в буфере
    // целиком (иначе — недобранный хвост, попытка с большим tail).
    private static bool TryFindManifest(byte[] data, long rangeStart, out string? manifestJson)
    {
        manifestJson = null;
        var firstHeader = rangeStart % 512 == 0
            ? rangeStart
            : rangeStart + (512 - rangeStart % 512);
        for (var position = firstHeader; position + 512 <= rangeStart + data.Length; position += 512)
        {
            var offset = (int)(position - rangeStart);
            var entry = TarStreamReader.TryParseHeaderAt(data, offset);
            if (entry is null || entry.Name != ManifestEntryName || entry.TypeFlag != TarTypeFlags.File)
                continue;
            if (entry.Size < 0 || entry.Size > data.Length - offset - 512)
                return false; // данных записи в буфере нет — нужен больший tail
            manifestJson = Encoding.UTF8.GetString(
                data, offset + 512, (int)entry.Size).TrimEnd('\0');
            return true;
        }

        return false;
    }

    // Проход по tar-объекту: sha256 каждой записи на лету, сверка с манифестом;
    // каталоги/symlink'и и записи, которых манифест не содержит по конструкции
    // (сам манифест, WAL-сегменты), не сверяются; WAL-сегменты собираются для
    // сверки WAL-Ranges. Возврат — первая ошибка.
    private static async Task<string?> VerifyPassAsync(
        Stream body, IReadOnlyDictionary<string, ManifestFile> files,
        HashSet<string> walSegments, CancellationToken ct)
    {
        var reader = new TarStreamReader(body);
        var verified = new HashSet<string>(StringComparer.Ordinal);
        while (await reader.ReadHeaderAsync(ct) is { } entry)
        {
            if (entry.TypeFlag is not (TarTypeFlags.File or TarTypeFlags.LegacyFile))
            {
                await reader.SkipDataAsync(ct);
                continue;
            }

            if (entry.Name == ManifestEntryName
                || (entry.Name.StartsWith(WalPrefix, StringComparison.Ordinal)
                    && IsWalSegment(entry.Name[WalPrefix.Length..])))
            {
                if (entry.Name.StartsWith(WalPrefix, StringComparison.Ordinal))
                {
                    // Сервер кладёт сегменты набора ВЕРХНИМ hex — сравнение с
                    // WAL-Ranges (строчные имена) после нормализации регистра.
                    walSegments.Add(entry.Name[WalPrefix.Length..].ToLowerInvariant());
                }
                await reader.SkipDataAsync(ct);
                continue;
            }

            if (!files.TryGetValue(entry.Name, out var expected))
                return $"unexpected file {entry.Name}";

            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            var buffer = new byte[64 * 1024];
            long read = 0;
            await using (var data = reader.OpenData())
            {
                int count;
                while ((count = await data.ReadAsync(buffer, ct)) > 0)
                {
                    hash.AppendData(buffer, 0, count);
                    read += count;
                }
            }

            var actual = Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
            if (read != expected.Size || actual != expected.Sha256Hex)
                return $"checksum mismatch {entry.Name}";
            verified.Add(entry.Name);
        }

        foreach (var path in files.Keys)
        {
            if (!verified.Contains(path))
                return $"missing file {path}";
        }

        return null;
    }

    private static bool IsWalSegment(string name)
        => name.Length == 24 && name.All(Uri.IsHexDigit);

    // Сверка присутствия сегментов WAL-Ranges в tar: диапазон — непрерывный
    // ряд сегментов от Start-LSN до End-LSN включительно (Timeline — из
    // диапазона). Отсутствие — permanent-вердикт (перечень неполон).
    private static string? CheckWalRanges(
        IReadOnlyList<ManifestWalRange> ranges, HashSet<string> present)
    {
        foreach (var range in ranges)
        {
            WalFileName start;
            WalFileName end;
            try
            {
                start = WalFileName.FromLsn(range.Timeline, range.StartLsn);
                end = WalFileName.FromLsn(range.Timeline, range.EndLsn);
            }
            catch (Exception e) when (e is FormatException or OverflowException
                                          or IndexOutOfRangeException)
            {
                return $"invalid WAL-Ranges entry ({range.Timeline}, {range.StartLsn}, {range.EndLsn})";
            }

            if (start.DistanceTo(end) < 0)
                return $"invalid WAL-Ranges entry: end {end.Name} precedes start {start.Name}";

            // Потолок обхода — лимит частей объекта (патологический диапазон
            // не должен вешать verify); реальный ряд ограничен содержимым tar.
            if (start.DistanceTo(end) > 10_000)
                return "WAL-Ranges span is too large to verify";
            for (var segment = start; ; segment = segment.Next())
            {
                if (!present.Contains(segment.Name))
                    return $"WAL segment {segment.Name} declared by WAL-Ranges is missing (pg_wal/{segment.Name})";
                if (segment.Equals(end))
                    break;
            }
        }

        return null;
    }

    // Транспортные исключения чтения S3/сети: transient (маркер downloading).
    // Таймаут HttpClient приходит как TaskCanceledException (подкласс OCE) —
    // сюда же; операторская отмена распознаётся отдельно по токену.
    private static bool IsTransport(Exception e)
        => e is IOException
            or System.Net.Http.HttpRequestException
            or System.Net.Sockets.SocketException
            or Amazon.S3.AmazonS3Exception
            or TaskCanceledException or TimeoutException;

    private IBackupS3 CreateClient(string endpoint, string? caBundlePath)
        => _targetFactory is { } factory
            ? factory(endpoint)
            : new BackupS3(
                new S3ClientOptions(
                    endpoint, _request.S3.Bucket, _request.S3.AccessKey, _request.S3.SecretKey,
                    CaBundlePath: caBundlePath, MaxErrorRetry: 0),
                WalReceiverCore.FileAttemptTimeout);

    // Обновление снимка с переносом started_unix TryBegin (возрастной бюджет
    // verify воркера, arch/backups/09-operability.md): Update заменяет снимок целиком.
    private void Update(string state, string phase, string? error = null)
        => _registry.Update(new OperationSnapshot(
            _request.BackupId, state, phase,
            StartedUnix: _registry.Current?.StartedUnix, Error: error,
            Kind: OperationKinds.Verify));

    private string CurrentPhase()
        => _registry.Current?.Phase ?? OperationPhases.Downloading;

    private void Fail(string phase, string error)
        => _registry.Update(new OperationSnapshot(
            _request.BackupId, OperationStates.Failed, phase,
            StartedUnix: _registry.Current?.StartedUnix, Error: error,
            Kind: OperationKinds.Verify));
}
