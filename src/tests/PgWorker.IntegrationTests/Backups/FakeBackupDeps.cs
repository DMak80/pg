using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using PgWorker.Backups;
using PgWorker.Backups.Sql;
using PgWorker.Core;

namespace PgWorker.IntegrationTests.Backups;

// Фейковый SQL-слой: слоты в памяти per-инстансу (: ключ — adminDsn источника,
// слоты разных нод независимы), LSN управляется тестом (AAA-Act). LostByDsn —
// подмножество слотов в wal_status='lost' ; Calls — журнал мутаций
// (create/drop) для ассертов идемпотентности.
public sealed class FakeWalSqlExecutor : IWalSqlExecutor
{
    public ConcurrentDictionary<string, HashSet<string>> SlotsByDsn { get; } = new();

    // Слоты в статусе lost (подмножество SlotsByDsn того же DSN).
    public ConcurrentDictionary<string, HashSet<string>> LostByDsn { get; } = new();

    // Журнал мутаций слота: ("create"|"drop", dsn, slot) — для ассертов
    // «нулевых мутаций» и «drop+create».
    public List<(string Op, string Dsn, string Slot)> Calls { get; } = [];

    public (string Lsn, int Tli) Current { get; set; } = ("0/1000000", 1);

    public Task<Result<(bool Exists, string? WalStatus)>> SlotProbeAsync(string adminDsn, string slot, CancellationToken ct)
        => Task.FromResult(Result<(bool, string?)>.Success(
            SlotsByDsn.TryGetValue(adminDsn, out var slots) && slots.Contains(slot)
                ? (true, IsLost(adminDsn, slot) ? "lost" : "reserved")
                : (false, null)));

    public Task<Result> EnsureSlotAliveAsync(string adminDsn, string slot, CancellationToken ct)
    {
        var exists = SlotsByDsn.TryGetValue(adminDsn, out var slots) && slots.Contains(slot);
        if (exists && !IsLost(adminDsn, slot))
            return Task.FromResult(Result.Success()); // жив — не трогать
        return RecreateSlotAsync(adminDsn, slot, ct); // отсутствует/lost → drop-допуск + create
    }

    public Task<Result> RecreateSlotAsync(string adminDsn, string slot, CancellationToken ct)
    {
        if (IsLost(adminDsn, slot))
        {
            Calls.Add(("drop", adminDsn, slot));
            LostByDsn.GetOrAdd(adminDsn, _ => []).Remove(slot);
        }

        Calls.Add(("create", adminDsn, slot));
        SlotsByDsn.GetOrAdd(adminDsn, _ => []).Add(slot);
        return Task.FromResult(Result.Success());
    }

    public Task<Result<(string Lsn, int Tli)>> CurrentWalAsync(string adminDsn, CancellationToken ct)
        => Task.FromResult(Result<(string, int)>.Success(Current));

    private bool IsLost(string adminDsn, string slot)
        => LostByDsn.TryGetValue(adminDsn, out var lost) && lost.Contains(slot);
}

// Фейковый S3 (поверхность IBackupS3): объекты в памяти, стартовое наполнение —
// тестом. : PrefixObjects — объекты произвольных префиксов (полные ключи),
// DeletedKeys — журнал удалений, FailNextDelete — сбой для transient-сценариев.
// PrefixedObjects — ключи относительно <C>/<X>/ (сид verify-наборов),
// Contents — содержимое для GetObjectAsync, Fails/FailsGetObject — транспорт-сбои.
// Тестовые tar-байты набора (реальный TarStreamWriter): backup_label ПЕРВОЙ
// записью (Range-чтение начала объекта — FullTarSet.ReadBackupLabelAsync),
// остальные записи — по потребности сценария.
public static class TestTarBytes
{
    public const string DefaultLabel =
        "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n" +
        "CHECKPOINT LOCATION: 0/2000028\nBACKUP METHOD: streamed\n";

    public static byte[] WithLabel(string label, params (string Name, byte[] Data)[] entries)
    {
        var buffer = new MemoryStream();
        using var writer = new PgWorker.Backups.Tar.TarStreamWriter(buffer);
        var labelBytes = System.Text.Encoding.UTF8.GetBytes(label);
        writer.WriteEntryAsync(
            new PgWorker.Backups.Tar.TarEntry(
                "backup_label", PgWorker.Backups.Tar.TarTypeFlags.File, labelBytes.Length, 0x1A0, ""),
            labelBytes, default).GetAwaiter().GetResult();
        foreach (var (name, data) in entries)
        {
            writer.WriteEntryAsync(
                new PgWorker.Backups.Tar.TarEntry(
                    name, PgWorker.Backups.Tar.TarTypeFlags.File, data.Length, 0x1A4, ""),
                data, default).GetAwaiter().GetResult();
        }

        writer.FinishAsync(default).GetAwaiter().GetResult();
        byte[] bytes = buffer.ToArray();
        return bytes;
    }

    // Сид объекта полного `full/<id>.tar` реальными tar-байтами (label —
    // первая запись): restore/drill читают его HEAD/Range/ReedStream.
    public static void SeedFullObject(
        FakeBackupS3 s3, string cluster, string shard, string id,
        string label = DefaultLabel,
        params (string Name, byte[] Data)[] entries)
        => s3.SeedTarObject(cluster, shard, $"full/{id}.tar", WithLabel(label, entries));
}

// Фейковый S3 (поверхность IBackupS3 + IStreamObjectS3 + IDeleteObjectS3 +
// IMultipartS3): объекты в памяти, стартовое наполнение — тестом. Полный —
// ОДИН объект `full/<id>.tar` (spec t01 §6.1): сиды — реальные tar-байты
// через TarStreamWriter (StreamObjects) либо размер-заглушка (PrefixObjects)
// для сценариев, где содержимое не читается. DeletedKeys — журнал удалений
// (абсолютные ключи), FailNextDelete — сбой batch/одиночного delete для
// transient-сценариев. MultipartUploads — список висячих сессий гигиены
// супервизора, AbortedUploads — журнал abort'ов, FailMultipartList — отказ
// листинга (транспорт). PrefixedObjects/Contents/Texts — совместимость
// старых сценариев (wal-имена, GET history, тексты).
public sealed class FakeBackupS3 : IBackupS3, IStreamObjectS3, IDeleteObjectS3, IMultipartS3
{
    public List<(string Cluster, string Shard, string Name)> Objects { get; } = [];

    // Объекты произвольных ключей (list/delete через ListPrefixAsync/DeleteKeysAsync).
    public List<(string Key, long SizeBytes)> PrefixObjects { get; } = [];

    public List<string> DeletedKeys { get; } = [];

    // Один сбой batch-delete/одиночного удаления (transient-сценарий AC3).
    public bool FailNextDelete { get; set; }

    // Объекты с ключом после <C>/<X>/ (голые wal-имена совместимости — Objects).
    public List<(string Cluster, string Shard, string Key)> PrefixedObjects { get; } = [];

    // Содержимое tar-объектов (реальные байты для потоковых restore/verify/
    // drill-сидов). Ключ — относительный (`full/<id>.tar`).
    public Dictionary<(string Cluster, string Shard, string Key), byte[]> StreamObjects { get; } = new();

    public Dictionary<(string Cluster, string Shard, string Key), string> Contents { get; } = new();

    public bool Fails { get; set; }           // транспорт-сбой чтений S3 (transient-тест list)
    public bool FailsGetObject { get; set; }  // падает только GET (transient-тест GET history)

    // полные шарда (id) — DR-поиск ListFullsAsync по fake-S3.
    public List<(string Cluster, string Shard, string Id)> Fulls { get; } = [];

    // тексты маленьких объектов по objectKey внутри префикса шарда.
    public Dictionary<string, string> Texts { get; } = new(StringComparer.Ordinal);

    // Один сбой list (transient-сценарий «S3 недоступен»): list не падает в статус.
    public bool FailList { get; set; }

    // Сбой list-prefix (сценарий «пул недоступен» для глобальных сверок).
    public bool FailListPrefix { get; set; }

    // Висячие multipart-сессии (гигиена супервизора, e-supervisor §1).
    public List<MultipartUploadInfo> MultipartUploads { get; } = [];
    public List<(string Key, string UploadId)> AbortedUploads { get; } = [];
    public bool FailMultipartList { get; set; }

    public DateTimeOffset LastModified { get; set; } = DateTimeOffset.UtcNow;

    // Сид tar-объекта реальными байтами (идемпотентно по ключу).
    public void SeedTarObject(string cluster, string shard, string key, byte[] data)
    {
        StreamObjects[(cluster, shard, key)] = data;
        if (!PrefixObjects.Any(o => o.Key == $"{cluster}/{shard}/{key}"))
            PrefixObjects.Add(($"{cluster}/{shard}/{key}", data.Length));
    }

    public Task<Result<bool>> BucketExistsAsync(CancellationToken ct)
        => Task.FromResult(Fails
            ? Result<bool>.Failed(new ApplicationException("s3 down"))
            : Result<bool>.Success(true));

    public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
        => ListAsync(cluster, shard, "wal/", maxKeysPerTest, ct);

    public Task<Result<IReadOnlyList<WalObject>>> ListAsync(
        string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        if (Fails)
            return Task.FromResult(Result<IReadOnlyList<WalObject>>.Failed(new ApplicationException("s3 down")));
        var prefixed = PrefixedObjects
            .Where(o => o.Cluster == cluster && o.Shard == shard
                        && o.Key.StartsWith(prefix, StringComparison.Ordinal))
            .Select(o => new WalObject(o.Key[(o.Key.LastIndexOf('/') + 1)..], LastModified));
        var walNames = prefix == "wal/"
            ? Objects.Where(o => o.Cluster == cluster && o.Shard == shard)
                .Select(o => new WalObject(o.Name, LastModified))
            : [];
        var merged = prefixed.Concat(walNames)
            .GroupBy(o => o.Name, StringComparer.Ordinal)
            .Select(g => g.First())
            .ToList();
        return Task.FromResult(Result<IReadOnlyList<WalObject>>.Success(
            (IReadOnlyList<WalObject>)merged));
    }

    public Task<Result> PutObjectMd5Async(string key, byte[] data, string md5Hex, CancellationToken ct = default)
        => throw new NotSupportedException();

    public Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(
        string cluster, string shard, string key, CancellationToken ct = default)
        => Task.FromResult(StreamObjects.TryGetValue((cluster, shard, key), out var bytes)
            ? Result<(byte[] Data, string? ETag)>.Success((bytes, null))
            : Result<(byte[] Data, string? ETag)>.Failed(
                new ApplicationException($"fake S3 get {cluster}/{shard}/{key}: not found")));

    // Файловое скачивание (fetch-wal) в фейке не используется — заглушка.
    public Task<Result> GetObjectToFileAsync(
        string cluster, string shard, string key, string destFilePath, CancellationToken ct = default)
        => Task.FromResult(Result.Failed(
            new ApplicationException("get-to-file: not supported by this fake")));

    // Прямое чтение текста (history-сценарии старой механики; в интерфейсе
    // IBackupS3 метода больше нет — фейк держит его для тестов).
    public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default)
        => Task.FromResult(Fails || FailsGetObject || !Contents.TryGetValue((cluster, shard, key), out var content)
            ? Result<string>.Failed(new ApplicationException("s3 get failed"))
            : Result<string>.Success(content));

    public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
        string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        if (FailListPrefix)
            return Task.FromResult(Result<IReadOnlyList<S3ObjectInfo>>.Failed(
                new ApplicationException("fake S3 list-prefix failure (FailListPrefix)")));
        var walKeys = Objects.Select(o =>
            new S3ObjectInfo($"{o.Cluster}/{o.Shard}/wal/{o.Name}", 16, LastModified));
        var prefixKeys = PrefixObjects.Select(o => new S3ObjectInfo(o.Key, o.SizeBytes, LastModified));
        var all = walKeys.Concat(prefixKeys)
            .Where(o => o.Key.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
        return Task.FromResult(Result<IReadOnlyList<S3ObjectInfo>>.Success(
            (IReadOnlyList<S3ObjectInfo>)all));
    }

    public Task<Result<IReadOnlyList<string>>> ListFullsAsync(
        string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
    {
        if (FailList)
            return Task.FromResult(Result<IReadOnlyList<string>>.Failed(
                new ApplicationException("fake S3 list failure (FailList)")));

        var fromFulls = Fulls.Where(f => f.Cluster == cluster && f.Shard == shard)
            .Select(f => f.Id);
        // Объект полного — `full/<id>.tar`: id — имя без `.tar` (spec t01 §6.1).
        var fromPrefixObjects = PrefixObjects
            .Select(o => o.Key)
            .Select(key => key.StartsWith($"{cluster}/{shard}/full/", StringComparison.Ordinal)
                ? key[$"{cluster}/{shard}/full/".Length..].Split('/')[0]
                : "")
            .Where(name => name.Length > 0)
            .Select(name => name.EndsWith(".tar", StringComparison.Ordinal) && name.Length > 4
                ? name[..^".tar".Length]
                : name);
        var ids = fromFulls.Concat(fromPrefixObjects)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(id => id, StringComparer.Ordinal)
            .ToList();
        return Task.FromResult(Result<IReadOnlyList<string>>.Success(
            (IReadOnlyList<string>)ids));
    }

    public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
    {
        if (FailNextDelete)
        {
            FailNextDelete = false;
            return Task.FromResult(Result.Failed(
                new ApplicationException("fake S3 delete failure (FailNextDelete)")));
        }

        foreach (var key in keys)
        {
            DeletedKeys.Add(key);
            Objects.RemoveAll(o => $"{o.Cluster}/{o.Shard}/wal/{o.Name}" == key);
            PrefixObjects.RemoveAll(o => o.Key == key);
            PrefixedObjects.RemoveAll(o => $"{o.Cluster}/{o.Shard}/{o.Key}" == key);
            var parts = key.Split('/', 3);
            if (parts.Length == 3)
                StreamObjects.Remove((parts[0], parts[1], parts[2]));
        }

        return Task.FromResult(Result.Success());
    }

    // Одиночное удаление полного (`full/<id>.tar`, ретенция/супервизор).
    public Task<Result> DeleteObjectAsync(
        string cluster, string shard, string key, CancellationToken ct)
    {
        if (FailNextDelete)
        {
            FailNextDelete = false;
            return Task.FromResult(Result.Failed(
                new ApplicationException("fake S3 delete failure (FailNextDelete)")));
        }

        var absolute = $"{cluster}/{shard}/{key}";
        DeletedKeys.Add(absolute);
        PrefixObjects.RemoveAll(o => o.Key == absolute);
        PrefixedObjects.RemoveAll(o => $"{o.Cluster}/{o.Shard}/{o.Key}" == absolute);
        StreamObjects.Remove((cluster, shard, key));
        return Task.FromResult(Result.Success());
    }

    // put байтов — объект в память (sha256 не проверяется: фейк не транспорт).
    public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
    {
        if (Fails)
            return Task.FromResult(Result.Failed(new ApplicationException("s3 down")));
        PrefixObjects.Add((key, data.Length));
        var parts = key.Split('/', 3);
        if (parts.Length == 3)
        {
            if (parts[2].StartsWith("wal/", StringComparison.Ordinal))
            {
                var name = parts[2][4..];
                if (!Objects.Any(o => o.Cluster == parts[0] && o.Shard == parts[1] && o.Name == name))
                    Objects.Add((parts[0], parts[1], name));
            }
            else
            {
                StreamObjects[(parts[0], parts[1], parts[2])] = data;
            }
        }

        return Task.FromResult(Result.Success());
    }

    // ── Потоковые чтения (restore/verify/drill) ──

    public Task<Result<(bool Exists, long SizeBytes)>> HeadObjectAsync(
        string cluster, string shard, string key, CancellationToken ct)
    {
        if (Fails)
            return Task.FromResult(Result<(bool Exists, long SizeBytes)>.Failed(
                new ApplicationException("s3 head failed")));
        if (StreamObjects.TryGetValue((cluster, shard, key), out var bytes))
            return Task.FromResult(Result<(bool Exists, long SizeBytes)>.Success((true, bytes.Length)));
        var size = PrefixObjects.FirstOrDefault(o => o.Key == $"{cluster}/{shard}/{key}").SizeBytes;
        var exists = size > 0;
        return Task.FromResult(Result<(bool Exists, long SizeBytes)>.Success((exists, size)));
    }

    public Task<Result<(Stream Body, long Size)>> GetObjectStreamAsync(
        string cluster, string shard, string key, CancellationToken ct)
    {
        if (Fails)
            return Task.FromResult(Result<(Stream Body, long Size)>.Failed(
                new ApplicationException("s3 stream failed")));
        if (!StreamObjects.TryGetValue((cluster, shard, key), out var bytes))
            return Task.FromResult(Result<(Stream Body, long Size)>.Failed(
                new ApplicationException($"fake S3 get-stream {cluster}/{shard}/{key}: not found")));
        return Task.FromResult(Result<(Stream Body, long Size)>.Success(
            (new MemoryStream(bytes, writable: false), bytes.Length)));
    }

    public Task<Result<byte[]>> GetObjectRangeAsync(
        string cluster, string shard, string key, long start, long end, CancellationToken ct)
    {
        if (Fails)
            return Task.FromResult(Result<byte[]>.Failed(new ApplicationException("s3 range failed")));
        if (!StreamObjects.TryGetValue((cluster, shard, key), out var bytes))
            return Task.FromResult(Result<byte[]>.Failed(
                new ApplicationException($"fake S3 get-range {cluster}/{shard}/{key}: not found")));
        var from = (int)Math.Clamp(start, 0, bytes.Length);
        var toExclusive = (int)Math.Clamp(end + 1, from, bytes.Length);
        return Task.FromResult(Result<byte[]>.Success(bytes[from..toExclusive]));
    }

    // ── Multipart-гигиена супервизора (spec t01 §3.7) ──

    public sealed class FakeMultipartSession(string key, string uploadId) : IMultipartUploadSession
    {
        public string Key => key;

        public string UploadId => uploadId;
        public Task<Result<string>> UploadPartAsync(int partNumber, byte[] data, CancellationToken ct)
            => Task.FromResult(Result<string>.Success("etag"));
        public Task<Result<string>> CompleteAsync(IReadOnlyDictionary<int, string> partETags, CancellationToken ct)
            => Task.FromResult(Result<string>.Success("etag"));
        public Task<Result> AbortAsync(CancellationToken ct) => Task.FromResult(Result.Success());
    }

    public Task<Result<IMultipartUploadSession>> CreateMultipartUploadAsync(
        string key, CancellationToken ct)
        => Task.FromResult(Result<IMultipartUploadSession>.Success(
            new FakeMultipartSession(key, Guid.NewGuid().ToString("N"))));

    public Task<Result<IReadOnlyList<MultipartUploadInfo>>> ListMultipartUploadsAsync(
        string prefix, CancellationToken ct)
    {
        if (Fails || FailMultipartList)
            return Task.FromResult(Result<IReadOnlyList<MultipartUploadInfo>>.Failed(
                new ApplicationException("fake S3 list-multipart failed")));
        var uploads = MultipartUploads
            .Where(u => u.Key.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
        return Task.FromResult(Result<IReadOnlyList<MultipartUploadInfo>>.Success(
            (IReadOnlyList<MultipartUploadInfo>)uploads));
    }

    public Task<Result> AbortMultipartUploadAsync(string key, string uploadId, CancellationToken ct)
    {
        if (Fails)
            return Task.FromResult(Result.Failed(new ApplicationException("fake S3 abort-multipart failed")));
        AbortedUploads.Add((key, uploadId));
        MultipartUploads.RemoveAll(u => u.Key == key && u.UploadId == uploadId);
        return Task.FromResult(Result.Success());
    }
}

/// <summary>Фейк Patroni REST : Kestrel https на свободном порту (зонд
/// TcpListener(0) — никаких литералов), отдаёт заданный /cluster-JSON; teardown
/// при любом исходе. Живёт до DisposeAsync — тест управляет появлением sync.
/// 8008 — TLS; серт из статической тестовой CA, клиенты проб берутся
/// CreateProbeClient() (доверие цепочкой, hostname не сверяется).</summary>
public sealed class FakePatroni : IAsyncDisposable
{
    // Тестовая CA фейка: один сертификат SAN 127.0.0.1 на процесс тестов.
    private static readonly (string CaPem, string CaKeyPem) Ca = E2e.FakePatroniPki.Ca;

    private WebApplication _app = null!;

    private FakePatroni(int port)
    {
        Port = port;
    }

    public int Port { get; }

    private string Body { get; init; } = "";

    // HttpClient ShardProbe-путей теста: https с доверием CA фейка.
    public static HttpClient CreateProbeClient()
        => new(new SocketsHttpHandler
        {
            SslOptions = new System.Net.Security.SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                {
                    var cert = certificate as X509Certificate2
                        ?? (certificate is null ? null : new X509Certificate2(certificate));
                    return cert is not null
                        && Shared.Tls.TlsChain.ValidateChain(cert, E2e.FakePatroniPki.CaCert);
                },
            },
        });

    public static async Task<FakePatroni> StartAsync(string body, CancellationToken ct)
    {
        var port = FreePort();
        var fake = new FakePatroni(port) { Body = body };

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(o =>
            o.Listen(IPAddress.Loopback, port,
                lo => lo.UseHttps(E2e.FakePatroniPki.ServerCert)));
        var app = builder.Build();
        app.MapGet("/cluster", () => Results.Text(fake.Body, "application/json"));
        await app.StartAsync(ct);
        fake._app = app;
        return fake;
    }

    // Свободный порт: зонд TcpListener(0) (динамические порты везде).
    private static int FreePort()
    {
        var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        try
        {
            return ((IPEndPoint)probe.LocalEndpoint).Port;
        }
        finally
        {
            probe.Stop();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}

// тестовая фабрика пула — резолвер всегда возвращает контекст с заданным
// write/read-списками (одна fake-цель на все uri — PoolBackupS3 пробрасывает
// вызовы в FakeBackupS3 теста). AAA-хелпер перевода существующих сценариев
// WalStreamProcess/Retention на пул-режим без etcd-сида /s3/.
public static class FakePoolFactory
{
    public static BackupS3Factory Single(
        IBackupS3 target, string bucket = "pgworker-backups", TimeSpan? breakerWindow = null)
    {
        var pool = new PgWorker.Etcd.S3.S3Pool("backups", false,
            [new PgWorker.Etcd.S3.S3PoolNode("node1", "RUNNING", "h1", "h1", 9000)],
            ["h1:9000"], Secrets: new("ak", "sk"));
        var ctx = new BackupPoolContext(
            "backups", "ak", "sk",
            ["http://h1:9000"], ["http://h1:9000"], ["http://h1:9000"]);
        return new BackupS3Factory(
            new BackupPoolResolver(
                new FixedPoolStore(pool),
                policyReader: (_, _) => Task.FromResult<PgWorker.Etcd.Parsing.BackupPolicy?>(
                    new PgWorker.Etcd.Parsing.BackupPolicy(1, 1, 1, 86400, true,
                        S3: new PgWorker.Etcd.Parsing.BackupPolicyS3(
                            "backups", ["node1"], ["node1"])))),
            new FixedPoolStore(pool),
            () => bucket, () => null, TimeSpan.FromSeconds(5),
            targetFactory: _ => target,
            breakerWindow: breakerWindow);
    }

    // Пул с ДВУМЯ read-целями, одна из которых мертва (arch/19 §5 —
    // неполнота union-наблюдения контроля): node1 → alive-фейк (write+read),
    // node2 → dead-фейк (транспортный отказ листинга).
    public static BackupS3Factory SplitAliveDead(
        IBackupS3 alive, IBackupS3 dead, string bucket = "pgworker-backups",
        TimeSpan? breakerWindow = null)
    {
        var pool = new PgWorker.Etcd.S3.S3Pool("backups", false,
            [new PgWorker.Etcd.S3.S3PoolNode("node1", "RUNNING", "h1", "h1", 9000),
             new PgWorker.Etcd.S3.S3PoolNode("node2", "RUNNING", "h2", "h2", 9000)],
            ["h1:9000", "h2:9000"], Secrets: new("ak", "sk"));
        var ctx = new BackupPoolContext(
            "backups", "ak", "sk",
            ["http://h1:9000"], ["http://h1:9000"], ["http://h1:9000", "http://h2:9000"]);
        return new BackupS3Factory(
            new BackupPoolResolver(
                new FixedPoolStore(pool),
                policyReader: (_, _) => Task.FromResult<PgWorker.Etcd.Parsing.BackupPolicy?>(
                    new PgWorker.Etcd.Parsing.BackupPolicy(1, 1, 1, 86400, true,
                        S3: new PgWorker.Etcd.Parsing.BackupPolicyS3(
                            "backups", ["node1"], ["node1"])))),
            new FixedPoolStore(pool),
            () => bucket, () => null, TimeSpan.FromSeconds(5),
            targetFactory: uri => uri == "http://h2:9000" ? dead : alive,
            breakerWindow: breakerWindow);
    }

    // Два пула («backups», «cold») на ОДНОМ фейк-таргете: union-сверки свипера
    // (arch/19 §5) видят одинаковые ключи в обоих пулах — дедуп по (пул, ключ)
    // учитывает каждую пул-копию. Store отдаётся раздельно (свипер листит имена
    // пулов из него).
    public static (BackupS3Factory Factory, PgWorker.Etcd.S3.IS3PoolStore Store) TwoPoolsSameTarget(
        IBackupS3 target, string bucket = "pgworker-backups")
    {
        var store = new MultiPoolStore(
            new PgWorker.Etcd.S3.S3Pool("backups", false,
                [new PgWorker.Etcd.S3.S3PoolNode("node1", "RUNNING", "h1", "h1", 9000)],
                ["h1:9000"], Secrets: new("ak", "sk")),
            new PgWorker.Etcd.S3.S3Pool("cold", false,
                [new PgWorker.Etcd.S3.S3PoolNode("node1", "RUNNING", "h1", "h1", 9000)],
                ["h1:9000"], Secrets: new("ak", "sk")));
        var factory = new BackupS3Factory(
            new BackupPoolResolver(
                store,
                policyReader: (_, _) => Task.FromResult<PgWorker.Etcd.Parsing.BackupPolicy?>(
                    new PgWorker.Etcd.Parsing.BackupPolicy(1, 1, 1, 86400, true,
                        S3: new PgWorker.Etcd.Parsing.BackupPolicyS3(
                            "backups", ["node1"], ["node1"])))),
            store,
            () => bucket, () => null, TimeSpan.FromSeconds(5),
            targetFactory: _ => target);
        return (factory, store);
    }

    // Отдельный store для процессов, берущих фабрику И store раздельно
    // (RetentionProcess/BackupOrphanSweeper — union пулов).
    public static PgWorker.Etcd.S3.IS3PoolStore Store() => new FixedPoolStore(
        new PgWorker.Etcd.S3.S3Pool("backups", false,
            [new PgWorker.Etcd.S3.S3PoolNode("node1", "RUNNING", "h1", "h1", 9000)],
            ["h1:9000"], Secrets: new("ak", "sk")));

    private sealed class FixedPoolStore(PgWorker.Etcd.S3.S3Pool? pool) : PgWorker.Etcd.S3.IS3PoolStore
    {
        public Task<Result<PgWorker.Etcd.S3.S3Pool?>> ReadAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<PgWorker.Etcd.S3.S3Pool?>.Success(pool));

        public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(pool is null ? [] : [pool.Name]));
    }

    // Store на несколько пулов: ListPoolNamesAsync — все имена, ReadAsync — по имени.
    private sealed class MultiPoolStore(params PgWorker.Etcd.S3.S3Pool[] pools) : PgWorker.Etcd.S3.IS3PoolStore
    {
        public Task<Result<PgWorker.Etcd.S3.S3Pool?>> ReadAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<PgWorker.Etcd.S3.S3Pool?>.Success(
                pools.FirstOrDefault(p => p.Name == name)));

        public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(
                pools.Select(p => p.Name).ToArray()));
    }
}
