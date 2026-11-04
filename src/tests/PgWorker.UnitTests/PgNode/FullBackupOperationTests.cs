using System.Security.Cryptography;
using System.Text;
using PgWorker.Backups;
using PgWorker.Backups.Tar;
using PgWorker.Etcd.S3;
using PgWorker.PgNode;
using PgWorker.PgNode.BaseBackup;
using PgWorker.PgNode.Operations;
using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.UnitTests.PgNode;

// Операция полного бэкапа приёмника (spec t01 §3.4) на фейках: BASE_BACKUP →
// единый tar → multipart ОДНИМ объектом full/<id>.tar; фазы running/basebackup
// → uploading → succeeded; дубль закрытых сегментов набора в wal/ (строчные
// имена, Range-GET из своего объекта); отказ endpoint'а → следующая попытка;
// исчерпание → s3-pool-exhausted; cancel → failed cancelled.
public class FullBackupOperationTests
{
    private const string Cluster = "c1";
    private const string Shard = "s1";
    private const string Node = "n1";
    private const string Id = "20260101000000Z";
    private const string ObjectKey = $"{Cluster}/{Shard}/full/{Id}.tar";

    private static readonly byte[] Segment2 = Encoding.ASCII.GetBytes("wal-segment-0002");
    private static readonly byte[] SegmentAb = Encoding.ASCII.GetBytes("wal-segment-00AB");
    private const string LabelText =
        "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n" +
        "CHECKPOINT LOCATION: 0/2000028\nBACKUP METHOD: streamed\n";
    private const string ManifestJson = """{"PostgreSQL-Backup-Manifest-Version":2,"Files":[],"WAL-Ranges":[]}""";

    // Фейковый пароль юнит-теста (не секрет).
    private static string Password =>
        Environment.GetEnvironmentVariable("PW_TEST_BACKUP_PASSWORD") ?? "unit-test-only-not-a-secret";

    private static string PasswordKey => $"/clusters/{Cluster}/backup_password";
    private static string PolicyKey => $"/pgworker/backups/{Cluster}/policy";

    private static PgNodeIdentity Identity()
        => PgNodeIdentity.Parse(
            key => key switch
            {
                "PGW_BACKUP_CLUSTER" => Cluster,
                "PGW_BACKUP_SHARD" => Shard,
                "PGW_BACKUP_NODE" => Node,
                "PGW_ETCD" => "http://etcd-1:2379",
                "PGW_BACKUP_S3_BUCKET" => "backups",
                _ => null,
            },
            out _)!;

    private static FakeEtcd Etcd(string writeTargets = "\"s3a\"")
        => new()
        {
            Kvs =
            {
                [PasswordKey] = Password,
                [PolicyKey] =
                    "{\"s3\":{\"pool\":\"p1\",\"write_master\":[" + writeTargets +
                    "],\"write_replica\":[" + writeTargets + "]}}",
            },
        };

    private static FakePools Pools(params string[] endpoints)
        => new(new S3Pool("p1", false,
            endpoints.Select(e => new S3PoolNode(e, "RUNNING", "host1", e, 9000)).ToList(),
            endpoints.Select(e => $"{e}:9000").ToList(),
            new S3PoolSecrets("ak", "sk")));

    /// <summary>Реестр с историей снимков (проверка порядка фаз).</summary>
    private sealed class RecordingRegistry : OperationRegistry
    {
        private readonly object _gate = new();

        public List<OperationSnapshot> History { get; } = [];

        public override void Update(OperationSnapshot next)
        {
            lock (_gate)
                History.Add(next);
            base.Update(next);
        }
    }

    // AAA: happy path — фазы running/basebackup → uploading → succeeded,
    // ОДИН объект full/<id>.tar, backup_manifest последней записью, дубль
    // закрытых сегментов набора в wal/ (строчные имена), sha256/size в снимке.
    [Fact]
    public async Task HappyPath_SingleObject_PhasesWalDuplicateAndSha()
    {
        // Arrange
        var fake = new FakeMultipartS3("s3a");
        var registry = new RecordingRegistry();
        var op = Create(Etcd(), Pools("s3a"), registry, fake,
            identify: new FakeIdentifySource(activeSegment: 0xAD));

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert — фазы операции
        registry.History.Select(s => $"{s.State}/{s.Phase}").Should().ContainInOrder(
            "running/basebackup", "uploading/uploading", "succeeded/uploading");
        // Assert — ровно один объект, ключ полного
        fake.CreatedKeys.Should().Equal(ObjectKey);
        fake.Objects.Keys.Should().Equal(ObjectKey);
        var objectBytes = fake.Objects[ObjectKey];
        // Assert — манифест последней записью единого tar
        var entryNames = ReadEntryNames(objectBytes);
        entryNames.Should().Contain("backup_label");
        entryNames.Should().Contain("pg_wal/000000010000000000000002");
        entryNames[^1].Should().Be("backup_manifest");
        // Assert — дубль закрытых сегментов: данные ровно из tar-объекта
        fake.WalPuts.Select(p => p.Key).Should().BeEquivalentTo(
            $"{Cluster}/{Shard}/wal/000000010000000000000002",
            $"{Cluster}/{Shard}/wal/0000000100000000000000ab");
        fake.WalPuts.Single(p => p.Key.EndsWith("000000010000000000000002"))
            .Data.Should().Equal(Segment2);
        fake.WalPuts.Single(p => p.Key.EndsWith("0000000100000000000000ab"))
            .Data.Should().Equal(SegmentAb);
        // Assert — итоговый снимок
        var snap = registry.Current!;
        snap.State.Should().Be("succeeded");
        snap.WalStartSegment.Should().Be("000000010000000000000002");
        snap.SizeBytes.Should().Be(objectBytes.Length);
        snap.Sha256.Should().Be(Convert.ToHexString(SHA256.HashData(objectBytes)).ToLowerInvariant());
        snap.BytesDone.Should().Be(objectBytes.Length);
        snap.PartsDone.Should().BeGreaterThan(0);
    }

    // AAA: живой сегмент источника в wal/ не дублируется (финал доставит стрим).
    [Fact]
    public async Task LiveSegmentInSource_NotDuplicated()
    {
        // Arrange — активный сегмент источника 0xAB=171: сегмент 0AB набора живой.
        var fake = new FakeMultipartS3("s3a");
        var registry = new RecordingRegistry();
        var op = Create(Etcd(), Pools("s3a"), registry, fake,
            new FakeIdentifySource(activeSegment: 0xAB));

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert — закрытый сегмент 2 продублирован, живой 0AB — нет.
        registry.Current!.State.Should().Be("succeeded");
        fake.WalPuts.Select(p => p.Key).Should().Equal(
            $"{Cluster}/{Shard}/wal/000000010000000000000002");
    }

    // AAA: отказ первой цели на части → Abort → полный рестарт на второй цели.
    [Fact]
    public async Task FirstEndpointFailsOnPart_RetriedOnSecond()
    {
        // Arrange — первая цель кидает на заливке части, вторая работает.
        var first = new FakeMultipartS3("s3a") { FailPartNumber = 1 };
        var second = new FakeMultipartS3("s3b");
        var registry = new RecordingRegistry();
        var op = Create(Etcd("\"s3a\",\"s3b\""), Pools("s3a", "s3b"), registry, first,
            identify: new FakeIdentifySource(activeSegment: 0xAD), second: second);

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert — первая цель отменена, объект на второй, операция succeeded.
        first.Aborted.Should().BeTrue();
        first.Objects.Should().BeEmpty();
        second.Objects.Keys.Should().Equal(ObjectKey);
        registry.Current!.State.Should().Be("succeeded");
    }

    // AAA: исчерпание write-списка → failed с маркером s3-pool-exhausted.
    [Fact]
    public async Task AllEndpointsFail_FailedPoolExhausted()
    {
        // Arrange — обе цели отказывают на заливке части.
        var first = new FakeMultipartS3("s3a") { FailPartNumber = 1 };
        var second = new FakeMultipartS3("s3b") { FailPartNumber = 1 };
        var registry = new RecordingRegistry();
        var op = Create(Etcd("\"s3a\",\"s3b\""), Pools("s3a", "s3b"), registry, first,
            identify: new FakeIdentifySource(activeSegment: 0xAD), second: second);

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        var snap = registry.Current!;
        snap.State.Should().Be("failed");
        snap.Error.Should().Contain(S3PoolMarkers.Exhausted);
        first.Aborted.Should().BeTrue();
        second.Aborted.Should().BeTrue();
    }

    // AAA: обрыв BASE_BACKUP-потока (EOF посреди ответа) — попытка падает,
    // вторая тоже (EOF) → failed s3-pool-exhausted; сессии отменены.
    [Fact]
    public async Task StreamEofMidBackup_BothAttemptsFail_Exhausted()
    {
        // Arrange — источник отдаёт только стартовые сообщения (без COPY).
        var first = new FakeMultipartS3("s3a");
        var second = new FakeMultipartS3("s3b");
        var registry = new RecordingRegistry();
        var op = Create(Etcd("\"s3a\",\"s3b\""), Pools("s3a", "s3b"), registry, first,
            identify: new FakeIdentifySource(activeSegment: 0xAD), second: second,
            wireMessages: [RowDescription(2), Message('D'), Message('C')]);

        // Act
        await op.RunAsync(TestContext.Current.CancellationToken);

        // Assert
        var snap = registry.Current!;
        snap.State.Should().Be("failed");
        snap.Error.Should().Contain(S3PoolMarkers.Exhausted);
        first.Aborted.Should().BeTrue();
        second.Aborted.Should().BeTrue();
    }

    // AAA: cancel посреди BASE_BACKUP → Abort сессии, failed `cancelled`
    // (без перебора следующего endpoint'а).
    [Fact]
    public async Task Cancel_MidBackup_FailedCancelled_NoEndpointRetry()
    {
        // Arrange — источник «висит» в ReadAsync до отмены.
        var fake = new FakeMultipartS3("s3a");
        var registry = new RecordingRegistry();
        var started = new TaskCompletionSource();
        var op = Create(Etcd(), Pools("s3a"), registry, fake,
            identify: new FakeIdentifySource(activeSegment: 0xAD), blockingSource: started);
        var testCt = TestContext.Current.CancellationToken;

        // Act
        var run = Task.Run(() => op.RunAsync(CancellationToken.None), testCt);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10), testCt);
        await op.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(10), testCt);

        // Assert
        registry.Current!.State.Should().Be("failed");
        registry.Current.Error.Should().Be("cancelled");
        fake.Aborted.Should().BeTrue();
        fake.CreatedKeys.Should().ContainSingle(); // перебора endpoint'ов нет
    }

    // ── Билдеры и фейки ──

    private static FullBackupOperation Create(
        FakeEtcd etcd, FakePools pools, RecordingRegistry registry,
        FakeMultipartS3 first, FakeIdentifySource? identify = null,
        FakeMultipartS3? second = null, PgMessage[]? wireMessages = null,
        TaskCompletionSource? blockingSource = null)
    {
        var byEndpoint = new Dictionary<string, FakeMultipartS3>(StringComparer.Ordinal)
        {
            ["http://s3a:9000"] = first,
        };
        if (second is not null)
            byEndpoint["http://s3b:9000"] = second;
        return new FullBackupOperation(
            Id, Identity(), etcd, ["http://etcd-1:2379"], pools,
            _ => Task.FromResult(WalReceiverRole.Master), registry,
            targetFactory: endpoint => byEndpoint[endpoint],
            sourceFactory: _ => identify ?? new FakeIdentifySource(activeSegment: 0xAD),
            wireFactory: (_, _) => blockingSource is { } started
                ? new BlockingSource(started)
                : new FakeWireSource(wireMessages ?? DefaultWireMessages()));
    }

    // Ответ BASE_BACKUP (реальный протокол PG18): стартовый result set,
    // tablespace list, ОДИН COPY OUT-поток (base.tar + манифест), финальный set.
    private static PgMessage[] DefaultWireMessages()
    {
        var tar = BuildBaseTarAsync().GetAwaiter().GetResult();
        return
        [
            RowDescription(2), Message('D'), Message('C'),
            RowDescription(3), Message('D'), Message('C'),
            Message('H'),
            CopyData(ArchiveStart("base.tar")),
            CopyData(Data(tar)),
            CopyData(Manifest()), CopyData(Data(Encoding.ASCII.GetBytes(ManifestJson))),
            Message('c'),
            RowDescription(2), Message('D'), Message('C'),
        ];
    }

    private static async Task<byte[]> BuildBaseTarAsync()
    {
        var buffer = new MemoryStream();
        using var writer = new TarStreamWriter(buffer);
        var label = Encoding.ASCII.GetBytes(LabelText);
        await writer.WriteEntryAsync(
            new TarEntry("backup_label", TarTypeFlags.File, label.Length, 0x1A0, ""), label, default);
        var conf = Encoding.ASCII.GetBytes("x");
        await writer.WriteEntryAsync(
            new TarEntry("postgresql.conf", TarTypeFlags.File, conf.Length, 0x1A4, ""), conf, default);
        await writer.WriteEntryAsync(
            new TarEntry("pg_wal/000000010000000000000002", TarTypeFlags.File, Segment2.Length, 0x1A4, ""),
            Segment2, default);
        // Сервер кладёт сегменты набора ВЕРХНИМ hex — проверка нормализации дубля.
        await writer.WriteEntryAsync(
            new TarEntry("pg_wal/0000000100000000000000AB", TarTypeFlags.File, SegmentAb.Length, 0x1A4, ""),
            SegmentAb, default);
        await writer.FinishAsync(default);
        return buffer.ToArray();
    }

    private static List<string> ReadEntryNames(byte[] tar)
    {
        var names = new List<string>();
        var reader = new TarStreamReader(new MemoryStream(tar));
        while (reader.ReadHeaderAsync(default).GetAwaiter().GetResult() is { } entry)
        {
            names.Add(entry.Name);
            // Данные не нужны: следующий ReadHeaderAsync дренирует остаток.
        }

        return names;
    }

    private static PgMessage RowDescription(int fields)
    {
        var payload = new byte[2];
        System.Buffers.Binary.BinaryPrimitives.WriteInt16BigEndian(payload, (short)fields);
        return new PgMessage((byte)'T', payload);
    }

    private static PgMessage Message(char type, string payload = "")
        => new((byte)type, Encoding.ASCII.GetBytes(payload));

    private static PgMessage CopyData(byte[] payload) => new((byte)'d', payload);

    private static byte[] ArchiveStart(string name)
    {
        var payload = new byte[1 + name.Length + 2];
        payload[0] = (byte)'n';
        Encoding.ASCII.GetBytes(name).CopyTo(payload, 1);
        return payload;
    }

    private static byte[] Manifest() => [(byte)'m'];

    private static byte[] Data(byte[] data)
    {
        var payload = new byte[1 + data.Length];
        payload[0] = (byte)'d';
        data.CopyTo(payload, 1);
        return payload;
    }

    /// <summary>Фейк-источник BASE_BACKUP: запрограммированная последовательность;
    /// пустая очередь — EOF (null) посреди ответа.</summary>
    private sealed class FakeWireSource(params PgMessage[] messages) : IBaseBackupSource
    {
        private readonly Queue<PgMessage?> _queue = new(messages);

        public Task SendAsync(string command, CancellationToken ct) => Task.CompletedTask;

        public Task<PgMessage?> ReadAsync(CancellationToken ct)
            => Task.FromResult(_queue.Count > 0 ? _queue.Dequeue() : null);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Источник, «висящий» в чтении до отмены (cancel-сценарий).</summary>
    private sealed class BlockingSource(TaskCompletionSource started) : IBaseBackupSource
    {
        public Task SendAsync(string command, CancellationToken ct) => Task.CompletedTask;

        public async Task<PgMessage?> ReadAsync(CancellationToken ct)
        {
            started.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            return null;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Фейк S3-цели: собирает multipart-объект из частей, отдаёт Range
    /// из него, копит wal-put'ы; управляемо отказывает созданию/части.</summary>
    private sealed class FakeMultipartS3(string name)
        : IBackupS3, IMultipartS3, IStreamObjectS3, IWalSegmentGuard
    {
        private readonly Dictionary<string, SortedDictionary<int, byte[]>> _uploads = [];

        public string Name => name;

        public int FailPartNumber { get; init; }

        public List<string> CreatedKeys { get; } = [];

        public Dictionary<string, byte[]> Objects { get; } = [];

        public List<(string Key, byte[] Data)> WalPuts { get; } = [];

        public List<(long Start, long End)> Ranges { get; } = [];

        public bool Aborted { get; private set; }

        public Task<Result<IMultipartUploadSession>> CreateMultipartUploadAsync(
            string key, CancellationToken ct)
        {
            var uploadId = $"{name}-upload-{Guid.NewGuid():N}";
            _uploads[uploadId] = [];
            CreatedKeys.Add(key);
            return Task.FromResult(Result<IMultipartUploadSession>.Success(
                new Session(this, key, uploadId)));
        }

        public Task<Result<IReadOnlyList<MultipartUploadInfo>>> ListMultipartUploadsAsync(
            string prefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<MultipartUploadInfo>>.Success([]));

        public Task<Result> AbortMultipartUploadAsync(
            string key, string uploadId, CancellationToken ct)
        {
            Aborted = true;
            return Task.FromResult(Result.Success());
        }

        public Task<Result> PutWalSegmentAsync(
            string key, byte[] data, string? sha256, CancellationToken ct = default)
        {
            WalPuts.Add((key, data.ToArray()));
            return Task.FromResult(Result.Success());
        }

        public Task<Result> PutWalSegmentFileAsync(
            string key, string filePath, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result<(Stream Body, long Size)>> GetObjectStreamAsync(
            string cluster, string shard, string key, CancellationToken ct)
        {
            var bytes = Objects[$"{cluster}/{shard}/{key}"];
            return Task.FromResult(Result<(Stream Body, long Size)>.Success(
                (new MemoryStream(bytes), bytes.Length)));
        }

        public Task<Result<byte[]>> GetObjectRangeAsync(
            string cluster, string shard, string key, long start, long end, CancellationToken ct)
        {
            Ranges.Add((start, end));
            var bytes = Objects[$"{cluster}/{shard}/{key}"];
            return Task.FromResult(Result<byte[]>.Success(
                bytes[(int)start..(int)(end + 1)]));
        }

        public Task<Result<(bool Exists, long SizeBytes)>> HeadObjectAsync(
            string cluster, string shard, string key, CancellationToken ct)
        {
            var full = $"{cluster}/{shard}/{key}";
            return Task.FromResult(Result<(bool Exists, long SizeBytes)>.Success(
                Objects.TryGetValue(full, out var bytes) ? (true, (long)bytes.Length) : (false, 0L)));
        }

        public Task<Result> DeleteObjectAsync(
            string cluster, string shard, string key, CancellationToken ct)
            => throw new NotSupportedException();

        // IBackupS3 — операцией полного не используется.
        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectMd5Async(string key, byte[] data, string md5Hex, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(string cluster, string shard, string objectKey, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> GetObjectToFileAsync(string cluster, string shard, string key, string destFilePath, CancellationToken ct = default) => throw new NotSupportedException();

        private sealed class Session(FakeMultipartS3 owner, string key, string uploadId)
            : IMultipartUploadSession
        {
            public string UploadId => uploadId;

            public Task<Result<string>> UploadPartAsync(
                int partNumber, byte[] data, CancellationToken ct)
            {
                if (partNumber == owner.FailPartNumber)
                    throw new ApplicationException($"{owner.Name}: part {partNumber} failed");
                owner._uploads[uploadId][partNumber] = data.ToArray();
                return Task.FromResult(Result<string>.Success($"etag-{partNumber}"));
            }

            public Task<Result<string>> CompleteAsync(
                IReadOnlyDictionary<int, string> partETags, CancellationToken ct)
            {
                using var buffer = new MemoryStream();
                foreach (var part in owner._uploads[uploadId].Values)
                    buffer.Write(part);
                owner.Objects[key] = buffer.ToArray();
                return Task.FromResult(Result<string>.Success("etag-object"));
            }

            public Task<Result> AbortAsync(CancellationToken ct)
            {
                owner.Aborted = true;
                return Task.FromResult(Result.Success());
            }
        }
    }

    /// <summary>Фейк etcd-gateway: KV-словарь (пароль + policy).</summary>
    internal sealed class FakeEtcd : IEtcdGateway
    {
        public Dictionary<string, string> Kvs { get; } = [];

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => Task.FromResult(Result<Kv?>.Success(
                Kvs.TryGetValue(key, out var v) ? new Kv(key, v, 1) : null));

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<Kv>>.Success([.. Kvs
                .Where(kv => kv.Key.StartsWith(prefix, StringComparison.Ordinal))
                .Select(kv => new Kv(kv.Key, kv.Value, 1))]));

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        {
            Kvs[key] = value;
            return Task.FromResult(Result.Success());
        }

        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => Task.FromResult(Result<long>.Success(1));
        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => Task.FromResult(Result.Success());
        public Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
        {
            Kvs.Remove(keyOrPrefix);
            return Task.FromResult(Result.Success());
        }

        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result<EtcdMember>> MemberAddAsync(string endpoint, IReadOnlyList<string> peerUrls, bool isLearner, CancellationToken ct)
            => throw new NotSupportedException("member-API не поддерживается этим фейком");
        public Task<Result> MemberRemoveAsync(string endpoint, ulong memberId, CancellationToken ct)
            => throw new NotSupportedException("member-API не поддерживается этим фейком");
        public Task<Result> MemberPromoteAsync(string endpoint, ulong memberId, CancellationToken ct)
            => throw new NotSupportedException("member-API не поддерживается этим фейком");
        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
            => throw new NotSupportedException();
    }

    /// <summary>Фейк пул-стора: фиксированный пул.</summary>
    internal sealed class FakePools(S3Pool? pool) : IS3PoolStore
    {
        public Task<Result<S3Pool?>> ReadAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<S3Pool?>.Success(pool));

        public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(pool is null ? [] : [pool.Name]));
    }

    /// <summary>Фейк replication-источника для IDENTIFY_SYSTEM дубля: только
    /// Open+Identify; активная позиция задаётся сегментом (0xAD — все сегменты
    /// набора закрыты; 0xAB — сегмент 0AB живой).</summary>
    private sealed class FakeIdentifySource(uint activeSegment) : IXLogReplicationSource
    {
        public Result Open { get; init; } = Result.Success();

        public Task<Result> OpenAsync(CancellationToken ct) => Task.FromResult(Open);

        public Task<Result<ServerIdentity>> IdentifySystemAsync(CancellationToken ct)
            => Task.FromResult(Result<ServerIdentity>.Success(
                new ServerIdentity(1, (ulong)activeSegment << 24)));

        public Task<Result> StartReplicationAsync(
            string slot, ulong startLsn, uint? timeline, CancellationToken ct)
            => throw new NotSupportedException();

        public IAsyncEnumerable<XLogChunk> Stream => throw new NotSupportedException();

        public Task<Result> SetConfirmedAsync(ulong confirmedLsn, CancellationToken ct)
            => throw new NotSupportedException();

        public Task<Result<byte[]?>> ReadTimelineHistoryAsync(uint tli, CancellationToken ct)
            => throw new NotSupportedException();

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
