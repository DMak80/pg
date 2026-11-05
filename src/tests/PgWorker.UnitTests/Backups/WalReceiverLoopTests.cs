using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Etcd.S3;
using PgWorker.PgNode;
using PgWorker.PgNode.Operations;
using Shared.Core;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Машина состояний цикла приёмника (arch/19 §3, spec §3.1 п.2–8): гейты
// (disabled → waiting_disabled; wal-ключ STOPPED/WAL_CHAIN_ERROR → paused), роль none →
// waiting_role, креды/цели (waiting_password/waiting_policy), слот (waiting_slot),
// permanent-исходы ядра без выхода процесса (failed_permanent), факты
// pool_exhausted (ставится/снимается первым успешным put) и tli_jump в
// статус-ключе, etcd-моргание при живом стриме — стрим не рвётся. Все фейки —
// транспорты из юнит-набора ядра; статус читается из etcd-фейка (значение
// статус-ключа). AAA-комментарии.
public class WalReceiverLoopTests
{
    private const int SegmentBytes = 16 * 1024 * 1024;
    private static readonly TimeSpan FastStep = TimeSpan.FromMilliseconds(20);
    private static readonly TimeSpan FastKeepalive = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(10);

    private const string Cluster = "c1";
    private const string Shard = "shard1";
    private const string Node = "s1a";

    private static string AgentKey => $"/pgworker/backups/{Cluster}/{Shard}/agent/{Node}";
    private static string WalKey => $"/pgworker/backups/{Cluster}/{Shard}/wal";
    private const string DisabledKey = "/pgworker/backups-disabled";
    private static string PasswordKey => $"/clusters/{Cluster}/backup_password";
    private static string PolicyKey => $"/pgworker/backups/{Cluster}/policy";

    private static ulong EndLsn(uint segId) => (segId + 1UL) << 24;

    private static Dictionary<string, string?> Env() => new()
    {
        ["PGW_BACKUP_CLUSTER"] = Cluster,
        ["PGW_BACKUP_SHARD"] = Shard,
        ["PGW_BACKUP_NODE"] = Node,
        ["PGW_ETCD"] = "http://etcd-1:2379",
        ["PGW_BACKUP_S3_BUCKET"] = "backups",
    };

    /// <summary>Фейк etcd-gateway: KV-словарь + lease-счётчик. FailReads —
    /// транспортный отказ ЧТЕНИЙ (гейты/пароль/policy): put/lease живут —
    /// имитация etcd-моргания при живом статус-ключе.</summary>
    private sealed class FakeEtcd : IEtcdGateway
    {
        public readonly Dictionary<string, string> Kvs = [];
        public bool FailReads;
        private long _lease;

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => Task.FromResult(FailReads
                ? Result<Kv?>.Failed(new EtcdUnreachableException("fake: etcd down"))
                : Result<Kv?>.Success(Kvs.TryGetValue(key, out var v) ? new Kv(key, v, 1) : null));

        public Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct)
            => Task.FromResult(FailReads
                ? Result<IReadOnlyList<Kv>>.Failed(new EtcdUnreachableException("fake: etcd down"))
                : Result<IReadOnlyList<Kv>>.Success([.. Kvs
                    .Where(kv => kv.Key.StartsWith(prefix, StringComparison.Ordinal))
                    .Select(kv => new Kv(kv.Key, kv.Value, 1))]));

        public Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct)
        {
            Kvs[key] = value;
            return Task.FromResult(Result.Success());
        }

        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => Task.FromResult(Result<long>.Success(Interlocked.Increment(ref _lease)));

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

        // Member-API этими тестами не используется — заглушки интерфейса.
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

    /// <summary>Фейк пул-стора: фиксированный пул (нода s3a с адресом).</summary>
    private sealed class FakePools(S3Pool? pool) : IS3PoolStore
    {
        public Task<Result<S3Pool?>> ReadAsync(string name, CancellationToken ct)
            => Task.FromResult(Result<S3Pool?>.Success(pool));

        public Task<Result<IReadOnlyList<string>>> ListPoolNamesAsync(CancellationToken ct)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(pool is null ? [] : [pool.Name]));
    }

    /// <summary>Мини-фейк IBackupS3: put в словарь; FailKeys — УСТОЙЧИВЫЙ отказ
    /// (исчерпание write-списка имитируется маркером S3PoolMarkers.Exhausted);
    /// FailListWal — УСТОЙЧИВЫЙ отказ list (ре-доклад инвентаря — откладывается);
    /// PutCalls — счётчик put-вызовов (проверка «дубль-put нет»).</summary>
    private sealed class FakeS3 : IBackupS3
    {
        public readonly Dictionary<string, byte[]> Objects = [];
        public readonly HashSet<string> FailKeys = [];
        public readonly List<string> PutOrder = []; // порядок успешных put (ре-доклад сегментов: возрастание позиций)
        public bool FailListWal;
        public int PutCalls;

        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
        {
            PutCalls++;
            if (!FailKeys.Contains(key))
            {
                // Сегментные буферы — из пула, возвращаются после put: фейк
                // копирует байты, как реальный S3 (контракт владения буфером).
                Objects[key] = data.ToArray();
                PutOrder.Add(key);
                return Task.FromResult(Result.Success());
            }

            // Маркер исчерпания пула — ядро поднимает флаг pool_exhausted.
            return Task.FromResult(Result.Failed(new ApplicationException(
                $"S3 put {key}: all write targets failed ({S3PoolMarkers.Exhausted})")));
        }

        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(FailListWal
                ? Result<IReadOnlyList<WalObject>>.Failed(new ApplicationException("fake: list wal/ down"))
                : Result<IReadOnlyList<WalObject>>.Success([.. Objects.Keys
                    .Where(k => k.StartsWith($"{cluster}/{shard}/wal/", StringComparison.Ordinal))
                    .Select(k => new WalObject(k[(k.LastIndexOf('/') + 1)..], DateTimeOffset.UnixEpoch))]));

        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
            string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
        {
            // Поглощение .partial финалом (arch/19 §3) — идемпотентный batch-delete.
            foreach (var key in keys)
                Objects.Remove(key);
            return Task.FromResult(Result.Success());
        }

        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(
            string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
        {
            // Узкий листинг версий .partial (сверка/поглощение приёмника).
            var fullPrefix = $"{cluster}/{shard}/{prefix}";
            return Task.FromResult(Result<IReadOnlyList<WalObject>>.Success([.. Objects.Keys
                .Where(k => k.StartsWith(fullPrefix, StringComparison.Ordinal))
                .Select(k => new WalObject(k[(k.LastIndexOf('/') + 1)..], DateTimeOffset.UnixEpoch))]));
        }
        public Task<Result> PutObjectMd5Async(string key, byte[] data, string md5Hex, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result<string>> GetObjectAsync(
            string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
// файловые методы интерфейса в этом тесте не используются —
        // заглушка (фейк нужен только перечисленным выше операциям).
        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default)
            => Task.FromResult(Result.Failed(new ApplicationException("put-file: not supported by this fake")));

        public Task<Result> GetObjectToFileAsync(string cluster, string shard, string key, string destFilePath, CancellationToken ct = default)
            => Task.FromResult(Result.Failed(new ApplicationException("get-to-file: not supported by this fake")));
        public Task<Result<string>> DownloadTextAsync(
            string cluster, string shard, string objectKey, CancellationToken ct = default) => throw new NotSupportedException();
    }

    /// <summary>Программируемый фейк транспорта репликации (как в WalReceiverCoreTests).</summary>
    private sealed class FakeXLogSource : IXLogReplicationSource
    {
        public readonly Queue<XLogChunk> Chunks = new();
        public Func<ulong, uint?, Result>? StartPolicy;
        public ServerIdentity? Identity { get; set; } = new(1, 0);
        public bool SegmentSizeMismatch;
        public ulong? ConfirmedLsn;
        public readonly Dictionary<uint, byte[]?> History = [];

        public Task<Result> OpenAsync(CancellationToken ct)
            => Task.FromResult(SegmentSizeMismatch
                ? Result.Failed(new ApplicationException("wal_segment_size 32MB ≠ 16 MiB"))
                : Result.Success());

        public Task<Result> StartReplicationAsync(string slot, ulong startLsn, uint? timeline, CancellationToken ct)
            => Task.FromResult(StartPolicy is { } policy
                ? policy(startLsn, timeline)
                : Result.Success());

        public Task<Result<ServerIdentity>> IdentifySystemAsync(CancellationToken ct)
            => Task.FromResult(Result<ServerIdentity>.Success(Identity!));

        public IAsyncEnumerable<XLogChunk> Stream => StreamImpl();

        private async IAsyncEnumerable<XLogChunk> StreamImpl()
        {
            while (Chunks.Count > 0)
                yield return Chunks.Dequeue();
            yield break; // очередь пуста — transient-переподключение сессии
        }

        public Task<Result> SetConfirmedAsync(ulong confirmedLsn, CancellationToken ct)
        {
            ConfirmedLsn = confirmedLsn;
            return Task.FromResult(Result.Success());
        }

        public Task<Result<byte[]?>> ReadTimelineHistoryAsync(uint tli, CancellationToken ct)
            => Task.FromResult(Result<byte[]?>.Success(History.TryGetValue(tli, out var c) ? c : null));

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Слот-ридер: значение или Failed («слот отсутствует»).</summary>
    private sealed class FakeSlots(ulong? restartLsn) : ISlotPositionReader
    {
        public Task<Result<ulong>> ReadRestartLsnAsync(string slot, CancellationToken ct)
            => Task.FromResult(restartLsn is { } lsn
                ? Result<ulong>.Success(lsn)
                : Result<ulong>.Failed(new WalSlotMissing(slot))); // типизированный «слота нет» — waiting_slot
    }

    // Посев «полного пути»: пароль + policy c секцией s3 (пул p1, нода s3a).
    private static void SeedHappyPath(FakeEtcd etcd)
    {
        etcd.Kvs[PasswordKey] = "pw";
        etcd.Kvs[PolicyKey] =
            """{"s3":{"pool":"p1","write_master":["s3a"],"write_replica":["s3a"]}}""";
    }

    private static S3Pool Pool() => new(
        "p1", false,
        [new S3PoolNode("s3a", "RUNNING", "host1", "s3a", 9000)],
        ["s3a:9000"],
        new S3PoolSecrets("ak", "sk"));

    // Посев непрерывной цепочки сегментов TLI 1 №0–1 (вход tli-процедуры).
    private static byte[] SegmentData(uint tli, ulong boundary)
    {
        var data = new byte[SegmentBytes];
        data[4] = (byte)tli;
        data[5] = (byte)(tli >> 8);
        data[6] = (byte)(tli >> 16);
        data[7] = (byte)(tli >> 24);
        for (var i = 0; i < 8; i++)
            data[8 + i] = (byte)(boundary >> (8 * i));
        return data;
    }

    private static void SeedChain(FakeS3 s3, uint[] segments)
    {
        foreach (var segId in segments)
            s3.Objects[$"{Cluster}/{Shard}/wal/00000001{segId / WalFileName.SegsPerLog:x8}{segId % WalFileName.SegsPerLog:x8}"]
                = SegmentData(1, segId * SegmentBytes);
    }

    /// <summary>Запуск цикла на фейках; снаружи — cancel через cts (StopAsync
    /// в тесте). roleProbe — фиксированная роль; sourceFactory — свежий источник
    /// на каждую итерацию цикла (подсчёт вызовов — детектор «сессия перезапускалась»).</summary>
    private static (WalReceiverLoop Loop, CancellationTokenSource Cts, List<FakeXLogSource> Sources) StartLoop(
        FakeEtcd etcd,
        WalReceiverRole role,
        Func<FakeXLogSource>? sourceFactory,
        ulong? slotRestartLsn = 0,
        FakeS3? s3 = null,
        Dictionary<string, string?>? env = null,
        string? pgWalDir = null,
        Func<WalReceiverRole>? roleScript = null,
        TimeSpan? probePeriod = null,
        TimeSpan? inventoryPeriod = null,
        Func<CancellationToken, Task<PgWorker.PgNode.Operations.HbaEnsureResponse>>? hbaEnsure = null,
        TimeSpan? retryDelay = null)
    {
        var s3Fake = s3 ?? new FakeS3();
        var sources = new List<FakeXLogSource>();
        var loop = new WalReceiverLoop(
            (env ?? Env()).GetValueOrDefault,
            etcd,
            ["http://etcd-1:2379"],
            new FakePools(Pool()),
            ct => Task.FromResult(roleScript is { } script ? script() : role),
            o =>
            {
                var source = sourceFactory is { } factory ? factory() : new FakeXLogSource();
                sources.Add(source);
                return source;
            },
            o => new FakeSlots(slotRestartLsn),
            retryDelay ?? FastStep,
            _ => s3Fake,
            FastKeepalive,
            pgWalDir,
            roleProbePeriod: probePeriod,
            inventoryPeriod: inventoryPeriod,
            hbaEnsure: hbaEnsure);
        var cts = new CancellationTokenSource();
        loop.StartAsync(CancellationToken.None);
        return (loop, cts, sources);
    }

    // Остановка цикла: cancel + StopAsync с бюджетом (зависание — видимый фейл).
    private static async Task StopLoopAsync(WalReceiverLoop loop, CancellationTokenSource cts)
    {
        await cts.CancelAsync();
        using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await loop.StopAsync(stopCts.Token);
    }

    /// <summary>Ожидание условия с бюджетом (таймауты тестов короткие).</summary>
    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan budget, Func<string>? debug = null)
    {
        var deadline = DateTime.UtcNow + budget;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.Fail($"the condition was not met within the wait budget; debug: {debug?.Invoke() ?? "none"}");
    }

    /// <summary>Ожидание статус-ключа, удовлетворяющего условию (значение из
    /// etcd-фейка — так виден весь контур lease-публикации).</summary>
    private static async Task<PgNodeAgentStatus> WaitStatusAsync(
        FakeEtcd etcd, Func<PgNodeAgentStatus, bool> match, string debug)
    {
        var deadline = DateTime.UtcNow + Budget;
        while (DateTime.UtcNow < deadline)
        {
            if (etcd.Kvs.TryGetValue(AgentKey, out var raw))
            {
                var parsed = PgNodeAgentStatusJson.Parse(raw);
                if (parsed is { } p && match(p.Status))
                    return p.Status;
            }

            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.Fail($"status condition not met within budget: {debug}; " +
                    $"raw={(etcd.Kvs.TryGetValue(AgentKey, out var r) ? r : "<absent>")}");
        throw new InvalidOperationException(); // недостижимо
    }

    [Fact]
    public async Task DisabledKey_WaitsDisabled_StreamNotStarted()
    {
        // Arrange — полный путь готов, НО глобальный стоп-ключ жив
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        etcd.Kvs[DisabledKey] = """{"updated_unix":1}""";
        var (loop, cts, sources) = StartLoop(etcd, WalReceiverRole.Master, null);

        try
        {
            // Act/Assert — статус waiting_disabled, сессия не стартовала
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.WaitingDisabled,
                "waiting_disabled");
            sources.Should().BeEmpty("streaming is gated by the disabled key");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    [Theory]
    [InlineData("STOPPED")]
    [InlineData("WAL_CHAIN_ERROR")]
    public async Task WalKeyStoppedOrChainError_Pauses_StreamNotStarted(string state)
    {
        // Arrange — wal-ключ шарда в STOPPED (карантин) / WAL_CHAIN_ERROR (разрыв)
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        etcd.Kvs[WalKey] = $$"""{"cluster":"{{Cluster}}","shard":"{{Shard}}","state":"{{state}}"}""";
        var (loop, cts, sources) = StartLoop(etcd, WalReceiverRole.Master, null);

        try
        {
            // Act/Assert — статус paused, сессия не стартовала
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.Paused, $"paused ({state})");
            sources.Should().BeEmpty("streaming is gated by the wal key state");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    [Fact]
    public async Task RoleNone_WaitsRole()
    {
        // Arrange — REST-проба роли не отвечает (переходное окно)
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var (loop, cts, sources) = StartLoop(etcd, WalReceiverRole.None, null);

        try
        {
            // Act/Assert — статус waiting_role, сессия не стартовала
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.WaitingRole, "waiting_role");
            sources.Should().BeEmpty("streaming requires a source role");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    [Fact]
    public async Task PasswordMissing_WaitsPassword()
    {
        // Arrange — ключа /clusters/<C>/backup_password нет (ensure делает воркер)
        var etcd = new FakeEtcd();
        etcd.Kvs[PolicyKey] =
            """{"s3":{"pool":"p1","write_master":["s3a"],"write_replica":["s3a"]}}""";
        var (loop, cts, sources) = StartLoop(etcd, WalReceiverRole.Master, null);

        try
        {
            // Act/Assert — статус waiting_password
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.WaitingPassword, "waiting_password");
            sources.Should().BeEmpty("streaming requires credentials");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    [Fact]
    public async Task PolicyWithoutS3_WaitsPolicy()
    {
        // Arrange — policy-ключ есть, секции s3 нет (пул не выбран)
        var etcd = new FakeEtcd();
        etcd.Kvs[PasswordKey] = "pw";
        etcd.Kvs[PolicyKey] = """{"retention":{"days":7}}""";
        var (loop, cts, sources) = StartLoop(etcd, WalReceiverRole.Master, null);

        try
        {
            // Act/Assert — статус waiting_policy
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.WaitingPolicy, "waiting_policy");
            sources.Should().BeEmpty("streaming requires backup targets");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    [Fact]
    public async Task SlotMissing_WaitsSlot()
    {
        // Arrange — полный путь готов, слота на источнике нет (ensure делает воркер)
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var (loop, cts, sources) = StartLoop(etcd, WalReceiverRole.Master, null, slotRestartLsn: null);

        try
        {
            // Act/Assert — статус waiting_slot (transient-ожидание, НЕ permanent)
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.WaitingSlot, "waiting_slot");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    [Fact]
    public async Task SegmentSizeMismatch_FailedPermanent_LoopStaysAlive()
    {
        // Arrange — источник с другим wal_segment_size
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var (loop, cts, _) = StartLoop(
            etcd, WalReceiverRole.Master,
            () => new FakeXLogSource { SegmentSizeMismatch = true });

        try
        {
            // Act/Assert — failed_permanent с текстом ошибки (НЕ exit процесса)
            await WaitStatusAsync(
                etcd, s => s.State == WalReceiverState.FailedPermanent
                           && s.Error is not null && s.Error.Contains("wal_segment_size"),
                "failed_permanent (segment size)");

            // Assert — процесс жив: машина продолжает реагировать на гейты
            // (STOPPED → paused), permanent — состояние статус-ключа, не выход
            etcd.Kvs[WalKey] = $$"""{"cluster":"{{Cluster}}","shard":"{{Shard}}","state":"STOPPED"}""";
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.Paused, "paused after permanent");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    [Fact]
    public async Task StreamLsnGap_FailedPermanent()
    {
        // Arrange — чанки с дырой в WalStart (разрыв протокола — permanent)
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var (loop, cts, _) = StartLoop(
            etcd, WalReceiverRole.Master,
            () =>
            {
                var source = new FakeXLogSource();
                source.Chunks.Enqueue(Chunk(0));
                source.Chunks.Enqueue(new XLogChunk(99UL * (1 << 20), 100UL * (1 << 20), new byte[1 << 20]));
                return source;
            });

        try
        {
            // Act/Assert — failed_permanent с текстом разрыва
            await WaitStatusAsync(
                etcd, s => s.State == WalReceiverState.FailedPermanent
                           && s.Error is not null && s.Error.Contains("gap"),
                "failed_permanent (lsn gap)");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    private static XLogChunk Chunk(ulong boundary, uint tli = 1)
        => new(boundary, boundary + SegmentBytes, SegmentData(tli, boundary));

    // AAA (debounce, граница сессий): подозрение моргания, оставшееся от
    // сессии A (ровно ОДНА проба с иной ролью), НЕ рвёт первую пробу сессии
    // B — роль подтверждается свежей пробей вне сессии (4.2), подозрение
    // гаснет; разрыв новой сессии — только вторая ПОСЛЕДОВАТЕЛЬНАЯ проба с
    // той же иной ролью. Период проб инжектируется (1 c — окна ~0.5 c с
    // запасом, тест не держит продовые 5-с интервалы; бюджет ~15 c).
    [Fact]
    public async Task RoleFlapSuspicion_FromPreviousSession_DoesNotBreakNewSession()
    {
        // Arrange — скрипт проб по вызовам: 4.2(A)=master; проба в A=sync
        // (моргание, suspicion живёт); сессию A рвёт стоп-гейт (Reconnect);
        // 4.2(B)=master (сброс suspicion); в B: sync (№1 — НЕ рвёт), sync
        // (№2 — рвёт RoleChanged)
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var script = new Queue<WalReceiverRole>(new[]
        {
            WalReceiverRole.Master, // 4.2 сессии A
            WalReceiverRole.Sync,   // проба в A — моргание (не рвёт)
            WalReceiverRole.Master, // 4.2 сессии B — прямое подтверждение, сброс
            WalReceiverRole.Sync,   // проба B №1 — не рвёт (подозрение чистое)
            WalReceiverRole.Sync,   // проба B №2 — рвёт (вторая последовательная)
        });
        var (loop, cts, sources) = StartLoop(
            etcd, WalReceiverRole.Master, () => new FakeXLogSource(),
            roleScript: () => script.Count > 0 ? script.Dequeue() : WalReceiverRole.Master,
            probePeriod: TimeSpan.FromSeconds(1));

        try
        {
            // Act/Assert — сессия A стартовала (первый источник)
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.Streaming, "streaming A");
            sources.Count.Should().Be(1);

            // Act — выжидаем первую пробу (моргание sync в A: suspicion
            // записан, сессия жива), затем рвём A стоп-гейтом (в окне ДО
            // второй пробы): пауза → подозрение переносится через границу
            await Task.Delay(TimeSpan.FromSeconds(1.5), TestContext.Current.CancellationToken);
            sources.Count.Should().Be(1, "a single flapped probe must not break the live session A");
            etcd.Kvs[WalKey] = $$"""{"cluster":"{{Cluster}}","shard":"{{Shard}}","state":"STOPPED"}""";
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.Paused, "paused (session A torn by the stop gate)");

            // Act — снимаем гейт: 4.2 подтверждает роль master (сброс suspicion),
            // сессия B стартует; первая проба B (sync) НЕ рвёт её
            etcd.Kvs.Remove(WalKey);
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.Streaming, "streaming B");
            var sessionBStarted = DateTime.UtcNow;
            await Task.Delay(TimeSpan.FromSeconds(1.5), TestContext.Current.CancellationToken);
            sources.Count.Should().Be(2, "session B started (new source)");
            sources.Count.Should().Be(2, "the FIRST flapped probe of session B must not break it (suspicion reset at the session boundary)");
            sessionBStarted.Should().BeBefore(DateTime.UtcNow);

            // Assert — вторая последовательная проба B (sync) рвёт сессию:
            // RoleChanged → цикл перепроверяет роль → новая (уже третья) сессия
            await WaitUntilAsync(() => sources.Count >= 3, TimeSpan.FromSeconds(20),
                () => $"sources={sources.Count} role-проба ещё не доехала");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    // AAA (стоп-гейт живого стрима, arch/19 §3): wal-ключ STOPPED при
    // живой сессии — проба из стрима завершает её (Reconnect), цикл
    // перепроверяет гейты и уходит в paused; приёмник не ждёт обрыва
    // транспорта.
    [Fact]
    public async Task WalKeyStopped_DuringLiveStream_PausesReceiver()
    {
        // Arrange — полный путь, сессия стримит (первый сегмент доставлен;
        // поток пуст — сессия живёт в reconnect-цикле ядра)
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var (loop, cts, sources) = StartLoop(
            etcd, WalReceiverRole.Master,
            () =>
            {
                var source = new FakeXLogSource();
                source.Chunks.Enqueue(Chunk(0));
                return source;
            });

        try
        {
            // Act/Assert — сессия стартовала и доставила сегмент ...000000
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.Streaming, "streaming");
            await WaitUntilAsync(
                () => sources.Count > 0 && sources[^1].ConfirmedLsn == EndLsn(0),
                Budget,
                () => $"confirmed={(sources.Count > 0 ? sources[^1].ConfirmedLsn?.ToString() : "-")}");

            // Act — wal-ключ шарда STOPPED при живом процессе
            etcd.Kvs[WalKey] = $$"""{"cluster":"{{Cluster}}","shard":"{{Shard}}","state":"STOPPED"}""";

            // Assert — проба из стрима гасит сессию; цикл уходит в paused
            await WaitStatusAsync(
                etcd, s => s.State == WalReceiverState.Paused,
                "paused after STOPPED while the live session was streaming");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    [Fact]
    public async Task PoolExhausted_FlagSetAndClearedByFirstSuccessfulPut()
    {
        // Arrange — write-список исчерпан (put сегмента ...000001 отказывает с
        // маркером), три сегмента в очереди источника
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var s3 = new FakeS3();
        s3.FailKeys.Add($"{Cluster}/{Shard}/wal/000000010000000000000001");
        var (loop, cts, sources) = StartLoop(
            etcd, WalReceiverRole.Master,
            () =>
            {
                var source = new FakeXLogSource();
                foreach (var boundary in new[] { 0UL, (ulong)SegmentBytes, 2UL * SegmentBytes })
                    source.Chunks.Enqueue(Chunk(boundary));
                return source;
            },
            s3: s3);

        try
        {
            // Act/Assert — флаг pool_exhausted=true опубликован ЖИВЫМ (сессия не
            // завершается в put-ретрае — факт обязан попасть в статус сразу)
            await WaitStatusAsync(etcd, s => s.PoolExhausted, "pool_exhausted=true");

            // Act — подъём ноды пула (снятие отказа): первый успешный put снимает
            s3.FailKeys.Clear();
            await WaitUntilAsync(
                () => sources.Count > 0 && sources[^1].ConfirmedLsn == EndLsn(2),
                Budget,
                () => $"confirmed={(sources.Count > 0 ? sources[^1].ConfirmedLsn?.ToString() : "-")} keys=[{string.Join(",", s3.Objects.Keys)}]");

            // Assert — флаг снят, доставка продолжилась
            await WaitStatusAsync(etcd, s => !s.PoolExhausted, "pool_exhausted=false");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    // AAA (граница сессии, arch/19 §3 «Агент»): факт pool_exhausted наблюдала
    // СЕССИЯ A (put отказал маркером исчерпания); после смены сессии (STOPPED
    // → пауза → гейт снят) новая сессия B на idle-источнике НЕ ДЕЛАЕТ ни
    // одного put — «снятый первым успешным put» не наступает никогда, и без
    // якоря границы сессии здоровый пул держал бы шард в DEGRADED неограниченно
    // (факт прогона AC5: promote-мастер без записи, wal-ключ DEGRADED весь
    // бюджет). Новая итерация цикла перепубликует флаг фактическим состоянием.
    [Fact]
    public async Task PoolExhausted_SessionBoundary_RepublishedFromFreshSession()
    {
        // Arrange — сессия A: сегмент …000000 доставлен, put отказывает
        // маркером исчерпания (write-список «мертв»); сессию рвёт STOPPED
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var s3 = new FakeS3();
        s3.FailKeys.Add($"{Cluster}/{Shard}/wal/000000010000000000000000");
        var firstSource = true;
        var (loop, cts, sources) = StartLoop(
            etcd, WalReceiverRole.Master,
            () =>
            {
                if (firstSource)
                {
                    firstSource = false;
                    var source = new FakeXLogSource();
                    source.Chunks.Enqueue(Chunk(0));
                    return source;
                }

                return new FakeXLogSource(); // сессии B+: пустой поток (idle-мастер)
            },
            s3: s3,
            probePeriod: TimeSpan.FromSeconds(1));

        try
        {
            // Act/Assert — флаг исчерпания опубликован живым (сессия в put-ретрае)
            await WaitStatusAsync(etcd, s => s.PoolExhausted, "pool_exhausted=true in session A");

            // Act — STOPPED рвёт сессию A (проба из стрима), цикл уходит в paused;
            // гейт снимаем — сессия B стартует на idle-источнике (put'ов нет)
            etcd.Kvs[WalKey] = $$"""{"cluster":"{{Cluster}}","shard":"{{Shard}}","state":"STOPPED"}""";
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.Paused, "paused (session A torn)");
            etcd.Kvs.Remove(WalKey);

            // Assert — на границе сессии факт перепубликован фактическим
            // состоянием новой сессии (исчерпания нет), БЕЗ единого put'а
            await WaitStatusAsync(
                etcd, s => !s.PoolExhausted && s.State == WalReceiverState.Streaming,
                "pool_exhausted=false at the session boundary (no puts in session B)");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    // AAA (arch/19 §3, idle-детект): инвентарный проход трогает пул S3 тем же
    // write-списком — на idle-источнике (put'ов нет) отказ прохода (list down)
    // ОБЯЗАН ставить факт pool_exhausted без ожидания закрытия сегмента;
    // успешный проход после исцеления пула снимает (симметрия с «снятый
    // первым успешным put»). retryDelay 2 с: флаг живёт до начала следующей
    // итерации (перепубликация на границе сессии, arch/19 §3) — окно ≥ 2 с
    // ловится поллингом; снятие — следующий проход после исцеления.
    [Fact]
    public async Task InventoryPoolFailure_SetsExhaustedOnIdleMaster_ClearedByHealedPool()
    {
        // Arrange — мастер без записи (источник пуст), list wal/ отказывает
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var s3 = new FakeS3 { FailListWal = true };
        var (loop, cts, _) = StartLoop(
            etcd, WalReceiverRole.Master,
            () => new FakeXLogSource { Identity = new ServerIdentity(1, 4UL * SegmentBytes) },
            s3: s3,
            retryDelay: TimeSpan.FromSeconds(2),
            inventoryPeriod: TimeSpan.FromSeconds(1));

        try
        {
            // Act/Assert — отказ инвентарного списка → факт исчерпания немедленно
            // (put'ов сессия не делает — ставить флаг больше некому)
            await WaitStatusAsync(etcd, s => s.PoolExhausted,
                "pool_exhausted=true by the failed inventory pass (idle master)");

            // Act — пул исцелился; следующий проход доводится
            s3.FailListWal = false;

            // Assert — флаг снят успешным проходом
            await WaitStatusAsync(etcd, s => !s.PoolExhausted,
                "pool_exhausted=false by the successful inventory pass");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    // AAA (arch/19 §7, первый канал hba): приёмник — владелец строки
    // backup_exec — самообеспечение выполняется ДО сессии цикла подключения
    // (воркерный тик не связан со стартом контейнера — гонка 28000).
    [Fact]
    public async Task Loop_EnsuresHbaLine_BeforeSessionEachCycle()
    {
        // Arrange — happy path, счётчик вызовов самообеспечения
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var ensureCalls = 0;
        var (loop, cts, _) = StartLoop(
            etcd, WalReceiverRole.Master,
            () => new FakeXLogSource(),
            hbaEnsure: _ =>
            {
                Interlocked.Increment(ref ensureCalls);
                return Task.FromResult(new PgWorker.PgNode.Operations.HbaEnsureResponse(
                    Applied: false, Bootstrap: false));
            });

        try
        {
            // Act/Assert — сессия стартовала (status streaming): самообеспечение
            // было выполнено ДО неё хотя бы раз
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.Streaming, "streaming");
            Assert.True(ensureCalls >= 1,
                $"hba self-ensure must run before the session cycle; calls={ensureCalls}");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    [Fact]
    public async Task TliJump_FactPublishedToStatus()
    {
        // Arrange — хвост S3 на TLI 1, сервер промоутнулся на TLI 2, старты
        // без TIMELINE отказывают (58P01): досыпка отказывает → прыжок-fallback
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var s3 = new FakeS3();
        SeedChain(s3, [0U, 1U]);
        var (loop, cts, _) = StartLoop(
            etcd, WalReceiverRole.Master,
            () => new FakeXLogSource
            {
                Identity = new ServerIdentity(2, 3UL * SegmentBytes + 5UL * (1 << 20)),
                History = { [2] = [1, 2, 3] },
                StartPolicy = (_, _) => Result.Failed(
                    new WalStartRefused("fake: requested WAL segment has already been removed")),
            },
            s3: s3);

        try
        {
            // Act/Assert — факт tli_jump в статус-ключе (живая публикация —
            // сессия после прыжка продолжает жить; прыжок до исхода сессии)
            var status = await WaitStatusAsync(
                etcd, s => s.TliJump is { Tli: 2 }, "tli_jump fact");
            status.TliJump!.From.Should().Be("000000010000000000000001");
            status.TliJump.To.Should().Be("000000020000000000000003");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    [Fact]
    public async Task EtcdFlap_WhileStreaming_StreamNotBroken()
    {
        // Arrange — полный путь до стрима, один сегмент доставлен
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var s3 = new FakeS3();
        var (loop, cts, sources) = StartLoop(
            etcd, WalReceiverRole.Master,
            () =>
            {
                var source = new FakeXLogSource();
                source.Chunks.Enqueue(Chunk(0));
                return source;
            },
            s3: s3);

        try
        {
            // Act — сессия стартовала и доставила сегмент ...000000
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.Streaming, "streaming");
            await WaitUntilAsync(
                () => sources.Count > 0 && sources[^1].ConfirmedLsn == EndLsn(0),
                Budget,
                () => $"keys=[{string.Join(",", s3.Objects.Keys)}]");
            var sessionCount = sources.Count;

            // Act — etcd-моргание (чтения гейтов отказывают) при живом стриме;
            // подкладываем сегмент ...000001
            etcd.FailReads = true;
            sources[^1].Chunks.Enqueue(Chunk((ulong)SegmentBytes));
            await WaitUntilAsync(
                () => sources[^1].ConfirmedLsn == EndLsn(1),
                Budget,
                () => $"confirmed={sources[^1].ConfirmedLsn} keys=[{string.Join(",", s3.Objects.Keys)}]");

            // Assert — стрим НЕ рвался циклом (новой сессии не заводилось:
            // гейты не читаются во время стрима — слот держит WAL, RPO не страдает)
            sources.Count.Should().Be(sessionCount, "the etcd flap must not break the live stream");
            var raw = etcd.Kvs[AgentKey];
            PgNodeAgentStatusJson.Parse(raw)!.Value.Status.State
                .Should().Be(WalReceiverState.Streaming);
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    // AAA (MERGE-публикация, spec §7 №8): прыжок в начале сессии + позже
    // исчерпание write-списка — в статус-ключе ОБА факта (tli_jump НЕ затёрт
    // pool-фактом; pool_exhausted НЕ затёрт прыжком); подъём ноды пула — флаг
    // снят, tli_jump персистентен до следующего прыжка.
    [Fact]
    public async Task LiveFacts_MergedInStatus_JumpNotErasedByPoolFlag()
    {
        // Arrange — хвост S3 на TLI 1; сервер промоутнулся на TLI 2 (58P01 →
        // jump-fallback на …03); сегмент …03 после прыжка отказывает put'ом
        // с маркером исчерпания
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var s3 = new FakeS3();
        SeedChain(s3, [0U, 1U]);
        s3.FailKeys.Add($"{Cluster}/{Shard}/wal/00000002{3 / WalFileName.SegsPerLog:x8}{3 % WalFileName.SegsPerLog:x8}");
        var (loop, cts, sources) = StartLoop(
            etcd, WalReceiverRole.Master,
            () =>
            {
                var source = new FakeXLogSource
                {
                    Identity = new ServerIdentity(2, 3UL * SegmentBytes + 5UL * (1 << 20)),
                    History = { [2] = [1, 2, 3] },
                    // Старты ДО прыжка (от хвоста 0/2000000) — 58P01; старт от
                    // границы прыжка 0/3000000 — успех: сессия стримит …03
                    StartPolicy = (startLsn, _) =>
                        startLsn == 3UL * SegmentBytes
                            ? Result.Success()
                            : Result.Failed(new WalStartRefused(
                                "fake: requested WAL segment has already been removed")),
                };
                // Чанки после прыжка: …03 (стрим от границы 0/3000000) и
                // граница …04 — закрытие …03 → put → маркер исчерпания
                source.Chunks.Enqueue(Chunk(3UL * SegmentBytes, tli: 2));
                source.Chunks.Enqueue(Chunk(4UL * SegmentBytes, tli: 2));
                return source;
            },
            s3: s3);

        try
        {
            // Act/Assert — ОБА факта в статусе: tli_jump жив, флаг исчерпания жив
            var merged = await WaitStatusAsync(
                etcd, s => s.TliJump is { Tli: 2 } && s.PoolExhausted,
                "tli_jump + pool_exhausted merged (neither erased)");

            // Act — подъём ноды пула: первый успешный put снимает флаг
            s3.FailKeys.Clear();
            await WaitStatusAsync(etcd, s => !s.PoolExhausted, "pool_exhausted=false");

            // Assert — tli_jump ПЕРСИСТЕНТЕН до следующего прыжка (флаг снят —
            // не событие снятия прыжка); факты сессии согласованы
            merged.TliJump!.From.Should().Be("000000010000000000000001");
            await WaitStatusAsync(
                etcd, s => !s.PoolExhausted && s.TliJump is { Tli: 2 },
                "flag cleared, jump persists");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    // AAA (arch/pgnode/03 §3): хвост S3 впереди текущей позиции записи сервера
    // (restore вернул LSN назад — сегменты откатанной ветки остались в wal/) —
    // старт от хвоста ЖИВОЙ ветки (CheckWithRestart): запрошенную точку сервер
    // не отдаёт (flush позади), retry-цикл стоял бы до его догона.
    [Fact]
    public async Task TailBeyondServerPosition_StartsFromLiveBranchTail()
    {
        // Arrange — архив: TLI 1 №0..15 (откатанная ветка) + TLI 3 №14 (живая);
        // сервер на TLI 3, позиция записи — внутри сегмента №15
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var s3 = new FakeS3();
        foreach (var seg in Enumerable.Range(0, 16))
            s3.Objects[WalKeyOf(1, seg)] = SegmentData(1, (ulong)seg * SegmentBytes);
        s3.Objects[WalKeyOf(3, 14)] = SegmentData(3, 14UL * SegmentBytes);
        var started = new TaskCompletionSource<ulong>(TaskCreationOptions.RunContinuationsAsynchronously);
        var (loop, cts, _) = StartLoop(etcd, WalReceiverRole.Master, () => new FakeXLogSource
        {
            Identity = new ServerIdentity(3, 15UL * SegmentBytes + 4096),
            StartPolicy = (startLsn, _) =>
            {
                started.TrySetResult(startLsn);
                return Result.Success();
            },
        }, s3: s3);
        try
        {
            // Act — стартовая позиция первой сессии
            var startLsn = await started.Task.WaitAsync(Budget, TestContext.Current.CancellationToken);

            // Assert — старт от конца живого TLI 3 №14 (0/0F000000), а не от
            // откатанного TLI 1 №15 (0/10000000, впереди позиции сервера)
            startLsn.Should().Be(15UL << 24);
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    // ── Ре-доклад инвентаря.history (spec §3.1 п.6, arch/19 §3) ──

    // tmpdir с pg_wal-файлом истории (имя — 8 hex + ".history", как пишет PG).
    private static string PgWalDirWith(params string[] names)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pgwal-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        foreach (var name in names)
            File.WriteAllText(Path.Combine(dir, name), "1\t0/20000060\n");
        return dir;
    }

    // AAA: файл есть в pg_wal, в S3 отсутствует → ре-доклад кладёт ключ
    // <C>/<X>/wal/<имя> (одиночный PutObjectAsync, идемпотентно).
    [Fact]
    public async Task RedeliverHistory_PutsMissingFile()
    {
        // Arrange — pg_wal с 00000002.history; S3 пуст (листинг — пустой список)
        var pgWalDir = PgWalDirWith("00000002.history");
        var s3 = new FakeS3();

        // Act — ре-доклад инвентаря
        await WalReceiverCore.RedeliverHistoryInventoryAsync(
            s3, Cluster, Shard, pgWalDir, null, TestContext.Current.CancellationToken);

        // Assert — ключ появился в S3
        s3.Objects.Should().ContainKey($"{Cluster}/{Shard}/wal/00000002.history",
            "a pg_wal history file missing from S3 must be redelivered");
        s3.PutCalls.Should().Be(1, "exactly one put for the single missing file");
    }

    // AAA: объект уже в S3 → дубль-put нет (идемпотентность по list).
    [Fact]
    public async Task RedeliverHistory_SkipsPresent_NoDuplicatePut()
    {
        // Arrange — файл в pg_wal И уже в S3
        var pgWalDir = PgWalDirWith("00000002.history");
        var s3 = new FakeS3();
        s3.Objects[$"{Cluster}/{Shard}/wal/00000002.history"] = [1, 2, 3];

        // Act — ре-доклад инвентаря
        await WalReceiverCore.RedeliverHistoryInventoryAsync(
            s3, Cluster, Shard, pgWalDir, null, TestContext.Current.CancellationToken);

        // Assert — ни одного put-вызова (объект уже в архиве)
        s3.PutCalls.Should().Be(0, "a present object must not be re-put");
        s3.Objects.Should().ContainKey($"{Cluster}/{Shard}/wal/00000002.history");
    }

    // AAA: отказ list → ни одного put; после «починки» листинга повторный
    // вызов (следующий цикл подключения) доносит файл.
    [Fact]
    public async Task RedeliverHistory_ListFails_DefersToNextCycle()
    {
        // Arrange — pg_wal с файлом; list wal/ отказывает устойчиво
        var pgWalDir = PgWalDirWith("00000002.history");
        var s3 = new FakeS3 { FailListWal = true };

        // Act/Assert — отказ list: ни одного put (ре-доклад отложен)
        await WalReceiverCore.RedeliverHistoryInventoryAsync(
            s3, Cluster, Shard, pgWalDir, null, TestContext.Current.CancellationToken);
        s3.PutCalls.Should().Be(0, "a failed listing defers the redelivery — no puts");

        // Act — листинг починен: следующий цикл подключения доносит файл
        s3.FailListWal = false;
        await WalReceiverCore.RedeliverHistoryInventoryAsync(
            s3, Cluster, Shard, pgWalDir, null, TestContext.Current.CancellationToken);

        // Assert — ключ появился
        s3.Objects.Should().ContainKey($"{Cluster}/{Shard}/wal/00000002.history",
            "the next connection cycle delivers the deferred file");
    }

    // AAA (интеграция цикла): WalReceiverLoop на фейках (роль master, слот,
    // пустой поток) с tmpdir-каталогом — объект в S3 без наблюдаемого перехода
    // в сессии (ре-доклад выполняется каждой итерацией цикла, дошедшей до сессии).
    [Fact]
    public async Task Loop_RedeliversHistoryInventory_EachCycle()
    {
        // Arrange — полный путь до сессии; pg_wal содержит 00000002.history;
        // поток пуст (переходов в сессии не наблюдается)
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var s3 = new FakeS3();
        var pgWalDir = PgWalDirWith("00000002.history");
        var (loop, cts, _) = StartLoop(
            etcd, WalReceiverRole.Master, () => new FakeXLogSource(), s3: s3, pgWalDir: pgWalDir);

        try
        {
            // Act/Assert — сессия стартовала; объект появился в S3 ре-докладом
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.Streaming, "streaming");
            await WaitUntilAsync(
                () => s3.Objects.ContainsKey($"{Cluster}/{Shard}/wal/00000002.history"),
                Budget,
                () => $"keys=[{string.Join(",", s3.Objects.Keys)}]");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    // ── Ре-доклад инвентаря СЕГМЕНТОВ (arch/19 §3) ──

    // tmpdir с закрытыми сегментными файлами штатного размера (разряжённые —
    // PG пишет имена ВЕРХНИМ hex; содержимое для ре-докала не важно).
    private static string PgWalDirWithSegments(params string[] names)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pgwal-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        foreach (var name in names)
        {
            using var fs = File.Create(Path.Combine(dir, name));
            fs.SetLength(SegmentBytes);
        }
        return dir;
    }

    // Имя ключа wal/ для TLI+позиции (нижний hex — канон архива).
    private static string WalKeyOf(uint tli, long pos)
        => $"{Cluster}/{Shard}/wal/{tli:x8}{pos / WalFileName.SegsPerLog:x8}{pos % WalFileName.SegsPerLog:x8}";

    // AAA: локальные закрытые сегменты выше хвоста архива доезжают в S3 строго
    // по возрастанию позиций; открытый (текущий) сегмент не грузится.
    [Fact]
    public async Task RedeliverWalInventory_AppendsClosedSegmentsInAscendingOrder()
    {
        // Arrange — архив TLI1 №0–2; локально №3, №4 (закрыты), №5 (текущий)
        var s3 = new FakeS3();
        SeedChain(s3, [0U, 1U, 2U]);
        var pgWalDir = PgWalDirWithSegments(
            "000000010000000000000003",
            "000000010000000000000004",
            "000000010000000000000005");
        var diagnostics = new List<string>();

        // Act — текущая позиция записи: начало сегмента №5
        await WalReceiverCore.RedeliverWalInventoryAsync(
            s3, Cluster, Shard, pgWalDir, currentTli: 1, currentLsn: 5UL * SegmentBytes,
            diagnostics.Add, TestContext.Current.CancellationToken);

        // Assert — ровно №3 и №4 в порядке возрастания; текущий №5 не тронут
        s3.PutOrder.Should().Equal([WalKeyOf(1, 3), WalKeyOf(1, 4)],
            "closed local segments above the S3 tail are appended in ascending order");
        s3.Objects.Should().NotContainKey(WalKeyOf(1, 5),
            "the open (current) segment is still being written — it must not be uploaded");
        diagnostics.Should().Contain(m => m.Contains("redelivered 2"));
    }

    // AAA: позиции не выше хвоста S3 не грузятся — сегмент точки переключения
    // уже лежит в архиве под именем РОДИТЕЛЯ (mixedness): дубль под именем
    // нового TLI дал бы две копии одной позиции.
    [Fact]
    public async Task RedeliverWalInventory_SwitchSegmentUnderParentName_NotDuplicated()
    {
        // Arrange — архив: TLI1 №9 → TLI1 №10 (mixed-сегмент точки переключения
        // под именем РОДИТЕЛЯ) + history нового TLI
        var s3 = new FakeS3();
        s3.Objects[WalKeyOf(1, 9)] = SegmentData(1, 9UL * (ulong)SegmentBytes);
        s3.Objects[WalKeyOf(1, 10)] = SegmentData(1, 10UL * (ulong)SegmentBytes);
        s3.Objects[$"{Cluster}/{Shard}/wal/00000002.history"] = [1, 2, 3];
        var pgWalDir = PgWalDirWithSegments(
            "00000002000000000000000A", // копия точки переключения под НОВЫМ TLI
            "00000002000000000000000B",
            "00000002000000000000000C");

        // Act — текущая позиция: начало сегмента TLI2 №13
        await WalReceiverCore.RedeliverWalInventoryAsync(
            s3, Cluster, Shard, pgWalDir, currentTli: 2, currentLsn: 13UL * SegmentBytes,
            null, TestContext.Current.CancellationToken);

        // Assert — позиция хвоста (10) не залита повторно; залиты 11 и 12
        s3.PutOrder.Should().Equal([WalKeyOf(2, 11), WalKeyOf(2, 12)]);
        s3.PutOrder.Should().NotContain(WalKeyOf(2, 10),
            "the switch-point segment stays archived under the parent TLI name only");
    }

    // AAA: обрыв локальной цепи останавливает проход — сегменты выше дыры не
    // заливаются (архив не перепрыгивает позиции); следующий цикл продолжит.
    [Fact]
    public async Task RedeliverWalInventory_LocalGap_StopsBeforeTheGap()
    {
        // Arrange — архив №0–2; локально №3 есть, №4 отсутствует, №5 есть
        var s3 = new FakeS3();
        SeedChain(s3, [0U, 1U, 2U]);
        var pgWalDir = PgWalDirWithSegments(
            "000000010000000000000003",
            "000000010000000000000005");

        // Act
        await WalReceiverCore.RedeliverWalInventoryAsync(
            s3, Cluster, Shard, pgWalDir, currentTli: 1, currentLsn: 6UL * SegmentBytes,
            null, TestContext.Current.CancellationToken);

        // Assert — только №3; №5 ждёт заполнения дыры
        s3.PutOrder.Should().Equal([WalKeyOf(1, 3)],
            "a local gap stops the pass — no segment above the hole is uploaded");
    }

    // AAA: несколько TLI на одну позицию — грузится НОВЕЙШИЙ (живая ветка ноды).
    [Fact]
    public async Task RedeliverWalInventory_SamePosition_PrefersNewestTimeline()
    {
        // Arrange — архив TLI1 №0–2; локально TLI2 №3 и TLI4 №3, затем TLI4 №4
        var s3 = new FakeS3();
        SeedChain(s3, [0U, 1U, 2U]);
        var pgWalDir = PgWalDirWithSegments(
            "000000020000000000000003",
            "000000040000000000000003",
            "000000040000000000000004");

        // Act
        await WalReceiverCore.RedeliverWalInventoryAsync(
            s3, Cluster, Shard, pgWalDir, currentTli: 4, currentLsn: 5UL * SegmentBytes,
            null, TestContext.Current.CancellationToken);

        // Assert — позиция 3 залита новейшим TLI (4), позиция 4 — тоже TLI4
        s3.PutOrder.Should().Equal(WalKeyOf(4, 3), WalKeyOf(4, 4));
    }

    // AAA (восстановленный шард): в АРХИВЕ позиция занята ДВУМЯ именами —
    // сегменты откатанной ветки (TLI 1) остались в wal/, живая ветка (TLI 3)
    // пишет ту же позицию своим именем. Построение карты позиций бросало
    // duplicate key и убивало проход ре-докала на весь цикл (статус получал
    // ложный факт s3-pool-exhausted): хвост — живая ветка, её закрытый
    // локальный сегмент доносится.
    [Fact]
    public async Task RedeliverWalInventory_SamePositionTwoBranches_NewestTliTail()
    {
        // Arrange — архив: TLI1 №14 (откатанная ветка) + TLI3 №14 (живая);
        // локально закрыт TLI3 №15, текущий №16
        var s3 = new FakeS3();
        s3.Objects[WalKeyOf(1, 14)] = SegmentData(1, 14UL * (ulong)SegmentBytes);
        s3.Objects[WalKeyOf(3, 14)] = SegmentData(3, 14UL * (ulong)SegmentBytes);
        var pgWalDir = PgWalDirWithSegments("00000003000000000000000F");

        // Act
        await WalReceiverCore.RedeliverWalInventoryAsync(
            s3, Cluster, Shard, pgWalDir, currentTli: 3, currentLsn: 16UL * SegmentBytes,
            null, TestContext.Current.CancellationToken);

        // Assert — проход не упал: закрытый сегмент живой ветки доехал
        s3.PutOrder.Should().Equal(WalKeyOf(3, 15));
    }

    // AAA: отказ put останавливает проход на этой позиции (порядок не
    // ломается); следующий цикл подключения продолжает с неё.
    [Fact]
    public async Task RedeliverWalInventory_PutFailure_DefersToNextCycle()
    {
        // Arrange — архив №0; локально №1 и №2; put №2 отказывает
        var s3 = new FakeS3();
        SeedChain(s3, [0U]);
        var pgWalDir = PgWalDirWithSegments(
            "000000010000000000000001",
            "000000010000000000000002");
        s3.FailKeys.Add(WalKeyOf(1, 2));

        // Act/Assert — доехал только №1, проход остановлен
        await WalReceiverCore.RedeliverWalInventoryAsync(
            s3, Cluster, Shard, pgWalDir, currentTli: 1, currentLsn: 3UL * SegmentBytes,
            null, TestContext.Current.CancellationToken);
        s3.PutOrder.Should().Equal(WalKeyOf(1, 1));
        s3.Objects.Should().NotContainKey(WalKeyOf(1, 2));

        // Act — put починен: следующий цикл доносит №2
        s3.FailKeys.Clear();
        await WalReceiverCore.RedeliverWalInventoryAsync(
            s3, Cluster, Shard, pgWalDir, currentTli: 1, currentLsn: 3UL * SegmentBytes,
            null, TestContext.Current.CancellationToken);

        // Assert — хвост вырос до №2
        s3.PutOrder.Should().Equal(WalKeyOf(1, 1), WalKeyOf(1, 2));
    }

    // AAA: пустой префикс архива — продолжать нечего (цепочку закрепит стрим).
    [Fact]
    public async Task RedeliverWalInventory_EmptyPrefix_NoOp()
    {
        // Arrange — S3 пуст; локально есть закрытый сегмент
        var s3 = new FakeS3();
        var pgWalDir = PgWalDirWithSegments("000000010000000000000003");

        // Act
        await WalReceiverCore.RedeliverWalInventoryAsync(
            s3, Cluster, Shard, pgWalDir, currentTli: 1, currentLsn: 4UL * SegmentBytes,
            null, TestContext.Current.CancellationToken);

        // Assert — ни одного put (истина старта — restart_lsn слота)
        s3.PutCalls.Should().Be(0);
    }

    // AAA (arch/19 §3, граница собственного прогресса): сегмент ЕСТЬ в архиве
    // (чужой соло-хвост 0007 — доставлен другим источником двойной архивации),
    // но доставлен НЕ этой нодой (свой delivered 0006) — локально закрытые
    // 0007/0008 ДОНОСЯТСЯ на write-цель: идемпотентный повтор того же имени,
    // «числится на чужой ноде» не значит «доставлено мной».
    [Fact]
    public async Task RedeliverWalInventory_OwnDeliveredBelowForeignTail_DeliversAboveOwnProgress()
    {
        // Arrange — архив 0–7 (чужой хвост 7); свой delivered = позиция 6;
        // локально закрыты 7 и 8, текущий 9
        var s3 = new FakeS3();
        SeedChain(s3, [0U, 1U, 2U, 3U, 4U, 5U, 6U, 7U]);
        var pgWalDir = PgWalDirWithSegments(
            "000000010000000000000007",
            "000000010000000000000008",
            "000000010000000000000009");
        var diagnostics = new List<string>();

        // Act
        await WalReceiverCore.RedeliverWalInventoryAsync(
            s3, Cluster, Shard, pgWalDir, currentTli: 1, currentLsn: 9UL * SegmentBytes,
            diagnostics.Add, TestContext.Current.CancellationToken,
            deliveredPosition: 6);

        // Assert — 0007 донесён (идемпотентный дубль чужой копии), 0008 за ним
        s3.PutOrder.Should().Equal([WalKeyOf(1, 7), WalKeyOf(1, 8)],
            "segments between own delivered progress and the foreign tail must be re-delivered");
        diagnostics.Should().Contain(m => m.Contains("redelivered 2"));
    }

    // AAA (mixedness, arch/19 §3): позиция границы+1 уже заархивирована ДРУГИМ
    // именем (tli-ветвление: точка переключения под именем родителя) — проход
    // останавливается БЕЗ второй копии позиции.
    [Fact]
    public async Task RedeliverWalInventory_TliBranchedPosition_StopsWithoutSecondCopy()
    {
        // Arrange — архив: TLI1 0006 + TLI1 0007 (parent-имя); свой delivered
        // TLI1/0006; локально ветка TLI2: 0007, 0008
        var s3 = new FakeS3();
        s3.Objects[WalKeyOf(1, 6)] = SegmentData(1, 6UL * (ulong)SegmentBytes);
        s3.Objects[WalKeyOf(1, 7)] = SegmentData(1, 7UL * (ulong)SegmentBytes);
        var pgWalDir = PgWalDirWithSegments(
            "000000020000000000000007",
            "000000020000000000000008");
        var diagnostics = new List<string>();

        // Act
        await WalReceiverCore.RedeliverWalInventoryAsync(
            s3, Cluster, Shard, pgWalDir, currentTli: 2, currentLsn: 9UL * SegmentBytes,
            diagnostics.Add, TestContext.Current.CancellationToken,
            deliveredPosition: 6);

        // Assert — позиция 7 уже лежит другим именем: ни одного put
        s3.PutCalls.Should().Be(0);
        diagnostics.Should().Contain(m => m.Contains("tli branch point"));
    }

    // AAA (arch/19 §3 «Возобновление»): старт сессии НЕ выше собственного
    // доставленного прогресса — соло-хвост чужой ноды (0007 в архиве) не
    // переводит сессию через свой недоставленный сегмент: сессия B стартует от
    // своего delivered 0001 и доносит 0002..0008 (повтор 0007 идемпотентен).
    [Fact]
    public async Task SessionStart_ClampedToOwnDelivered_WhenForeignTailAhead()
    {
        // Arrange — сессия A доставляет сегменты 0 и 1
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var s3 = new FakeS3();
        var second = false;
        var (loop, cts, sources) = StartLoop(
            etcd, WalReceiverRole.Master,
            () =>
            {
                if (!second)
                {
                    second = true;
                    var a = new FakeXLogSource();
                    a.Chunks.Enqueue(Chunk(0));
                    a.Chunks.Enqueue(Chunk(SegmentBytes));
                    return a;
                }

                // Сессия B — сегменты 2..8 (полный перезаказ у источника)
                var b = new FakeXLogSource();
                for (var boundary = 2UL * SegmentBytes; boundary <= 8UL * SegmentBytes; boundary += SegmentBytes)
                    b.Chunks.Enqueue(Chunk(boundary));
                return b;
            },
            s3: s3);

        try
        {
            // Act 1 — сессия A доставила 0/1; стоп-гейт чисто завершает её
            await WaitUntilAsync(
                () => s3.Objects.ContainsKey(WalKeyOf(1, 1)),
                Budget,
                () => $"keys=[{string.Join(",", s3.Objects.Keys)}]");
            etcd.Kvs[WalKey] = $$"""{"cluster":"{{Cluster}}","shard":"{{Shard}}","state":"STOPPED"}""";
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.Paused, "paused (session A torn)");

            // Arrange 2 — «чужая» доставка: хвост архива уходит выше своего
            // delivered (0007 доставлен другим источником двойной архивации)
            s3.Objects[WalKeyOf(1, 7)] = SegmentData(1, 7UL * (ulong)SegmentBytes);
            etcd.Kvs.Remove(WalKey);

            // Act 2 — сессия B: хвост 0007, свой delivered 0001 → старт 0002+0
            await WaitUntilAsync(
                () => s3.Objects.ContainsKey(WalKeyOf(1, 8)),
                Budget,
                () => $"keys=[{string.Join(",", s3.Objects.Keys)}]");

            // Assert — клэмп: сессия доносит СВОЙ недоставленный диапазон
            // (без клэмпа стартовала бы с 0008+0 и 0002..0007 не заливались)
            s3.PutOrder.Should().Contain([WalKeyOf(1, 2), WalKeyOf(1, 6), WalKeyOf(1, 7)],
                "the session re-delivers its own undelivered range below the foreign tail");
            sources[^1].ConfirmedLsn.Should().Be(EndLsn(8), "the slot is confirmed at the last delivered segment");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    // AAA (arch/19 §3, повтор инвентаря в живой сессии): сегмент закрылся
    // локально ПОСЛЕ старта сессии — периодический инвентарный проход доносит
    // его БЕЗ переподключения (сессия жива: один источник на всём тесте).
    [Fact]
    public async Task InventoryRepeatsInLivingSession_ClosesGapFromLocallyClosedSegment()
    {
        // Arrange — живой idle-стрим после доставки сегмента 0; IDENTIFY
        // отдаёт позицию записи выше (сегмент 10 — «текущий»)
        var dir = PgWalDirWithSegments();
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var s3 = new FakeS3();
        var (loop, cts, sources) = StartLoop(
            etcd, WalReceiverRole.Master,
            () =>
            {
                var source = new FakeXLogSource
                {
                    Identity = new(1, 10UL * SegmentBytes),
                };
                source.Chunks.Enqueue(Chunk(0));
                return source;
            },
            s3: s3,
            pgWalDir: dir,
            inventoryPeriod: TimeSpan.FromMilliseconds(300));

        try
        {
            // Act 1 — сессия доставила сегмент 0 (confirmedBase = конец 0)
            await WaitUntilAsync(
                () => s3.Objects.ContainsKey(WalKeyOf(1, 0)),
                Budget,
                () => $"keys=[{string.Join(",", s3.Objects.Keys)}]");

            // Act 2 — сегмент 1 закрылся локально ПОСЛЕ старта сессии
            using (var fs = File.Create(Path.Combine(dir, "000000010000000000000001")))
                fs.SetLength(SegmentBytes);

            // Assert — повторный инвентарный проход донёс 0001; сессия не
            // переподключалась (единственный источник)
            await WaitUntilAsync(
                () => s3.Objects.ContainsKey(WalKeyOf(1, 1)),
                Budget,
                () => $"keys=[{string.Join(",", s3.Objects.Keys)}]");
            sources.Should().HaveCount(1, "the living session delivers the late-closed segment without reconnect");
            s3.PutOrder.Should().Contain(WalKeyOf(1, 1));
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    // AAA (интеграция цикла): каждая итерация, дошедшая до сессии, доносит
    // локальные закрытые сегменты выше хвоста S3 (вне hot-path стрима).
    [Fact]
    public async Task Loop_RedeliversWalInventory_EachCycle()
    {
        // Arrange — полный путь до сессии; архив №0–1; локально №2, №3 (закрыты)
        var etcd = new FakeEtcd();
        SeedHappyPath(etcd);
        var s3 = new FakeS3();
        SeedChain(s3, [0U, 1U]);
        var pgWalDir = PgWalDirWithSegments(
            "000000010000000000000002",
            "000000010000000000000003");
        var (loop, cts, _) = StartLoop(
            etcd, WalReceiverRole.Master,
            () => new FakeXLogSource { Identity = new ServerIdentity(1, 4UL * SegmentBytes) },
            s3: s3, pgWalDir: pgWalDir);

        try
        {
            // Act/Assert — сессия стартовала; сегменты появились в S3
            await WaitStatusAsync(etcd, s => s.State == WalReceiverState.Streaming, "streaming");
            await WaitUntilAsync(
                () => s3.Objects.ContainsKey(WalKeyOf(1, 3)),
                Budget,
                () => $"keys=[{string.Join(",", s3.Objects.Keys)}]");
        }
        finally
        {
            await StopLoopAsync(loop, cts);
        }
    }

    // ---: env-парсер компакции дельт (t15, spec §4.2) ----

    // AAA: env PGW_WAL_PARTIAL_COMPACT_INTERVAL_SEC — значение ≥60 применяется,
    // мусор/отсутствие/<60 — дефолт 3600 (клэмп приёмника ≥60).
    [Theory]
    [InlineData("120", 120)]
    [InlineData("", 3600)]
    [InlineData("abc", 3600)]
    [InlineData("59", 3600)]
    [InlineData("3600", 3600)]
    public void ReadPartialCompactIntervalSec_EnvParsing(string raw, int expected)
    {
        // Arrange
        var previous = Environment.GetEnvironmentVariable("PGW_WAL_PARTIAL_COMPACT_INTERVAL_SEC");
        Environment.SetEnvironmentVariable("PGW_WAL_PARTIAL_COMPACT_INTERVAL_SEC", raw);

        try
        {
            // Act
            var sec = WalReceiverLoop.ReadPartialCompactIntervalSec();

            // Assert
            sec.Should().Be(expected);
        }
        finally
        {
            Environment.SetEnvironmentVariable("PGW_WAL_PARTIAL_COMPACT_INTERVAL_SEC", previous);
        }
    }
}
