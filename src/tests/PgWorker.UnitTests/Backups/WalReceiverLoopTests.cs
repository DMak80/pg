using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Etcd.S3;
using PgWorker.WalReceiver;
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
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(
            string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
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
        TimeSpan? probePeriod = null)
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
            FastStep,
            _ => s3Fake,
            FastKeepalive,
            pgWalDir,
            roleProbePeriod: probePeriod);
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
    private static async Task<WalReceiverAgentStatus> WaitStatusAsync(
        FakeEtcd etcd, Func<WalReceiverAgentStatus, bool> match, string debug)
    {
        var deadline = DateTime.UtcNow + Budget;
        while (DateTime.UtcNow < deadline)
        {
            if (etcd.Kvs.TryGetValue(AgentKey, out var raw))
            {
                var parsed = WalReceiverAgentStatusJson.Parse(raw);
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
            WalReceiverAgentStatusJson.Parse(raw)!.Value.Status.State
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
}
