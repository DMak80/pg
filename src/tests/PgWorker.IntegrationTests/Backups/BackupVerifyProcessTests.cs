using System.Text.Json;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции BackupVerifyProcess (spec Ф3): у КАЖДОГО Fact своё etcd-
// окружение OwnEtcd (guid-имя pgw-ee-*, динамический порт, own-only teardown
// с ассертом чистоты — docs/e2e-isolation.md §1/§3; ключи умирают с контейнером)
// + фейки S3 и API приёмника нод (verify-операции исполняет WalReceiver ноды-
// источника полного — docker-движка в verify-процессе больше нет).
[Collection(NonE2eCollection.Name)]
public class BackupVerifyProcessTests
{
    // Окружение Fact'а (свой etcd); создаётся в начале каждого сценария.
    private OwnEtcd Fx = null!;

    // ClaimStore ОДИН на окружение: InstanceId фиксируется клэймом SeedAsync —
    // новый экземпляр на вызов давал бы чужой InstanceId в тике (guard «клэйм не наш»).
    private ClaimStore? _claimsStore;

    private ClaimStore Claims => _claimsStore ??= new("/pgworker", [Fx.Endpoint], Fx.Gateway, TimeProvider.System);

    // ── (arch/19 §2 «Наблюдаемость итога verify»): spy-логгер — снимает
    //    форматированные строки (итог verify наблюдаем из host-лога воркера) ──
    internal sealed class SpyLogger : Microsoft.Extensions.Logging.ILogger<BackupVerifyProcess>
    {
        public readonly List<string> Lines = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
            TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Lines.Add(formatter(state, exception));
    }

    // ── Сид-хелперы ──
    private static ClusterSnapshot BuildSnap(string cluster) => new(
        new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
        [new ShardSpec("shard1", 1, $"host=shard1a dbname={cluster}", "shard1a:17001",
            [new NodeSpec("shard1", "shard1a", NodeState.Running)])],
        []);

    private async Task SeedAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        await Fx.Gateway.DeleteAsync(Fx.Endpoint, $"/pgworker/backups/{cluster}/", prefix: true, ct);
        await Fx.Gateway.DeleteAsync(Fx.Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        await Fx.Gateway.PutAsync(Fx.Endpoint, $"/pgworker/portalloc/{cluster}",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("h1", new NodePorts(16001, 18001, 17001)),
            }), null, ct);
        (await Claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue("claim is a tick precondition");
    }

    // Фейк API приёмника: ответы GetVerify/StartVerify программируются тестами.
    private readonly FakeReceiverClient Receiver = new();

    // Ответы GET /api/ops/verify (маркер транзиента — пара state/phase, spec §3.2).
    private static ReceiverReply ApiOk() =>
        new(200, new ReceiverStatus("succeeded", "verifying"), null);
    private static ReceiverReply ApiDownloadTransient(string error) =>
        new(200, new ReceiverStatus("failed", "downloading", Error: error), null);
    private static ReceiverReply ApiVerifyFailed(string error) =>
        new(200, new ReceiverStatus("failed", "verifying", Error: error), null);

    /// <summary>Мини-фейк API приёмника (verify-сценарии): StartVerify —
    /// программируемый ответ (дефолт 202), GetVerify — программируемый снимок,
    /// CancelOps — журнал вызовов (ветка возрастного бюджета).</summary>
    private sealed class FakeReceiverClient : IWalReceiverApiClient
    {
        public ReceiverReply StartReply { get; set; } =
            new(202, new ReceiverStatus("running", "downloading"), null);
        public ReceiverReply GetReply { get; set; } =
            new(200, new ReceiverStatus("running", "downloading"), null);
        public readonly List<StartVerifyRequest> StartBodies = [];
        public readonly List<string> CancelUrls = [];

        public Task<ReceiverReply> StartVerifyAsync(string baseUrl, StartVerifyRequest body, CancellationToken ct)
        {
            StartBodies.Add(body);
            return Task.FromResult(StartReply);
        }

        public Task<ReceiverReply> GetVerifyAsync(string baseUrl, CancellationToken ct)
            => Task.FromResult(GetReply);

        public Task<ReceiverReply> CancelOpsAsync(string baseUrl, CancellationToken ct)
        {
            CancelUrls.Add(baseUrl);
            return Task.FromResult(new ReceiverReply(202, null, null));
        }

        public Task<ReceiverReply> StartFullAsync(string baseUrl, string id, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> GetFullAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> CancelFullAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> StartRestoreAsync(string baseUrl, StartRestoreRequest body, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> GetRestoreAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<NodeReply<HbaEnsureResponse>> EnsureHbaAsync(string baseUrl, CancellationToken ct)
            => Task.FromResult(new NodeReply<HbaEnsureResponse>(200, new HbaEnsureResponse(true, false), null));
        public Task<NodeReply<PgDumpResponse>> PgDumpAsync(string baseUrl, StartPgDumpRequest body, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<NodeReply<PgDataResponse>> GetPgDataAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
    }

    private BackupVerifyProcess BuildProcess(
        string cluster, IBackupS3 s3, BackupsRuntimeOptions? options = null,
        Action<string, string, string>? verifyObserver = null,
        Microsoft.Extensions.Logging.ILogger<BackupVerifyProcess>? logger = null)
        => new(
            Fx.Gateway, [Fx.Endpoint],
            new ShardEndpoints(Fx.Gateway, [Fx.Endpoint], new ShardProbe(new HttpClient())),
            FakePoolFactory.Single(s3), Receiver, Claims,
            new WorkJournal("/pgworker", Fx.Gateway, [Fx.Endpoint]),
            options ?? new BackupsRuntimeOptions(
                Enabled: true, S3Bucket: "bkt",
                StagingDir: "/home/postgres/pgdata/backup-staging"),
            TimeProvider.System,
            logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<BackupVerifyProcess>.Instance,
            verifyObserver);

    private static IReadOnlyList<ClusterBackups> BackupsOf(string cluster, params FullBackupState[] fulls)
        => [new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new(fulls, null) })];

    private static FullBackupState Completed(string id, string? walStart, BackupVerify? verify = null) =>
        new(id, FullBackupStatus.Completed, "shard1a", BackupSourceRole.Replica,
            1757500000, 1757500300, walStart, 1024, null, verify);

    private async Task<JsonElement> ReadVerifyAsync(string cluster, string id)
    {
        var kv = await Fx.Gateway.GetAsync(
            Fx.Endpoint, $"/pgworker/backups/{cluster}/shard1/full/{id}",
            TestContext.Current.CancellationToken);
        kv.Value.Should().NotBeNull("the verify outcome is written to the full backup key");
        return JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!["verify"];
    }

    // AAA: PENDING-кандидат при on_create — цепочка цела → GET 404 → POST
    // запускает verify-операцию на приёмнике ноды-источника + journal started
    // (AC1; секреты — только в POST-теле)
    [Fact]
    public async Task TickPendingCandidateIntactChainStartsOperation()
    {
        // Arrange — full COMPLETED verify=PENDING; wal/: сегменты 1..3 + history нет;
        // pg_wal набора: сегмент 3 (end); приёмник отвечает 404 (операции нет)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc1");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc1", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc1", "shard1", "000000010000000000000002"));
        s3.Objects.Add(("vc1", "shard1", "000000010000000000000003"));
        s3.PrefixedObjects.Add(("vc1", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000003"));
        Receiver.GetReply = new ReceiverReply(404, null, null);
        var process = BuildProcess("vc1", s3);
        var backups = BackupsOf("vc1", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        var result = await process.TickAsync(BuildSnap("vc1"), backups, ct);

        // Assert — POST ушёл на приёмник ноды (id полного, префикс кластер/шард,
        // бакет опций); запуск зафиксирован журналом
        result.IsSuccess.Should().BeTrue();
        var body = Receiver.StartBodies.Should().ContainSingle().Subject;
        body.BackupId.Should().Be("20260911120000Z");
        body.S3.Prefix.Should().Be("vc1/shard1");
        body.S3.Bucket.Should().Be("bkt");
        var journal = await Fx.Gateway.GetAsync(Fx.Endpoint, "/pgworker/work/vc1", ct);
        journal.Value!.Value.Should().Contain("started/shard1/20260911120000Z");
    }

    // AAA: отказ POST запуска (не 202/409) — transient: попытка запуска была,
    // статус кандидата не тронут (тик повторит)
    [Fact]
    public async Task TickStartPostRefusedTransientStatusUntouched()
    {
        // Arrange — GET 404 (операции нет), POST отвечает 500; ключ кандидата
        // в etcd записан руками (ассерт «не изменён»)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc9");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc9", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("vc9", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000001"));
        Receiver.GetReply = new ReceiverReply(404, null, null);
        Receiver.StartReply = new ReceiverReply(500, null, "receiver busy");
        var candidate = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var key = "/pgworker/backups/vc9/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(candidate);
        await Fx.Gateway.PutAsync(Fx.Endpoint, key, before, null, ct);
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess("vc9", s3, verifyObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act
        (await process.TickAsync(BuildSnap("vc9"), BackupsOf("vc9", candidate), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — POST был (один), статус PENDING не тронут, observe transient
        Receiver.StartBodies.Should().ContainSingle().Which.BackupId.Should().Be("20260911120000Z");
        var after = (await Fx.Gateway.GetAsync(Fx.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "a refused start is transient — the PENDING status is untouched");
        outcomes.Should().ContainSingle().Which.Should().Be(("vc9", "shard1", "transient"));
    }

    // AAA: дыра в wal/ внутри [start..end] → permanent FAILED с границами,
    // verify-операция НЕ запускается (AC3: до приёмника дело не доходит)
    [Fact]
    public async Task TickChainGapFailedWithoutOperation()
    {
        // Arrange — wal/: 1,3 (нет 2); набор: pg_wal/3 → end=3
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc2");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc2", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc2", "shard1", "000000010000000000000003"));
        s3.PrefixedObjects.Add(("vc2", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000003"));
        var process = BuildProcess("vc2", s3);
        var backups = BackupsOf("vc2", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        (await process.TickAsync(BuildSnap("vc2"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — verify.state=FAILED + error с границами + checked_unix; запуска нет
        var verify = await ReadVerifyAsync("vc2", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("error").GetString().Should().Contain("000000010000000000000002");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);
        Receiver.StartBodies.Should().BeEmpty("the verdict is determined by the chain — no download is spent (spec §3.1)");
    }

    // AAA: COMPLETED без wal_start_segment (аномалия) — permanent FAILED без запуска
    [Fact]
    public async Task TickNoWalStartFailedWithoutOperation()
    {
        // Arrange — wal_start_segment = null (ранний упавший UPLOADING не бывает
        // COMPLETED — аномалия; но guard обязателен, spec §3.7)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc3");
        var process = BuildProcess("vc3", new FakeBackupS3());
        var backups = BackupsOf("vc3", Completed("20260911120000Z", null,
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        (await process.TickAsync(BuildSnap("vc3"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert
        var verify = await ReadVerifyAsync("vc3", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("error").GetString().Should().Contain("wal_start_segment");
        Receiver.StartBodies.Should().BeEmpty();
    }

    // AAA: строгий TLI-переход с валидным switchWALLSN (GET history из S3) —
    // цепочка цела, verify-операция запускается (AC4: verify использует содержимое history)
    [Fact]
    public async Task TickStrictTliValidPointStartsOperation()
    {
        // Arrange — wal/: tli1/seg1, history, tli2/seg2; набор: pg_wal/tli2/seg2;
        // приёмник отвечает 404 (операции нет)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc4");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc4", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc4", "shard1", "00000002.history"));
        s3.Objects.Add(("vc4", "shard1", "000000020000000000000002"));
        s3.PrefixedObjects.Add(("vc4", "shard1", "full/20260911120000Z/pg_wal/000000020000000000000002"));
        s3.Contents[("vc4", "shard1", "wal/00000002.history")] = "1\t0/2000000\n";
        Receiver.GetReply = new ReceiverReply(404, null, null);
        var process = BuildProcess("vc4", s3);
        var backups = BackupsOf("vc4", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        (await process.TickAsync(BuildSnap("vc4"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert
        Receiver.StartBodies.Should().ContainSingle().Which.BackupId.Should().Be("20260911120000Z");
    }

    // AAA: строгий TLI-переход с НЕвалидной точкой (switchWALLSN вне границы) —
    // FAILED с упоминанием history, без запуска (AC4)
    [Fact]
    public async Task TickStrictTliPointOffBoundaryFailed()
    {
        // Arrange — как vc4, но history говорит parent=1, lsn=0/5000000 (сегмент 5)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc5");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc5", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc5", "shard1", "00000002.history"));
        s3.Objects.Add(("vc5", "shard1", "000000020000000000000002"));
        s3.PrefixedObjects.Add(("vc5", "shard1", "full/20260911120000Z/pg_wal/000000020000000000000002"));
        s3.Contents[("vc5", "shard1", "wal/00000002.history")] = "1\t0/5000000\n";
        var process = BuildProcess("vc5", s3);
        var backups = BackupsOf("vc5", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        (await process.TickAsync(BuildSnap("vc5"), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert
        var verify = await ReadVerifyAsync("vc5", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("error").GetString().Should().Contain("00000002.history");
        Receiver.StartBodies.Should().BeEmpty();
    }

    // AAA: transient S3 (list wal/ недоступен) — шард-skip тиком: тик успешен,
    // операция не запускается, ключ полного НЕ изменён (spec §3.1: статус не трогаем)
    [Fact]
    public async Task TickTransientS3StatusUntouched()
    {
        // Arrange — PENDING-ключ в etcd (записан руками — ассерт «не изменён»);
        // S3 «лежит» (FakeBackupS3.Fails)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc6");
        var s3 = new FakeBackupS3 { Fails = true };
        var candidate = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var key = $"/pgworker/backups/vc6/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(candidate);
        await Fx.Gateway.PutAsync(Fx.Endpoint, key, before, null, ct);
        var process = BuildProcess("vc6", s3);
        var backups = BackupsOf("vc6", candidate);

        // Act
        var result = await process.TickAsync(BuildSnap("vc6"), backups, ct);

        // Assert — transient: тик без ошибки (шард-skip), ничего не запущено/не записано
        result.IsSuccess.Should().BeTrue("transient S3 — not a tick error (spec §3.1)");
        Receiver.StartBodies.Should().BeEmpty("an operation without a chain does not start");
        var after = (await Fx.Gateway.GetAsync(Fx.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "the PENDING status is untouched — retry on the next tick");
    }

    // AAA: transient GET history (spec §3.1): цепочка с TLI-переходом требует
    // скачивания .history → GET падает → шард-skip, запуска нет, статус не тронут
    [Fact]
    public async Task TickTransientGetHistoryStatusUntouched()
    {
        // Arrange — сид как в vc4 (TLI-переход в диапазоне), но GET падает;
        // PENDING-ключ в etcd для ассерта «не изменён»
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc7");
        var s3 = new FakeBackupS3 { FailsGetObject = true };
        s3.Objects.Add(("vc7", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("vc7", "shard1", "00000002.history"));
        s3.Objects.Add(("vc7", "shard1", "000000020000000000000002"));
        s3.PrefixedObjects.Add(("vc7", "shard1", "full/20260911120000Z/pg_wal/000000020000000000000002"));
        s3.Contents[("vc7", "shard1", "wal/00000002.history")] = "1\t0/2000000\n";
        var candidate = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var key = "/pgworker/backups/vc7/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(candidate);
        await Fx.Gateway.PutAsync(Fx.Endpoint, key, before, null, ct);
        var process = BuildProcess("vc7", s3);

        // Act
        var result = await process.TickAsync(BuildSnap("vc7"), BackupsOf("vc7", candidate), ct);

        // Assert — list прошёл (переход в диапазоне найден), GET упал → transient
        result.IsSuccess.Should().BeTrue("transient GET — not a tick error (spec §3.1)");
        Receiver.StartBodies.Should().BeEmpty("without the history content the strict parse is impossible — the operation does not start");
        var after = (await Fx.Gateway.GetAsync(Fx.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "the PENDING status is untouched — retry on the next tick");
    }

    // Локальный хелпер: PENDING-кандидат + непрерывная цепочка 1..3; первый тик
    // (GET 404 → POST 202) запускает verify-операцию — precondition супервиз-тестов.
    private async Task StartOperationAsync(string cluster, FakeBackupS3 s3, string id = "20260911120000Z")
    {
        s3.Objects.Add((cluster, "shard1", "000000010000000000000001"));
        s3.Objects.Add((cluster, "shard1", "000000010000000000000002"));
        s3.Objects.Add((cluster, "shard1", "000000010000000000000003"));
        s3.PrefixedObjects.Add((cluster, "shard1", $"full/{id}/pg_wal/000000010000000000000003"));
        Receiver.GetReply = new ReceiverReply(404, null, null);
        var process = BuildProcess(cluster, s3);
        var backups = BackupsOf(cluster, Completed(id, "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));
        (await process.TickAsync(BuildSnap(cluster), backups, TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        Receiver.StartBodies.Should().ContainSingle(b => b.BackupId == id,
            "the operation is started by the first tick (precondition)");
    }

    // AAA: GET succeeded → verify OK + checked_unix + journal verified-ok (AC1)
    [Fact]
    public async Task SupervisorSucceededWritesOkVerdict()
    {
        // Arrange — операция запущена первым тиком; приёмник отдал succeeded
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv1");
        var s3 = new FakeBackupS3();
        await StartOperationAsync("sv1", s3);
        Receiver.GetReply = ApiOk();
        var process = BuildProcess("sv1", s3);

        // Act — второй тик (поллинг GET /api/ops/verify)
        (await process.TickAsync(BuildSnap("sv1"), BackupsOf("sv1",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — OK записан ключом, журнал несёт verified-ok
        var verify = await ReadVerifyAsync("sv1", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("OK");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);
        var journal = await Fx.Gateway.GetAsync(Fx.Endpoint, "/pgworker/work/sv1", ct);
        journal.Value!.Value.Should().Contain("verified-ok/shard1/20260911120000Z");
    }

    // AAA: snapshot failed + phase=verifying → permanent FAILED с error от
    // pg_verifybackup (AC2)
    [Fact]
    public async Task SupervisorVerifyPhaseFailedFailed()
    {
        // Arrange — операция запущена; приёмник отдал failed+verifying
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv2");
        var s3 = new FakeBackupS3();
        await StartOperationAsync("sv2", s3);
        Receiver.GetReply = ApiVerifyFailed("checksum mismatch failed");
        var process = BuildProcess("sv2", s3);

        // Act — поллинг GET
        (await process.TickAsync(BuildSnap("sv2"), BackupsOf("sv2",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert
        var verify = await ReadVerifyAsync("sv2", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("FAILED");
        verify.GetProperty("error").GetString().Should().Contain("checksum mismatch");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);
    }

    // AAA: snapshot failed + phase=downloading — transient: статус ОСТАЛСЯ
    // PENDING (AC2); следующий тик (приёмник снял терминальный снимок — GET 404)
    // запускает операцию заново (ретрай тиками)
    [Fact]
    public async Task SupervisorDownloadPhaseTransientStaysPending()
    {
        // Arrange — операция запущена; приёмник отдал failed+downloading
        // (mc/staging ENOSPC); ключ кандидата в etcd записан руками (transient
        // итог НЕ пишет — паттерн vc6/vc7)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv3");
        var s3 = new FakeBackupS3();
        await StartOperationAsync("sv3", s3);
        Receiver.GetReply = ApiDownloadTransient("mc cp failed");
        var key = "/pgworker/backups/sv3/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));
        await Fx.Gateway.PutAsync(Fx.Endpoint, key, before, null, ct);
        var process = BuildProcess("sv3", s3);

        // Act — поллинг: срыв скачивания
        (await process.TickAsync(BuildSnap("sv3"), BackupsOf("sv3",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — статус НЕ изменён (остался PENDING без квоты), журнал download-retry
        var after = (await Fx.Gateway.GetAsync(Fx.Endpoint, key, ct)).Value!.Value;
        after.Should().Be(before, "download-phase transient: the PENDING status is untouched");
        var journal = await Fx.Gateway.GetAsync(Fx.Endpoint, "/pgworker/work/sv3", ct);
        journal.Value!.Value.Should().Contain("download-retry/shard1/20260911120000Z");

        // Act 2 — следующий тик: приёмник снял терминальный снимок (GET 404) →
        // операция запущена заново
        Receiver.GetReply = new ReceiverReply(404, null, null);
        (await process.TickAsync(BuildSnap("sv3"), BackupsOf("sv3",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert 2 — повторный POST (ретрай тиками, spec §3.1)
        Receiver.StartBodies.Should().HaveCount(2, "a transient is retried on the next tick (spec §3.1)");
    }

    // AAA: GET 404 при живом кандидате (операция исчезла — рестарт приёмника) —
    // идемпотентный перезапуск: повторный POST следующим тиком, статус остаётся
    // PENDING (НЕ фейлится)
    [Fact]
    public async Task SupervisorNotFound404RestartsOperationIdempotently()
    {
        // Arrange — операция запущена первым тиком (POST#1); GET остаётся 404;
        // PENDING-ключ в etcd записан руками (ассерт «не изменён»)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv13");
        var s3 = new FakeBackupS3();
        await StartOperationAsync("sv13", s3);
        var pending = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var key = "/pgworker/backups/sv13/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(pending);
        await Fx.Gateway.PutAsync(Fx.Endpoint, key, before, null, ct);
        var process = BuildProcess("sv13", s3);

        // Act — следующий тик: операция vanished → повторный POST
        (await process.TickAsync(BuildSnap("sv13"), BackupsOf("sv13", pending), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — перезапуск выполнен, статус PENDING не тронут
        Receiver.StartBodies.Should().HaveCount(2, "the vanished operation is retried on the next tick");
        (await Fx.Gateway.GetAsync(Fx.Endpoint, key, ct)).Value!.Value
            .Should().Be(before, "the vanish is a transient — PENDING is not failed");
    }

    // AAA (arch/19 §5): GET транспортно-отказал (приёмник ноды недостижим) —
    // transient: статус PENDING не тронут, повторного POST нет (тик повторит GET)
    [Fact]
    public async Task SupervisorTransportRefusedTransientWaits()
    {
        // Arrange — операция запущена; GET молчит (StatusCode 0)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv10");
        var s3 = new FakeBackupS3();
        await StartOperationAsync("sv10", s3);
        Receiver.GetReply = new ReceiverReply(0, null, "Connection refused");
        var key = "/pgworker/backups/sv10/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(Completed("20260911120000Z",
            "000000010000000000000001", new BackupVerify(BackupVerifyStatus.Pending, null)));
        await Fx.Gateway.PutAsync(Fx.Endpoint, key, before, null, ct);
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess("sv10", s3, verifyObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act — поллинг
        (await process.TickAsync(BuildSnap("sv10"), BackupsOf("sv10",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — статус не тронут, нового запуска нет, observe transient
        (await Fx.Gateway.GetAsync(Fx.Endpoint, key, ct)).Value!.Value.Should().Be(before);
        Receiver.StartBodies.Should().ContainSingle("no re-POST on a transport failure");
        outcomes.Should().ContainSingle().Which.Should().Be(("sv10", "shard1", "transient"));
    }

    // AAA: GET 5xx (приёмник ответил ошибкой) — transient: статус не тронут,
    // повторного POST нет (тик повторит GET, spec §3.2)
    [Fact]
    public async Task SupervisorReceiver5xxStatusUntouched()
    {
        // Arrange — операция запущена; GET отвечает 503
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv7");
        var s3 = new FakeBackupS3();
        await StartOperationAsync("sv7", s3);
        Receiver.GetReply = new ReceiverReply(503, null, "gated");
        var key = "/pgworker/backups/sv7/shard1/full/20260911120000Z";
        var before = BackupStatusJson.Serialize(Completed("20260911120000Z",
            "000000010000000000000001", new BackupVerify(BackupVerifyStatus.Pending, null)));
        await Fx.Gateway.PutAsync(Fx.Endpoint, key, before, null, ct);
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess("sv7", s3, verifyObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act
        (await process.TickAsync(BuildSnap("sv7"), BackupsOf("sv7",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — ключ не изменён, запусков больше нет, observe transient
        var after = (await Fx.Gateway.GetAsync(
            Fx.Endpoint, "/pgworker/backups/sv7/shard1/full/20260911120000Z", ct)).Value!.Value;
        after.Should().Be(before);
        Receiver.StartBodies.Should().ContainSingle("no re-POST on a 5xx reply");
        outcomes.Should().ContainSingle().Which.Should().Be(("sv7", "shard1", "transient"));
    }

    // AAA: операторская отмена verify (cancel API — внешний канал мимо воркера;
    // GET отдаёт снимок state=cancelled — НЕ failed и НЕ 404-vanished) —
    // операция как не начинавшаяся: кандидат возвращается в PENDING с
    // checked_unix=now (квота попытки — перепроверка по расписанию политики,
    // симметрия таймаут-ветке), перманентного FAILED нет; следующий тик НЕ
    // перезапускает операцию (отличие от vanished-ветки — перезапуск там
    // немедленно, здесь — по расписанию периодики).
    [Fact]
    public async Task SupervisorCancelledBackToPendingNoRestart()
    {
        // Arrange — операция запущена (running); GET cancelled; PENDING-ключ в etcd
        // (checked_unix=null — попытки ещё не было)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv14");
        var s3 = new FakeBackupS3();
        await StartOperationAsync("sv14", s3);
        Receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("cancelled", "verifying", Error: "cancelled by operator"), null);
        var pending = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var key = "/pgworker/backups/sv14/shard1/full/20260911120000Z";
        await Fx.Gateway.PutAsync(Fx.Endpoint, key,
            BackupStatusJson.Serialize(pending), null, ct);
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess("sv14", s3, verifyObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act 1 — поллинг: операция отменена оператором → PENDING с квотой
        (await process.TickAsync(BuildSnap("sv14"), BackupsOf("sv14", pending), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — кандидат PENDING с checked_unix=now (квота попытки);
        // FAILED нет; журнал cancelled-by-operator; observe transient
        var verify = await ReadVerifyAsync("sv14", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("PENDING",
            "the cancel is not a verdict on the data — the candidate is re-checked by policy");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0,
            "the attempt counts (retry schedule), unlike the vanished branch");
        verify.TryGetProperty("error", out _).Should().BeFalse("no failure verdict");
        var journal = await Fx.Gateway.GetAsync(Fx.Endpoint, "/pgworker/work/sv14", ct);
        journal.Value!.Value.Should().Contain("cancelled-by-operator/shard1/20260911120000Z");
        outcomes.Should().ContainSingle().Which.Should().Be(("sv14", "shard1", "transient"));

        // Act 2 — следующий тик: модель = факт etcd после cancel-ветки
        // (PENDING c checked_unix=now — квота попытки записана ключом)
        var afterCancel = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending,
                TimeProvider.System.GetUtcNow().ToUnixTimeSeconds()));
        (await process.TickAsync(BuildSnap("sv14"), BackupsOf("sv14", afterCancel), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — операция НЕ перезапущена (квота попытки: перепроверка по
        // расписанию verify.interval_sec, немедленных перезапусков нет)
        Receiver.StartBodies.Should().HaveCount(1,
            "cancelled is not vanished — no immediate restart, the policy schedule rules");
    }

    // AAA (arch/19 §6): running-снимок с started_unix старше бюджета
    // JobVerifyTimeoutSec → cancel best-effort + кандидат PENDING с
    // checked_unix=now (квота попытки); лив-лок исключён — следующий due только
    // через interval
    [Fact]
    public async Task SupervisorOlderThanBudgetCancelsAndCountsAttempt()
    {
        // Arrange — COMPLETED-полный с Verify=PENDING; приёмник отдаёт running-
        // снимок с StartedUnix = now-7h (бюджет 6 ч дефолт); цепочка сида цела —
        // тик доходит до GET
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv9");
        var now = TimeProvider.System.GetUtcNow().ToUnixTimeSeconds();
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("sv9", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("sv9", "shard1", "000000010000000000000002"));
        s3.Objects.Add(("sv9", "shard1", "000000010000000000000003"));
        s3.PrefixedObjects.Add(("sv9", "shard1", "full/20260911090000Z/pg_wal/000000010000000000000003"));
        Receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("running", "verifying", Id: "20260911090000Z", StartedUnix: now - 7 * 3600), null);
        var a = Completed("20260911090000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var key = "/pgworker/backups/sv9/shard1/full/20260911090000Z";
        await Fx.Gateway.PutAsync(Fx.Endpoint, key, BackupStatusJson.Serialize(a), null, ct);
        var process = BuildProcess("sv9", s3);

        // Act — тик: поллинг видит running-снимок старше бюджета
        (await process.TickAsync(BuildSnap("sv9"), BackupsOf("sv9", a), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — cancel вызван; ключ полного: verify.state=PENDING,
        // checked_unix ≈ now (попытка зачтена, вердикта FAILED нет); журнал
        // verify-timeout
        Receiver.CancelUrls.Should().ContainSingle("the stale operation is cancelled best-effort");
        var verify = await ReadVerifyAsync("sv9", "20260911090000Z");
        ((string?)verify.GetProperty("state").GetString()).Should().Be("PENDING");
        var checkedUnix = verify.GetProperty("checked_unix").GetInt64();
        checkedUnix.Should().BeGreaterOrEqualTo(now - 60, "the attempt quota — the supervisor time");
        verify.TryGetProperty("error", out _).Should().BeFalse("the data is not at fault — FAILED is not set");
        var journal = await Fx.Gateway.GetAsync(Fx.Endpoint, "/pgworker/work/sv9", ct);
        journal.Value!.Value.Should().Contain("verify-timeout/shard1/20260911090000Z");

        // Act 2 — повторный тик немедленно (квота только что записана)
        var b = a with { Verify = new BackupVerify(BackupVerifyStatus.Pending, checkedUnix) };
        (await process.TickAsync(BuildSnap("sv9"), BackupsOf("sv9", b), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 3 — НОВЫЙ запуск НЕ выполнен (checked только что; pending due —
        // по interval), cancel не повторялся
        Receiver.StartBodies.Should().BeEmpty("the live-lock of immediate restarts is excluded (receiver)");
        Receiver.CancelUrls.Should().HaveCount(1);
    }

    // AAA: два due-кандидата — по одному за тик; PENDING раньше периодики (AC8)
    [Fact]
    public async Task TwoDueOnePerTickPendingEarlier()
    {
        // Arrange — full A (старее): verify=OK, CheckedUnix=now-7200 (периодика due при
        // interval 3600); full B (свежее): verify=PENDING (on_create); приёмник — 404
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv5");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("sv5", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv5", "shard1", "full/20260911090000Z/pg_wal/000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv5", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000001"));
        Receiver.GetReply = new ReceiverReply(404, null, null);
        var process = BuildProcess("sv5", s3);
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var backups = new ClusterBackups("sv5",
            new BackupPolicy(7, 4, 6, 86400, true, VerifyIntervalSec: 3600),
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new(
                [
                    Completed("20260911090000Z", "000000010000000000000001",
                        new BackupVerify(BackupVerifyStatus.Ok, now - 7200)),
                    Completed("20260911120000Z", "000000010000000000000001",
                        new BackupVerify(BackupVerifyStatus.Pending, null)),
                ], null),
            });

        // Act — тик 1: только B (PENDING-очередь раньше периодики)
        (await process.TickAsync(BuildSnap("sv5"), [backups], ct)).IsSuccess.Should().BeTrue();

        // Assert — один запуск, и это B
        Receiver.StartBodies.Should().ContainSingle()
            .Which.BackupId.Should().Be("20260911120000Z");
    }

    // AAA: периодика (AC5): OK-полный перепроверяется по interval_sec — цикл
    // ДОКАНЦА: succeeded → checked_unix РАСТЁТ; interval_sec<=0 — только
    // on_create; verify=null («никогда не проверялся») — периодика due;
    // FAILED не перепроверяется (терминален)
    [Fact]
    public async Task PeriodicityByIntervalDisabledAndFailedTerminality()
    {
        // Arrange — OK-полный CheckedUnix=now-7200; policy interval_sec=3600 → due
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv6");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("sv6", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv6", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000001"));
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var okBackup = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Ok, now - 7200));
        ClusterBackups WithInterval(long? interval, FullBackupState? full = null) => new("sv6",
            new BackupPolicy(7, 4, 6, 86400, true, interval),
            new Dictionary<string, ShardBackups> { ["shard1"] = new([full ?? okBackup], null) });

        // Act 1 — interval=3600: операция запущена (перепроверка due)
        Receiver.GetReply = new ReceiverReply(404, null, null);
        var process = BuildProcess("sv6", s3);
        (await process.TickAsync(BuildSnap("sv6"), [WithInterval(3600)], ct)).IsSuccess.Should().BeTrue();
        Receiver.StartBodies.Should().ContainSingle("the OK full is older than the interval — re-verify (AC5)");

        // Act 1b — операция завершилась (GET succeeded) → поллинг пишет итог
        Receiver.GetReply = ApiOk();
        (await process.TickAsync(BuildSnap("sv6"), [WithInterval(3600)], ct)).IsSuccess.Should().BeTrue();

        // Assert 1b — verify OK и checked_unix РАСТЁТ (было now-7200, стало ~now)
        var verify = await ReadVerifyAsync("sv6", "20260911120000Z");
        verify.GetProperty("state").GetString().Should().Be("OK");
        verify.GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(now - 7200,
            "the periodic re-verify refreshes checked_unix (AC5)");

        // Act 2 / Assert 2 — interval=0: не due (только on_create)
        Receiver.GetReply = new ReceiverReply(404, null, null);
        var processOff = BuildProcess("sv6", s3);
        (await processOff.TickAsync(BuildSnap("sv6"), [WithInterval(0)], ct)).IsSuccess.Should().BeTrue();
        Receiver.StartBodies.Should().HaveCount(1, "interval_sec<=0 — periodicity is off (AC5)");

        // Act 2b / Assert 2b — verify=null («никогда не проверялся», spec §3.1
        // due-periodic): ловится ТОЛЬКО периодикой (не on_create — verify нет)
        var neverVerified = Completed("20260911120000Z", "000000010000000000000001", verify: null);
        (await processOff.TickAsync(BuildSnap("sv6"), [WithInterval(3600, neverVerified)], ct))
            .IsSuccess.Should().BeTrue();
        Receiver.StartBodies.Should().HaveCount(2, "an unverified full (verify=null) is due by periodicity (§3.1)");

        // Act 3 / Assert 3 — FAILED не перепроверяется ни при каком interval
        var failed = new ClusterBackups("sv6",
            new BackupPolicy(7, 4, 6, 86400, true, 1),
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new([Completed("20260911120000Z", "000000010000000000000001",
                    new BackupVerify(BackupVerifyStatus.Failed, now, "bad"))], null),
            });
        (await processOff.TickAsync(BuildSnap("sv6"), [failed], ct)).IsSuccess.Should().BeTrue();
        Receiver.StartBodies.Should().HaveCount(2, "FAILED is terminal (spec §3.1)");
    }

    // AAA: running-снимок кандидата A жив (операция в работе) + кандидат B due
    // (PENDING) → новый запуск НЕ выполняется, статус B не тронут (spec §3.3 п.1)
    [Fact]
    public async Task LiveRunningOperationWaitsNoNewStart()
    {
        // Arrange — приёмник отвечает running-снимком (StartedUnix null — бюджета
        // нет); A — старее (выбирается первым), B — PENDING; ключи обоих
        // кандидатов записаны в etcd руками (ассерт «не тронут»)
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("sv8");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("sv8", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("sv8", "shard1", "full/20260911090000Z/pg_wal/000000010000000000000001"));
        var a = Completed("20260911090000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var b = Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null));
        var keyB = "/pgworker/backups/sv8/shard1/full/20260911120000Z";
        var beforeB = BackupStatusJson.Serialize(b);
        await Fx.Gateway.PutAsync(Fx.Endpoint,
            "/pgworker/backups/sv8/shard1/full/20260911090000Z", BackupStatusJson.Serialize(a), null, ct);
        await Fx.Gateway.PutAsync(Fx.Endpoint, keyB, beforeB, null, ct);
        var process = BuildProcess("sv8", s3);

        // Act — тик: поллинг видит running-снимок A (ждать), B due
        (await process.TickAsync(BuildSnap("sv8"), BackupsOf("sv8", a, b), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — новый запуск не выполнен; ключ B не изменён
        Receiver.StartBodies.Should().BeEmpty(
            "a live verify operation of the shard waits — no new start (§3.3 invariant)");
        var afterB = (await Fx.Gateway.GetAsync(Fx.Endpoint, keyB, ct)).Value!.Value;
        afterB.Should().Be(beforeB, "the due-candidate status is untouched while the operation of A is running");
    }

    // AAA (arch/19 §5): нода-источник исчезла из portalloc (шард менялся) —
    // приёмника нет: journal-факт node-missing + observe transient, статус
    // кандидата не тронут (кандидат останется PENDING до переснятия полного)
    [Fact]
    public async Task TickNodeGoneFromPortallocNodeMissingTransient()
    {
        // Arrange — portalloc ПУСТ (узел shard1a исчез); PENDING-кандидат с node=shard1a
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("vc8");
        await Fx.Gateway.PutAsync(Fx.Endpoint, "/pgworker/portalloc/vc8",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>()), null, ct);
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("vc8", "shard1", "000000010000000000000001"));
        s3.PrefixedObjects.Add(("vc8", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000001"));
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess("vc8", s3, verifyObserver: (c, s, r) => outcomes.Add((c, s, r)));
        var backups = BackupsOf("vc8", Completed("20260911120000Z", "000000010000000000000001",
            new BackupVerify(BackupVerifyStatus.Pending, null)));

        // Act
        var result = await process.TickAsync(BuildSnap("vc8"), backups, ct);

        // Assert — запуска нет; выбор зафиксирован журналом; observe transient
        result.IsSuccess.Should().BeTrue();
        Receiver.StartBodies.Should().BeEmpty("no receiver address — no operation");
        var journal = await Fx.Gateway.GetAsync(Fx.Endpoint, "/pgworker/work/vc8", ct);
        journal.Value!.Value.Should().Contain("node-missing/shard1/20260911120000Z");
        outcomes.Should().ContainSingle().Which.Should().Be(("vc8", "shard1", "transient"));
    }

    // AAA: — observe-точки verify: S3 недоступен → transient; битый
    // wal_start → permanent FAILED → failed; succeeded → ok
    // (процесс не меняется — тест фиксирует вызовы всех трёх исходов)
    [Fact]
    public async Task VerifyOutcomesCallObserverTransientFailedOk()
    {
        // Arrange 1 — COMPLETED PENDING-кандидат; S3-транспорт лежит
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify-obs", ct);
        Fx = fx;
        const string cluster = "vo1";
        await SeedAsync(cluster);
        var s3 = new FakeBackupS3 { Fails = true };
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess(cluster, s3,
            verifyObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act 1
        (await process.TickAsync(BuildSnap(cluster), BackupsOf(cluster,
                Completed("20260911120000Z", "000000010000000000000001",
                    new BackupVerify(BackupVerifyStatus.Pending, null))), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — list S3 упал: transient, статус кандидата не тронут
        outcomes.Should().ContainSingle().Which.Should().Be((cluster, "shard1", "transient"));

        // Arrange 2 — wal_start не разбирается: permanent FAILED сразу
        const string cluster2 = "vo2";
        await SeedAsync(cluster2);
        var outcomes2 = new List<(string C, string S, string Result)>();
        var process2 = BuildProcess(cluster2, new FakeBackupS3(),
            verifyObserver: (c, s, r) => outcomes2.Add((c, s, r)));

        // Act 2
        (await process2.TickAsync(BuildSnap(cluster2), BackupsOf(cluster2,
                Completed("20260911120000Z", "garbage-wal-start",
                    new BackupVerify(BackupVerifyStatus.Pending, null))), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — вердикт permanent: failed после успешной записи FAILED в etcd
        outcomes2.Should().ContainSingle().Which.Should().Be((cluster2, "shard1", "failed"));

        // Arrange 3 — операция запущена первым тиком (старт-тик исходов не эмитит),
        // приёмник отдал succeeded (GET Ok)
        const string cluster3 = "vo3";
        await SeedAsync(cluster3);
        var s3ok = new FakeBackupS3();
        await StartOperationAsync(cluster3, s3ok);
        Receiver.GetReply = ApiOk();
        var outcomes3 = new List<(string C, string S, string Result)>();
        var process3 = BuildProcess(cluster3, s3ok,
            verifyObserver: (c, s, r) => outcomes3.Add((c, s, r)));

        // Act 3 — тик поллинга итога
        (await process3.TickAsync(BuildSnap(cluster3), BackupsOf(cluster3,
                Completed("20260911120000Z", "000000010000000000000001",
                    new BackupVerify(BackupVerifyStatus.Pending, null))), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 3 — GET succeeded → observe ok после успешной записи OK в etcd
        outcomes3.Should().ContainSingle().Which.Should().Be((cluster3, "shard1", "ok"));
    }

    // --- (arch/19 §2 «Наблюдаемость итога verify»): итог — строка host-лога ----

    // AAA: итог OK от приёмника (GET succeeded) — строка host-лога воркера с
    // кластером/шардом и id полного (журнал остаётся летописью после
    // deprovisioning'а etcd-ключей).
    [Fact]
    public async Task SupervisorOkLogsOutcomeToHostLog()
    {
        // Arrange — операция запущена; приёмник отдал succeeded; spy-логгер снимает строки
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("hl1");
        var s3 = new FakeBackupS3();
        await StartOperationAsync("hl1", s3);
        Receiver.GetReply = ApiOk();
        var logger = new SpyLogger();
        var process = BuildProcess("hl1", s3, logger: logger);

        // Act — тик поллинга итога
        (await process.TickAsync(BuildSnap("hl1"), BackupsOf("hl1",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — строка итога OK с <C>/<X> и id полного
        logger.Lines.Should().Contain(l =>
            l.Contains("hl1/shard1", StringComparison.Ordinal)
            && l.Contains("verify 20260911120000Z OK", StringComparison.Ordinal));
    }

    // AAA: итог FAILED от приёмника (snapshot failed+verifying, ненулевой
    // pg_verifybackup) — строка host-лога с причиной из verify.error.
    [Fact]
    public async Task SupervisorVerifyPhaseFailedLogsOutcome()
    {
        // Arrange — операция запущена; приёмник отдал failed+verifying
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("hl2");
        var s3 = new FakeBackupS3();
        await StartOperationAsync("hl2", s3);
        Receiver.GetReply = ApiVerifyFailed("checksum mismatch failed");
        var logger = new SpyLogger();
        var process = BuildProcess("hl2", s3, logger: logger);

        // Act — тик поллинга итога
        (await process.TickAsync(BuildSnap("hl2"), BackupsOf("hl2",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — строка итога FAILED с причиной
        logger.Lines.Should().Contain(l =>
            l.Contains("verify 20260911120000Z FAILED", StringComparison.Ordinal)
            && l.Contains("checksum mismatch failed", StringComparison.Ordinal));
    }

    // AAA: итог FAILED цепочечной проверки (без запуска операции) — строка
    // host-лога с причиной (границы дыры); РОВНО один итог на полный —
    // transient-пути итоговой строки не пишут.
    [Fact]
    public async Task ChainFailedLogsOutcome()
    {
        // Arrange — wal/: 1,3 (дыра на 2); набор pg_wal/3; spy-логгер
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("verify", ct);
        Fx = fx;
        await SeedAsync("hl3");
        var s3 = new FakeBackupS3();
        s3.Objects.Add(("hl3", "shard1", "000000010000000000000001"));
        s3.Objects.Add(("hl3", "shard1", "000000010000000000000003"));
        s3.PrefixedObjects.Add(("hl3", "shard1", "full/20260911120000Z/pg_wal/000000010000000000000003"));
        var logger = new SpyLogger();
        var process = BuildProcess("hl3", s3, logger: logger);

        // Act — тик: дыра цепочки → permanent FAILED без запуска
        (await process.TickAsync(BuildSnap("hl3"), BackupsOf("hl3",
            Completed("20260911120000Z", "000000010000000000000001",
                new BackupVerify(BackupVerifyStatus.Pending, null))), ct)).IsSuccess.Should().BeTrue();

        // Assert — ровно одна итоговая строка FAILED с границей дыры
        logger.Lines.Should().ContainSingle(l =>
                l.Contains("verify 20260911120000Z FAILED", StringComparison.Ordinal)
                && l.Contains("000000010000000000000002", StringComparison.Ordinal),
            "exactly one terminal outcome line per full — transient paths log no outcome");
    }
}
