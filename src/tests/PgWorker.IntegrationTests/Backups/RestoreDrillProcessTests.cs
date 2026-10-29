using System.Text.Json;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Drill;
using PgWorker.Backups.Job;
using PgWorker.Backups.Process;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции RestoreDrillProcess (reliability, spec §3.3 п.2): у КАЖДОГО
// Fact своё etcd-окружение OwnEtcd (guid-имя pgw-ee-*, динамический порт,
// own-only teardown с ассертом чистоты — docs/e2e-isolation.md). Drill-операцию
// исполняет WalReceiver ПЕРВОЙ ноды шарда по HTTP API (POST/GET /api/ops/restore)
// — docker-фейков у процесса больше нет: поведение приёмника фейкается
// FakeReceiverClient (паттерны BackupVerifyProcessTests/RestoreProcessTests),
// portalloc для адреса приёмника читается реальным ShardEndpoints из etcd.
[Collection(NonE2eCollection.Name)]
public class RestoreDrillProcessTests
{
    // Окружение Fact'а (свой etcd); создаётся в начале каждого сценария.
    private OwnEtcd Fx = null!;

    // ClaimStore ОДИН на окружение: InstanceId фиксируется клэймом SeedAsync.
    private ClaimStore? _claimsStore;

    private ClaimStore Claims => _claimsStore ??= new("/pgworker", [Fx.Endpoint], Fx.Gateway, TimeProvider.System);

    // ── Сид-хелперы ──
    private static ClusterSnapshot BuildSnap(string cluster) => new(
        new ClusterConfig(cluster, 2, cluster, null, ClusterState.Active),
        [new ShardSpec("shard1", 1, $"host=shard1a dbname={cluster}", "shard1a:17001",
            [new NodeSpec("shard1", "shard1a", NodeState.Running)])],
        []);

    private const string Shard = "shard1";
    private const string DrillId = "20261001120000Z";

    private static string DrillKey(string cluster) => BackupNames.DrillKey(cluster, Shard);

    // Базовый URL API приёмника ПЕРВОЙ ноды шарда (арх/19 §3.6): portalloc сидит
    // канонической нодой (Object=null) → имя контейнера pgw-<C>-<X>-shard1a:8080.
    private static string ExpectedReceiverUrl(string cluster)
        => $"http://pgw-{cluster}-{Shard}-shard1a:8080";

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

    // portalloc БЕЗ шарда (первой ноды нет — шард демонтируется; паттерн dr20).
    private async Task SeedPortAllocWithoutShardAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        await Fx.Gateway.PutAsync(Fx.Endpoint, $"/pgworker/portalloc/{cluster}",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                ["shard2/shard2a"] = new("h1", new NodePorts(16011, 18011, 17011)),
            }), null, ct);
    }

    // Фейк API приёмника: ответы GetRestore/StartRestore программируются тестами.
    private readonly FakeReceiverClient Receiver = new();

    /// <summary>Мини-фейк API приёмника (drill-сценарии): StartRestore —
    /// программируемый ответ (202/409/отказ), GetRestore — программируемый,
    /// CancelOps — журнал URL. Все вызовы пишут URL (ассерты адреса первой
    /// ноды) и тело запроса (Id=drill.Id — drill-признак).</summary>
    private sealed class FakeReceiverClient : IWalReceiverApiClient
    {
        public ReceiverReply StartReply { get; set; } =
            new(202, new ReceiverStatus("running", "downloading"), null);
        public ReceiverReply GetReply { get; set; } =
            new(200, new ReceiverStatus("running", "downloading"), null);
        public readonly List<string> StartUrls = [];
        public readonly List<StartRestoreRequest> StartBodies = [];
        public readonly List<string> GetUrls = [];
        public readonly List<string> CancelUrls = [];

        // Колбэк ПЕРЕД ответом на POST: тест читает etcd в момент команды —
        // ассерт «journal-before-manipulations: RUNNING-ключ уже поставлен».
        public Func<Task>? OnStartRestore;

        public async Task<ReceiverReply> StartRestoreAsync(string baseUrl, StartRestoreRequest body, CancellationToken ct)
        {
            StartUrls.Add(baseUrl);
            StartBodies.Add(body);
            if (OnStartRestore is not null)
                await OnStartRestore();
            return StartReply;
        }

        public Task<ReceiverReply> GetRestoreAsync(string baseUrl, CancellationToken ct)
        {
            GetUrls.Add(baseUrl);
            return Task.FromResult(GetReply);
        }

        public Task<ReceiverReply> CancelOpsAsync(string baseUrl, CancellationToken ct)
        {
            CancelUrls.Add(baseUrl);
            return Task.FromResult(new ReceiverReply(200, null, null));
        }

        public Task<ReceiverReply> StartFullAsync(string baseUrl, string id, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> GetFullAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> CancelFullAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> StartVerifyAsync(string baseUrl, StartVerifyRequest body, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<ReceiverReply> GetVerifyAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<NodeReply<HbaEnsureResponse>> EnsureHbaAsync(string baseUrl, CancellationToken ct)
            => Task.FromResult(new NodeReply<HbaEnsureResponse>(200, new HbaEnsureResponse(true, false), null));
        public Task<NodeReply<PgDumpResponse>> PgDumpAsync(string baseUrl, StartPgDumpRequest body, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<NodeReply<PgDataResponse>> GetPgDataAsync(string baseUrl, CancellationToken ct)
            => throw new NotSupportedException();
    }

    private RestoreDrillProcess BuildProcess(
        string cluster, IBackupS3 s3, BackupsRuntimeOptions? options = null,
        Action<string, string, string>? drillObserver = null)
        => new(
            Fx.Gateway, [Fx.Endpoint],
            new ShardEndpoints(Fx.Gateway, [Fx.Endpoint], new ShardProbe(new HttpClient())),
            FakePoolFactory.Single(s3), Receiver, Claims,
            new WorkJournal("/pgworker", Fx.Gateway, [Fx.Endpoint]),
            options ?? new BackupsRuntimeOptions(
                Enabled: true, S3Bucket: "bkt"),
            TimeProvider.System,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<RestoreDrillProcess>.Instance,
            drillObserver);

    // Сид drill-ключа шарда (значение — сериализатор канона §4).
    private async Task SeedDrillAsync(string cluster, DrillState drill)
    {
        var ct = TestContext.Current.CancellationToken;
        await Fx.Gateway.PutAsync(Fx.Endpoint, DrillKey(cluster), DrillStatusJson.Serialize(drill), null, ct);
    }

    private static long NowUnix() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    private async Task<string> ReadDrillAsync(string cluster)
    {
        var kv = await Fx.Gateway.GetAsync(Fx.Endpoint, DrillKey(cluster),
            TestContext.Current.CancellationToken);
        kv.Value.Should().NotBeNull("the drill status is written to the /drill key");
        return kv.Value!.Value;
    }

    // Id активного дрилла из ключа (сверка «Id в теле POST == id RUNNING-ключа»).
    private async Task<string> ReadDrillIdAsync(string cluster)
        => JsonSerializer
            .Deserialize<Dictionary<string, JsonElement>>(await ReadDrillAsync(cluster))!["id"]
            .GetString()!;

    // Backups-аргумент тика — снапшот префикса кластера через снапшотный
    // BackupsParser (как в ReconcileLoop: процесс читает модель, не etcd).
    private async Task<IReadOnlyList<ClusterBackups>> SnapshotBackupsAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var range = await Fx.Gateway.RangeAsync(Fx.Endpoint, $"/pgworker/backups/{cluster}/", ct);
        range.IsSuccess.Should().BeTrue();
        var parsed = BackupsParser.Parse(range.Value, out var errors);
        errors.Should().BeEmpty("the seed keys must parse");
        return parsed.Value;
    }

    // ── Супервиз RUNNING-дрилла (API приёмника первой ноды) ──

    // AAA: RUNNING + операция running → phase из API пишется в ключ при изменении.
    [Fact]
    public async Task Supervise_RunningOperation_UpdatesPhaseFromApi()
    {
        // Arrange — свой etcd; RUNNING-ключ + GET приёмника отвечает recovering.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr1";
        await SeedAsync(cluster);
        Receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("running", "recovering"), null);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        var process = BuildProcess(cluster, new FakeBackupS3());

        // Act
        var result = await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct);

        // Assert — ключ содержит phase recovering (из статуса операции); GET ушёл
        // на URL приёмника первой ноды.
        result.IsSuccess.Should().BeTrue();
        (await ReadDrillAsync(cluster)).Should().Contain("\"phase\":\"recovering\"");
        Receiver.GetUrls.Should().ContainSingle().Which.Should().Be(ExpectedReceiverUrl(cluster));
    }

    // AAA: API-вердикт succeeded → SUCCEEDED + restored_to_lsn; ключ сразу
    // ЧИСТЫЙ терминальный итог (phase нет — scratch чистит приёмник).
    [Fact]
    public async Task Supervise_ApiSucceeded_CleanTerminalKeyWithLsn()
    {
        // Arrange — RUNNING-ключ; GET приёмника — succeeded с LSN.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr2";
        await SeedAsync(cluster);
        Receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("succeeded", "recovering", RestoredToLsn: "0/42"), null);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        var process = BuildProcess(cluster, new FakeBackupS3());

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — SUCCEEDED с LSN и finished_unix; phase снят; cancel не звался
        // (снос не нужен — scratch чистит приёмник).
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"SUCCEEDED\"").And.Contain("\"restored_to_lsn\":\"0/42\"");
        key.Should().Contain("finished_unix");
        key.Should().NotContain("phase", "a clean terminal outcome — the receiver tore down its scratch");
        key.Should().NotContain("error");
        Receiver.CancelUrls.Should().BeEmpty("no teardown commands — the receiver cleans up itself");
        (await ReadWorkPhaseAsync(cluster)).Should().Contain($"drill-done/{Shard}/{DrillId}");
    }

    // AAA: API-вердикт failed (phase=recovering — permanent) → FAILED с
    // причиной из статуса операции; ключ чистый терминальный итог.
    [Fact]
    public async Task Supervise_ApiFailed_FailsWithError()
    {
        // Arrange — RUNNING-ключ; GET приёмника — failed/recovering.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr3";
        await SeedAsync(cluster);
        Receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("failed", "recovering", Error: "boom"), null);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        var process = BuildProcess(cluster, new FakeBackupS3());

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — FAILED с причиной, phase снят.
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"FAILED\"").And.Contain("boom");
        key.Should().Contain("finished_unix");
        key.Should().NotContain("phase");
        (await ReadWorkPhaseAsync(cluster)).Should().Contain($"drill-failed/{Shard}/{DrillId}");
    }

    // AAA: transient GET (StatusCode 0 — приёмник недоступен) → ничего:
    // статус ключа не трогаем (следующий тик повторит супервиз).
    [Fact]
    public async Task Supervise_TransientGet_StatusUntouched()
    {
        // Arrange — RUNNING-ключ; GET транспортно-отказал.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr3t";
        await SeedAsync(cluster);
        Receiver.GetReply = new ReceiverReply(0, null, "connection refused");
        var before = DrillStatusJson.Serialize(
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        var process = BuildProcess(cluster, new FakeBackupS3());

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — ключ не изменён, POST не звался.
        (await ReadDrillAsync(cluster)).Should().Be(before, "transient GET — the status is untouched");
        Receiver.StartBodies.Should().BeEmpty();
    }

    // AAA: первой ноды шарда нет в portalloc (шард демонтируется) — transient:
    // статус RUNNING-ключа не трогаем, приёмник не вызывается.
    [Fact]
    public async Task Supervise_FirstNodeNotInPortalloc_StatusUntouched()
    {
        // Arrange — RUNNING-ключ; portalloc БЕЗ shard1.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr3p";
        await SeedAsync(cluster);
        await SeedPortAllocWithoutShardAsync(cluster);
        var started = NowUnix();
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", started));
        var before = DrillStatusJson.Serialize(
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", started));
        var process = BuildProcess(cluster, new FakeBackupS3());

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — ключ не изменён; GET не звался; журнал — transient «статус
        // не тронут».
        (await ReadDrillAsync(cluster)).Should().Be(before, "no receiver address — the status is left untouched");
        Receiver.GetUrls.Should().BeEmpty();
        (await ReadWorkPhaseAsync(cluster)).Should().Contain($"transient/{Shard}/{DrillId}")
            .And.Contain("status left untouched");
    }

    // ── Отбор / валидация / запуск (Task 7, spec §3.3 п.3–5) ──

    private const string FullId = "20261001090000Z";

    // Сид COMPLETED-полного шарда в etcd — кандидат отбора.
    private async Task SeedFullAsync(string cluster, string? walStart = "000000010000000000000001")
    {
        var ct = TestContext.Current.CancellationToken;
        var full = new FullBackupState(FullId, FullBackupStatus.Completed, "shard1a",
            BackupSourceRole.Replica, NowUnix() - 3600, NowUnix() - 3500, walStart, 1024, null, null);
        await Fx.Gateway.PutAsync(Fx.Endpoint, $"/pgworker/backups/{cluster}/{Shard}/full/{FullId}",
            BackupStatusJson.Serialize(full), null, ct);
    }

    // Непрерывная WAL-цепочка 1..3 + манифест кандидата (валидация проходит).
    private static void SeedValidCandidate(FakeBackupS3 s3, string cluster)
    {
        s3.Objects.Add((cluster, Shard, "000000010000000000000001"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000002"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000003"));
        s3.Texts[$"{cluster}/{Shard}/full/{FullId}/backup_manifest"] = "MANIFEST";
    }

    private async Task<string> ReadWorkPhaseAsync(string cluster)
    {
        var kv = await Fx.Gateway.GetAsync(Fx.Endpoint, $"/pgworker/work/{cluster}",
            TestContext.Current.CancellationToken);
        return kv.Value?.Value ?? "";
    }

    // AAA: journal-before-manipulations: первый дрилл стартует немедленно —
    // RUNNING-ключ ставится ДО команды (hook фейка читает etcd в момент POST);
    // команда POST /api/ops/restore уходит на приёмник первой ноды с Id дрилла.
    [Fact]
    public async Task Tick_NoDrillYet_PutsRunningThenStartsReceiver()
    {
        // Arrange — свой etcd; полный + цепочка + манифест валидны.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr6";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        var keyExistedAtStart = new List<bool>();
        Receiver.OnStartRestore = async () =>
        {
            var kv = await Fx.Gateway.GetAsync(Fx.Endpoint, DrillKey(cluster), ct);
            keyExistedAtStart.Add(kv.Value is not null);
        };
        var process = BuildProcess(cluster, s3, new BackupsRuntimeOptions(
            Enabled: true, S3Bucket: "bkt", RestoreRecoveryTimeoutSec: 777));

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — ключ RUNNING уже стоял в момент команды; POST один, на URL
        // приёмника первой ноды; тело: latest (TargetTime=null), Id дрилла из
        // ключа (drill-признак), бюджет recovery из options, S3-префикс кластера;
        // журнал started.
        keyExistedAtStart.Should().ContainSingle().Which.Should().BeTrue(
            "journal-before-manipulations: the RUNNING key precedes the command");
        (await ReadDrillAsync(cluster)).Should().Contain("\"RUNNING\"");
        Receiver.StartUrls.Should().ContainSingle().Which.Should().Be(ExpectedReceiverUrl(cluster));
        var body = Receiver.StartBodies.Should().ContainSingle().Subject;
        body.BackupId.Should().Be(FullId);
        body.TargetTime.Should().BeNull("drill is a latest-restore");
        body.RecoveryTimeoutSec.Should().Be(777);
        body.Id.Should().Be(await ReadDrillIdAsync(cluster), "the body carries the drill id — the receiver's drill marker");
        body.S3.Prefix.Should().Be($"{cluster}/{Shard}");
        body.S3.Bucket.Should().Be("bkt");
        (await ReadWorkPhaseAsync(cluster)).Should().Contain($"started/{Shard}/");
    }

    // AAA: 202 → журнал started/{shard}/{id} (операция принята приёмником).
    [Fact]
    public async Task Start_Accepted202_JournalStarted()
    {
        // Arrange — валидный кандидат; приёмник отвечает 202 (дефолт фейка).
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr6a";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        var process = BuildProcess(cluster, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — ключ RUNNING, журнал started с id операции.
        (await ReadDrillAsync(cluster)).Should().Contain("\"RUNNING\"");
        (await ReadWorkPhaseAsync(cluster)).Should().Contain($"started/{Shard}/");
    }

    // AAA: 409 (операция уже жива — пере-старт тика) → идемпотентно: тот же
    // журнал started, ключ RUNNING, тик успешен.
    [Fact]
    public async Task Start_Conflict409_JournalStartedKeyRunning()
    {
        // Arrange — валидный кандидат; приёмник отвечает 409 (операция жива).
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr6b";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        Receiver.StartReply = new ReceiverReply(409,
            new ReceiverStatus("running", "downloading", Id: "other-op"), null);
        var process = BuildProcess(cluster, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — ключ RUNNING, журнал started (пере-старт не ломает учёт).
        (await ReadDrillAsync(cluster)).Should().Contain("\"RUNNING\"");
        (await ReadWorkPhaseAsync(cluster)).Should().Contain($"started/{Shard}/");
    }

    // AAA: отказ команды (транспорт, не 202/409) — transient: ключ RUNNING
    // остаётся (следующий тик повторит POST), журнал started НЕ пишется.
    [Fact]
    public async Task Start_Refused_KeepsRunningNoJournal()
    {
        // Arrange — валидный кандидат; приёмник недоступен (StatusCode 0).
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr6c";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        Receiver.StartReply = new ReceiverReply(0, null, "connection refused");
        var process = BuildProcess(cluster, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — ключ RUNNING без error/финала; started в журнале нет.
        (await ReadDrillAsync(cluster)).Should().Contain("\"RUNNING\"")
            .And.NotContain("error").And.NotContain("finished_unix");
        (await ReadWorkPhaseAsync(cluster)).Should().NotContain("started/");
    }

    // AAA: первой ноды шарда нет в portalloc — запуск отложен (transient):
    // ни ключа, ни команды; журнал «start on next tick».
    [Fact]
    public async Task Start_FirstNodeNotInPortalloc_StartDeferredToNextTick()
    {
        // Arrange — валидный кандидат; portalloc БЕЗ shard1.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr6p";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        await SeedPortAllocWithoutShardAsync(cluster);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        var process = BuildProcess(cluster, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — ключа дрилла нет, приёмник не звался, журнал — transient.
        (await Fx.Gateway.GetAsync(Fx.Endpoint, DrillKey(cluster), ct)).Value.Should().BeNull(
            "no receiver address — nothing is started");
        Receiver.StartBodies.Should().BeEmpty();
        (await ReadWorkPhaseAsync(cluster)).Should().Contain($"transient/{Shard}")
            .And.Contain("start on next tick");
    }

    // AAA: свежий SUCCEEDED (finished_unix=now-60) → новых запусков нет до interval_days.
    [Fact]
    public async Task Tick_RecentSuccess_NoNewStart()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr7";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Succeeded, FullId, NowUnix() - 120,
                FinishedUnix: NowUnix() - 60, RestoredToLsn: "0/42"));
        var process = BuildProcess(cluster, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — команды нет, ключ не изменился (интервал сутки не истёк).
        Receiver.StartBodies.Should().BeEmpty();
        (await ReadDrillAsync(cluster)).Should().Contain("\"SUCCEEDED\"")
            .And.Contain("\"restored_to_lsn\":\"0/42\"");
    }

    // AAA: interval_days=0 (конфиг) — запусков нет.
    [Fact]
    public async Task Tick_IntervalZero_NoStart()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr8";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        var process = BuildProcess(cluster, s3, new BackupsRuntimeOptions(
            Enabled: true, S3Bucket: "bkt",
            DrillIntervalDays: 0));

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — ключа дрилла не появилось, команды нет.
        Receiver.StartBodies.Should().BeEmpty();
        var kv = await Fx.Gateway.GetAsync(Fx.Endpoint, DrillKey(cluster), ct);
        kv.Value.Should().BeNull("interval 0 — the drill is off");
    }

    // AAA: активная restore-заявка шарда — шард не кандидат.
    [Fact]
    public async Task Tick_ActiveRestore_ShardExcluded()
    {
        // Arrange — restore-ключ RUNNING шарда (restore владеет ЖЦ, arch/19 §3.5).
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr9";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        await Fx.Gateway.PutAsync(Fx.Endpoint,
            $"/pgworker/backups/{cluster}/{Shard}/restore/20261001100000Z",
            """{"state":"RUNNING","backup_id":"20261001090000Z","source":"c/x","target":"latest","node":"n1","requested_unix":1,"requested_by":"operator"}""",
            null, ct);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        var process = BuildProcess(cluster, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert
        Receiver.StartBodies.Should().BeEmpty("restore owns the shard LIFECYCLE — the drill does not start");
        (await Fx.Gateway.GetAsync(Fx.Endpoint, DrillKey(cluster), ct)).Value.Should().BeNull();
    }

    // AAA: валидационный провал (манифеста нет) → FAILED без запуска операции:
    // started/finished поставлены, error причина, ключ БЕЗ phase, journal failed.
    [Fact]
    public async Task Tick_ManifestMissing_FailsWithoutStart()
    {
        // Arrange — полный есть, манифест НЕ докачан (в S3 его нет).
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr10";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var s3 = new FakeBackupS3();
        s3.Objects.Add((cluster, Shard, "000000010000000000000001"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000002"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000003"));
        var process = BuildProcess(cluster, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — FAILED с причиной; started/finished; команды нет; phase нет.
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"FAILED\"").And.Contain("backup_manifest");
        key.Should().Contain("started_unix").And.Contain("finished_unix");
        key.Should().NotContain("phase", "no operation — a clean terminal outcome");
        Receiver.StartBodies.Should().BeEmpty();
        (await ReadWorkPhaseAsync(cluster)).Should().Contain($"failed/{Shard}/");
    }

    // AAA: дыра WAL-цепочки → FAILED с границами от WalChain, без запуска.
    [Fact]
    public async Task Tick_WalGap_FailsWithGapError()
    {
        // Arrange — wal/: 1, 3 (нет 2) — цепочка от 001 дырявая.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr11";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var s3 = new FakeBackupS3();
        s3.Texts[$"{cluster}/{Shard}/full/{FullId}/backup_manifest"] = "MANIFEST";
        s3.Objects.Add((cluster, Shard, "000000010000000000000001"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000003"));
        var process = BuildProcess(cluster, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — FAILED с границами дыры; команда не запускалась; чистый итог.
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"FAILED\"").And.Contain("000000010000000000000002");
        key.Should().NotContain("phase");
        Receiver.StartBodies.Should().BeEmpty();
    }

    // AAA: takeover: ключ RUNNING от «прошлого инстанса» — новый процесс
    // супервизит операцию до исхода (полный путь RUNNING → SUCCEEDED).
    [Fact]
    public async Task Tick_Takeover_SeesOperationThrough()
    {
        // Arrange — RUNNING-ключ (инстанс сменился); GET — downloading.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr12";
        await SeedAsync(cluster);
        Receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("running", "downloading"), null);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, FullId, NowUnix()));
        var process = BuildProcess(cluster, new FakeBackupS3());

        // Act 1 — супервиз: фаза из API.
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — ключ видит фазу операции прошлого инстанса.
        (await ReadDrillAsync(cluster)).Should().Contain("\"phase\":\"downloading\"");

        // Act 2 — операция «прошлого инстанса» дошла до исхода (GET succeeded).
        Receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("succeeded", "recovering", RestoredToLsn: "0/500"), null);
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — SUCCEEDED, restored_to_lsn, чистый итог.
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"SUCCEEDED\"").And.Contain("\"restored_to_lsn\":\"0/500\"");
        key.Should().NotContain("phase");
    }

    // AAA: RUNNING возраст > DrillTimeoutSec (бюджет мал в options) →
    // CancelOpsAsync приёмнику + FAILED drill-timeout; ключ чистый итог.
    [Fact]
    public async Task Tick_RunningTooLong_CancelsAndFailsTimeout()
    {
        // Arrange — бюджет 60 с, ключ RUNNING возрастом 120 с; GET — running.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr13";
        await SeedAsync(cluster);
        Receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("running", "recovering"), null);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, FullId, NowUnix() - 120));
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess(cluster, new FakeBackupS3(),
            new BackupsRuntimeOptions(
                Enabled: true, S3Bucket: "bkt",
                DrillTimeoutSec: 60),
            drillObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — FAILED drill-timeout; cancel ушёл на приёмник первой ноды
        // (гасит операцию и scratch); наблюдатель «failed»; журнал drill-failed.
        // error разбирается JSON-парсером: `>` в сериализованном JSON
        // экранируется (\u003E) — строковый Contain по сырому тексту хрупок.
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"FAILED\"").And.Contain("drill-timeout");
        key.Should().NotContain("phase");
        using var doc = System.Text.Json.JsonDocument.Parse(key);
        doc.RootElement.GetProperty("error").GetString().Should()
            .Be("drill-timeout: 120 s > 60 s");
        Receiver.CancelUrls.Should().ContainSingle().Which.Should().Be(ExpectedReceiverUrl(cluster));
        outcomes.Should().ContainSingle().Which.Should().Be((cluster, Shard, "failed"));
        (await ReadWorkPhaseAsync(cluster)).Should().Contain($"drill-failed/{Shard}/{DrillId}");
    }

    // AAA: операция vanished (GET 404), возраст ключа > бюджета → FAILED
    // drill-vanished без cancel (перезапуск не карается бюджетом — итог честный).
    [Fact]
    public async Task Tick_VanishedBeyondBudget_FailsVanished()
    {
        // Arrange — бюджет 60 с, ключ RUNNING возрастом 120 с; GET — 404.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr14";
        await SeedAsync(cluster);
        Receiver.GetReply = new ReceiverReply(404, null, null);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, FullId, NowUnix() - 120));
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess(cluster, new FakeBackupS3(),
            new BackupsRuntimeOptions(
                Enabled: true, S3Bucket: "bkt",
                DrillTimeoutSec: 60),
            drillObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — FAILED drill-vanished; cancel не звался; наблюдатель «failed».
        var key = await ReadDrillAsync(cluster);
        key.Should().Contain("\"FAILED\"").And.Contain("drill-vanished").And.Contain("operation missing");
        key.Should().Contain("finished_unix");
        key.Should().NotContain("phase");
        Receiver.CancelUrls.Should().BeEmpty();
        outcomes.Should().ContainSingle().Which.Should().Be((cluster, Shard, "failed"));
        (await ReadWorkPhaseAsync(cluster)).Should().Contain($"drill-failed/{Shard}/{DrillId}");
    }

    // AAA: операция vanished (GET 404), ключ МОЛОЖЕ бюджета → идемпотентный
    // повторный POST (резюмов нет — скачается заново); статус не фейлится.
    [Fact]
    public async Task Tick_VanishedYoung_RepostsIdempotentStart()
    {
        // Arrange — ключ RUNNING свежий; GET — 404.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr15";
        await SeedAsync(cluster);
        Receiver.GetReply = new ReceiverReply(404, null, null);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, FullId, NowUnix()));
        var process = BuildProcess(cluster, new FakeBackupS3());

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — повторный POST на приёмник первой ноды с Id ЭТОГО дрилла;
        // ключ остался RUNNING (не FAILED).
        Receiver.StartUrls.Should().ContainSingle().Which.Should().Be(ExpectedReceiverUrl(cluster));
        var body = Receiver.StartBodies.Should().ContainSingle().Subject;
        body.BackupId.Should().Be(FullId);
        body.Id.Should().Be(DrillId, "the repost restarts THIS drill operation");
        body.TargetTime.Should().BeNull();
        (await ReadDrillAsync(cluster)).Should().Contain("\"RUNNING\"")
            .And.NotContain("drill-vanished").And.NotContain("\"FAILED\"");
    }

    // AAA: Enabled=false — доводка активного дрилла работает (стоп-семантика),
    // новых запусков нет.
    [Fact]
    public async Task Tick_Disabled_FinishesActive_NoNewStarts()
    {
        // Arrange — полный валиден (кандидат есть), активная операция дошёл до
        // исхода; подсистема выключена.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr16";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        Receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("succeeded", "recovering", RestoredToLsn: "0/600"), null);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, FullId, NowUnix() - 30));
        var process = BuildProcess(cluster, s3, new BackupsRuntimeOptions(
            Enabled: false, S3Bucket: "bkt"));

        // Act 1 — доводка активного до терминала (стоп-семантика).
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — итог записан, чистый терминальный ключ.
        (await ReadDrillAsync(cluster)).Should().Contain("\"SUCCEEDED\"").And.NotContain("phase");

        // Act 2 — новых запусков при Enabled=false нет.
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2
        Receiver.StartBodies.Should().BeEmpty("disabled — finalization only, no new runs");
    }

    // AAA: интервал = policy.drill.interval_days ?? конфига (policy перекрывает).
    [Fact]
    public async Task Tick_PolicyInterval_OverridesConfig()
    {
        // Arrange — policy интервал 0 (выключен), конфиг 1: policy перекрывает.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr17";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        await Fx.Gateway.PutAsync(Fx.Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"retention":{"days":7,"weeks":4,"months":6},"full_max_age_sec":86400,"drill":{"interval_days":0}}""",
            null, ct);
        var s3 = new FakeBackupS3();
        SeedValidCandidate(s3, cluster);
        var process = BuildProcess(cluster, s3);

        // Act
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — запуска нет (policy 0 перекрыла дефолт конфига 1).
        Receiver.StartBodies.Should().BeEmpty();
    }

    // AAA: сбой скачивания (failed + phase=downloading) — МАРКЕР ТРАНЗИЕНТА:
    // статус не трогаем; следующий тик (GET 404) повторяет POST через
    // vanished-ветку.
    [Fact]
    public async Task Supervise_DownloadFailed_TransientNextTickReposts()
    {
        // Arrange — RUNNING-ключ; GET — failed/downloading.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr21";
        await SeedAsync(cluster);
        Receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("failed", "downloading", Error: "mc cp failed"), null);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, FullId, NowUnix()));
        var process = BuildProcess(cluster, new FakeBackupS3());

        // Act 1 — супервиз: сбой скачивания.
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — ключ остался RUNNING (transient), журнал transient, POST не звался.
        (await ReadDrillAsync(cluster)).Should().Contain("\"RUNNING\"")
            .And.NotContain("\"FAILED\"", "the download failure is transient — the drill is not failed");
        (await ReadWorkPhaseAsync(cluster)).Should().Contain($"transient/{Shard}/{DrillId}");
        Receiver.StartBodies.Should().BeEmpty();

        // Act 2 — следующий тик: операция vanished (GET 404), ключ молод → POST.
        Receiver.GetReply = new ReceiverReply(404, null, null);
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — команда запуска повторена с Id этого дрилла; статус RUNNING.
        Receiver.StartBodies.Should().ContainSingle().Which.Id.Should().Be(DrillId);
        (await ReadDrillAsync(cluster)).Should().Contain("\"RUNNING\"");
    }

    // AAA: — оба пути чистого терминального итога зовут наблюдателя:
    // FAILED-валидация без операции ("failed") и доведённый SUCCEEDED ("ok").
    [Fact]
    public async Task TerminalOutcomesCallObserver()
    {
        // Arrange 1 — полный есть, манифеста нет: FAILED-валидация без операции
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill-obs", ct);
        Fx = fx;
        const string cluster = "dro1";
        await SeedAsync(cluster);
        await SeedFullAsync(cluster);
        var s3 = new FakeBackupS3();
        s3.Objects.Add((cluster, Shard, "000000010000000000000001"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000002"));
        s3.Objects.Add((cluster, Shard, "000000010000000000000003"));
        var outcomes = new List<(string C, string S, string Result)>();
        var process = BuildProcess(cluster, s3,
            drillObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act 1 — тик валидации
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — чистый итог без операции
        outcomes.Should().ContainSingle().Which.Should().Be((cluster, Shard, "failed"));

        // Arrange 2 — RUNNING-операция succeeded → чистый итог SUCCEEDED
        const string cluster2 = "dro2";
        await SeedAsync(cluster2);
        Receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("succeeded", "recovering", RestoredToLsn: "0/42"), null);
        await SeedDrillAsync(cluster2,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        outcomes.Clear();
        var process2 = BuildProcess(cluster2, new FakeBackupS3(),
            drillObserver: (c, s, r) => outcomes.Add((c, s, r)));

        // Act 2 — вердикт SUCCEEDED
        (await process2.TickAsync(BuildSnap(cluster2), await SnapshotBackupsAsync(cluster2), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — ровно один вызов: ok на чистом терминальном итоге
        outcomes.Should().ContainSingle().Which.Should().Be((cluster2, Shard, "ok"));
    }

    // AAA: операторская отмена дрилла (POST /api/ops/cancel приёмника — внешний
    // канал мимо воркера; GET отдаёт state=cancelled — НЕ failed и НЕ 404) —
    // ключ дрилла УДАЛЯЕТСЯ (операций нет — записей нет; scratch чистит
    // приёмник); никакого FAILED; следующий тик ничего не запускает.
    [Fact]
    public async Task ReceiverCancelled_KeyRemovedNoTraceNoFailed()
    {
        // Arrange — RUNNING-ключ; GET — cancelled.
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await OwnEtcd.StartAsync("drill", ct);
        Fx = fx;
        const string cluster = "dr16x";
        await SeedAsync(cluster);
        Receiver.GetReply = new ReceiverReply(200,
            new ReceiverStatus("cancelled", "recovering", Error: "cancelled by operator"), null);
        await SeedDrillAsync(cluster,
            new DrillState(DrillId, DrillStatus.Running, "20261001090000Z", NowUnix()));
        var process = BuildProcess(cluster, new FakeBackupS3());

        // Act 1 — супервиз: операция отменена оператором
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 1 — ключ дрилла УДАЛЁН (следа нет); cancel сам воркер не звал.
        (await Fx.Gateway.GetAsync(Fx.Endpoint, DrillKey(cluster), ct)).Value.Should().BeNull(
            "no trace left — the drill key is removed with the cancelled operation");
        Receiver.CancelUrls.Should().BeEmpty();

        // Act 2 — следующий тик: ключа нет, кандидата нет — запускать нечего
        (await process.TickAsync(BuildSnap(cluster), await SnapshotBackupsAsync(cluster), ct))
            .IsSuccess.Should().BeTrue();

        // Assert 2 — команд нет, записи нет
        Receiver.StartBodies.Should().BeEmpty("the cancelled drill leaves no key to retry");
        (await Fx.Gateway.GetAsync(Fx.Endpoint, DrillKey(cluster), ct)).Value.Should().BeNull();
    }
}
