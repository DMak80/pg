using System.Diagnostics.Metrics;
using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.Core.OwnS3;
using S3Worker.Provisioning;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// Процесс F — SyncProbe (arch/24 §5 F, spec §8.1): матрица connectivity по
// фактическим targets, пары сверек, свёртка/бюджет страниц, вердикты
// synced/lagging/diverged по прогресс-критерию, гейт SyncIntervalSec и заявка
// /s3worker/probes/<C> (прогон → del). Канонические JSON — строгими строками.
public class SyncProbeTests
{
    private const string Prefix = "/s3worker";

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeNodeClient NodeClient = new();
        public FixedTimeProvider Clock = new();
        public WorkJournal Journal = null!;
        public ClaimStore Claims = null!;
        public SyncProbeProcess Probe = null!;

        public static Rig New(int syncIntervalSec = 60, int syncMaxPages = 20)
        {
            var rig = new Rig();
            var endpoints = new[] { "http://etcd:2379" };
            rig.Journal = new WorkJournal(Prefix, rig.Etcd, endpoints);
            rig.Claims = new ClaimStore(Prefix, endpoints, rig.Etcd, rig.Clock);
            rig.Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken).GetAwaiter().GetResult();
            var secrets = new SecretProfileStore(
                name => name == "S3W_NODE_SECRETS__backups-main__ROOT_USER" ? "root"
                    : name == "S3W_NODE_SECRETS__backups-main__ROOT_PASSWORD" ? "pw" : null,
                ["S3W_NODE_SECRETS__backups-main__ROOT_USER", "S3W_NODE_SECRETS__backups-main__ROOT_PASSWORD"]);
            var meter = new Meter("S3Worker");
            rig.Probe = new SyncProbeProcess(
                rig.Etcd, endpoints, rig.Claims, rig.Journal, secrets, rig.NodeClient,
                new S3ProvisioningOptions(18000, 18999, 100, 90, 15, null, "owns3:dev", "pgworker-backups",
                    SyncIntervalSec: syncIntervalSec, SyncMaxPages: syncMaxPages),
                rig.Clock, meter);
            return rig;
        }

        // Страница sync-report (равенство — по всем полям, удобно задавать сжато).
        public static OwnS3SyncReportPage Page(int count, string? firstKey, long firstMod,
            string? lastKey, long lastMod, long totalSize, string hash, string? nextMarker) =>
            new(count, firstKey, firstMod, lastKey, lastMod, totalSize, hash, nextMarker);
    }

    // Active-кластер: portalloc east1/east2, state-набор из аргумента.
    private static S3ClusterSnapshot Snap(
        (string Name, string State)[]? nodes = null,
        string secretProfile = "backups-main")
    {
        var nodeStates = nodes ?? [("east1", S3NodeStates.Running), ("east2", S3NodeStates.Running)];
        return new S3ClusterSnapshot(
            "demo",
            new S3ClusterConfig("owns3", nodeStates.Length, secretProfile, 1760156800, null),
            nodeStates.Select(n => new S3NodeSnapshot(n.Name, n.State, "h1",
                new S3NodePlacement("h1", null, null), null, null, null)).ToList(),
            "h1:18001,h1:18002",
            null,
            null,
            null);
    }

    private static void SeedPortAlloc(Fakes.FakeEtcd etcd, params string[] names)
    {
        var entries = names.Select((n, i) => $@"""{n}"":{{""host"":""h1"",""s3"":{18001 + i}}}");
        etcd.Seed($"/s3worker/portalloc/demo", $"{{{string.Join(",", entries)}}}");
    }

    // Зеркальные probe-ответы (east1↔east2, оба reachable) + равные sync-report
    // страницы с немедленным концом — база happy-path прогонов.
    private static void SeedMirrorProbe(Fakes.FakeNodeClient nodeClient)
    {
        nodeClient.ProbeByPort[18001] = new OwnS3ProbeReport(true, [new OwnS3ProbeTarget("east2", true, 12)]);
        nodeClient.ProbeByPort[18002] = new OwnS3ProbeReport(true, [new OwnS3ProbeTarget("east1", true, 10)]);
        nodeClient.SyncReportPages[18001] = new Dictionary<string, OwnS3SyncReportPage>
        {
            [""] = Rig.Page(0, null, 0, null, 0, 0, "0", null),
        };
        nodeClient.SyncReportPages[18002] = new Dictionary<string, OwnS3SyncReportPage>
        {
            [""] = Rig.Page(0, null, 0, null, 0, 0, "0", null),
        };
    }

    // ==================== 1. Матрица connectivity (§8.1.1) ====================

    [Fact]
    public async Task Матрица_ЗеркальныеТаргеты_OkПолнаяМатрица()
    {
        // Arrange: оба probe-ответа с взаимными reachable-таргетами.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        SeedMirrorProbe(rig.NodeClient);
        var now = rig.Clock.Utc.ToUnixTimeSeconds();

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: строгий канон arch/23 §3 — полная матрица ok.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/connectivity"].Value.Should().Be(
            "{\"state\":\"OK\",\"checked_unix\":" + now +
            ",\"links\":{\"east1\":{\"east2\":\"ok\"},\"east2\":{\"east1\":\"ok\"}}}");
    }

    [Fact]
    public async Task Матрица_ОдинOffline_Degraded()
    {
        // Arrange: у east2 пинг east1 не отвечает (reachable=false).
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.ProbeByPort[18001] = new OwnS3ProbeReport(true, [new OwnS3ProbeTarget("east2", true, 12)]);
        rig.NodeClient.ProbeByPort[18002] = new OwnS3ProbeReport(true, [new OwnS3ProbeTarget("east1", false)]);
        SeedMirrorSyncPages(rig.NodeClient);

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: часть линков offline → DEGRADED.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/connectivity"].Value.Should().Contain("\"state\":\"DEGRADED\"");
        rig.Etcd.Store["/s3/clusters/demo/connectivity"].Value.Should().Contain("""{"east1":"offline"}""");
    }

    [Fact]
    public async Task Матрица_ВсеОффлайн_Down()
    {
        // Arrange: оба линка offline (живых нет).
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.ProbeByPort[18001] = new OwnS3ProbeReport(true, [new OwnS3ProbeTarget("east2", false)]);
        rig.NodeClient.ProbeByPort[18002] = new OwnS3ProbeReport(true, [new OwnS3ProbeTarget("east1", false)]);
        SeedMirrorSyncPages(rig.NodeClient);

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/connectivity"].Value.Should().Contain("\"state\":\"DOWN\"");
    }

    [Fact]
    public async Task Матрица_ТаргетоВНет_OkПустаяМатрица()
    {
        // Arrange: у обоих targets пуст — «никому не настроено» = OK, sync SYNCED.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.ProbeByPort[18001] = new OwnS3ProbeReport(true, []);
        rig.NodeClient.ProbeByPort[18002] = new OwnS3ProbeReport(true, []);
        var now = rig.Clock.Utc.ToUnixTimeSeconds();

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: пустая матрица OK; пар нет — SYNCED (строгий канон).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/connectivity"].Value.Should().Be(
            "{\"state\":\"OK\",\"checked_unix\":" + now + ",\"links\":{}}");
        rig.Etcd.Store["/s3/clusters/demo/sync"].Value.Should().Be(
            "{\"state\":\"SYNCED\",\"checked_unix\":" + now + ",\"pairs\":[]}");
    }

    [Fact]
    public async Task Матрица_ОтказПробы_СтрокиНет_ProbeError()
    {
        // Arrange: probe east1 не ответит (не запрограммирован → Failed); east2
        // отвечает, пинг соседа мёртв.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.ProbeByPort[18002] = new OwnS3ProbeReport(true, [new OwnS3ProbeTarget("east1", false)]);
        SeedMirrorSyncPages(rig.NodeClient);

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: строки east1 в links нет; линки east2 по его ответу; единственный
        // линк offline (живых нет) → DOWN по канону arch/23 §3; probe_error
        // называет east1 (реконструкции нет — §3.2 п.4).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var json = rig.Etcd.Store["/s3/clusters/demo/connectivity"].Value;
        json.Should().Contain("\"state\":\"DOWN\"");
        json.Should().NotContain("\"east1\":{");
        json.Should().Contain("\"east2\":{\"east1\":\"offline\"}");
        json.Should().Contain("probe_error");
        json.Should().Contain("east1");
    }

    [Fact]
    public async Task Матрица_НольОтветивших_DownPlusProbeError()
    {
        // Arrange: оба probe-вызова Failed (транспорт).
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: DOWN + probe_error непуст (обязателен при 0 ответивших — §3.2 п.5).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var json = rig.Etcd.Store["/s3/clusters/demo/connectivity"].Value;
        json.Should().Contain("\"state\":\"DOWN\"");
        json.Should().Contain("\"probe_error\":\"");
    }

    // ==================== 2. Пары (§8.1.2) ====================

    [Fact]
    public async Task Пары_ЗеркальныеОднаПара()
    {
        // Arrange: зеркальные targets → одна нормализованная пара.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        SeedMirrorProbe(rig.NodeClient);
        var now = rig.Clock.Utc.ToUnixTimeSeconds();

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: строгий канон sync — одна пара {min,max}, сверена в 0/0.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/sync"].Value.Should().Be(
            "{\"state\":\"SYNCED\",\"checked_unix\":" + now +
            ",\"pairs\":[{\"from\":\"east1\",\"to\":\"east2\",\"missing\":0,\"extra\":0}]}");
    }

    [Fact]
    public async Task Пары_НезеркальныйТаргет_ОднаПара()
    {
        // Arrange: target есть только у east1 — пара всё равно одна (объединение).
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.ProbeByPort[18001] = new OwnS3ProbeReport(true, [new OwnS3ProbeTarget("east2", true)]);
        rig.NodeClient.ProbeByPort[18002] = new OwnS3ProbeReport(true, []);
        rig.NodeClient.SyncReportPages[18001] = new Dictionary<string, OwnS3SyncReportPage> { [""] = Rig.Page(0, null, 0, null, 0, 0, "0", null) };
        rig.NodeClient.SyncReportPages[18002] = new Dictionary<string, OwnS3SyncReportPage> { [""] = Rig.Page(0, null, 0, null, 0, 0, "0", null) };
        var now = rig.Clock.Utc.ToUnixTimeSeconds();

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: одна пара (east1,east2), SYNCED.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/sync"].Value.Should().Be(
            "{\"state\":\"SYNCED\",\"checked_unix\":" + now +
            ",\"pairs\":[{\"from\":\"east1\",\"to\":\"east2\",\"missing\":0,\"extra\":0}]}");
    }

    [Fact]
    public async Task Пары_НетПар_Synced()
    {
        // Arrange: targets пусты у всех — pairs пуст.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.NodeClient.ProbeByPort[18001] = new OwnS3ProbeReport(true, []);
        rig.NodeClient.ProbeByPort[18002] = new OwnS3ProbeReport(true, []);

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/sync"].Value.Should().Contain("\"state\":\"SYNCED\"");
        rig.Etcd.Store["/s3/clusters/demo/sync"].Value.Should().Contain("\"pairs\":[]");
    }

    // ==================== 3. Сверка (§8.1.3) ====================

    [Fact]
    public async Task Сверка_РавныеСтраницы_Synced00()
    {
        // Arrange: две равные страницы (nextMarker → вторая, конец) у обеих сторон.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        SeedMirrorProbe(rig.NodeClient);
        rig.NodeClient.SyncReportPages[18001] = new Dictionary<string, OwnS3SyncReportPage>
        {
            [""] = Rig.Page(2, "k1", 100, "k2", 200, 30, "h1", "m1"),
            ["m1"] = Rig.Page(1, "k3", 300, "k3", 300, 5, "h2", null),
        };
        rig.NodeClient.SyncReportPages[18002] = new Dictionary<string, OwnS3SyncReportPage>
        {
            [""] = Rig.Page(2, "k1", 100, "k2", 200, 30, "h1", "m1"),
            ["m1"] = Rig.Page(1, "k3", 300, "k3", 300, 5, "h2", null),
        };
        var now = rig.Clock.Utc.ToUnixTimeSeconds();

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: все страницы равны → SYNCED 0/0 (строгий канон).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/sync"].Value.Should().Be(
            "{\"state\":\"SYNCED\",\"checked_unix\":" + now +
            ",\"pairs\":[{\"from\":\"east1\",\"to\":\"east2\",\"missing\":0,\"extra\":0}]}");
    }

    [Fact]
    public async Task Сверка_РасхождениеНаВторойСтранице_DiffЛистингов()
    {
        // Arrange: первая страница равна, вторая различается → diff листингов:
        // east1 3 записи, east2 — одна общая → missing 2, lag_bytes = их суммарный объём.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        SeedMirrorProbe(rig.NodeClient);
        rig.NodeClient.SyncReportPages[18001] = new Dictionary<string, OwnS3SyncReportPage>
        {
            [""] = Rig.Page(1, "k1", 100, "k1", 100, 10, "h1", "m1"),
            ["m1"] = Rig.Page(3, "k1", 100, "k3", 300, 60, "h2f", null),
        };
        rig.NodeClient.SyncReportPages[18002] = new Dictionary<string, OwnS3SyncReportPage>
        {
            [""] = Rig.Page(1, "k1", 100, "k1", 100, 10, "h1", "m1"),
            ["m1"] = Rig.Page(1, "k1", 100, "k1", 100, 10, "h2t", null),
        };
        rig.NodeClient.Listings[18001] =
            [new OwnS3ListEntry("k1", 100, 10), new OwnS3ListEntry("k2", 200, 20), new OwnS3ListEntry("k3", 300, 30)];
        rig.NodeClient.Listings[18002] = [new OwnS3ListEntry("k1", 100, 10)];
        var now = rig.Clock.Utc.ToUnixTimeSeconds();

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: missing=2 (k2,k3), extra=0, lag_bytes=50; первый прогон без
        // прошлого и без очереди → lagging → агрегат LAGGING.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/sync"].Value.Should().Be(
            "{\"state\":\"LAGGING\",\"checked_unix\":" + now +
            ",\"pairs\":[{\"from\":\"east1\",\"to\":\"east2\",\"missing\":2,\"extra\":0,\"lag_bytes\":50}]}");
    }

    [Fact]
    public async Task Сверка_ТотЖеКлючДругойModTime_ИMissingИExtra()
    {
        // Arrange: ключ k с разными modTime у сторон — элемент сверки (key, modTime)
        // попадает в ОБА счётчика.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        SeedMirrorProbe(rig.NodeClient);
        rig.NodeClient.SyncReportPages[18001] = new Dictionary<string, OwnS3SyncReportPage> { [""] = Rig.Page(1, "k", 100, "k", 100, 5, "hf", null) };
        rig.NodeClient.SyncReportPages[18002] = new Dictionary<string, OwnS3SyncReportPage> { [""] = Rig.Page(1, "k", 200, "k", 200, 5, "ht", null) };
        rig.NodeClient.Listings[18001] = [new OwnS3ListEntry("k", 100, 5)];
        rig.NodeClient.Listings[18002] = [new OwnS3ListEntry("k", 200, 5)];

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: missing=1 И extra=1.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var sync = rig.Etcd.Store["/s3/clusters/demo/sync"].Value;
        sync.Should().Contain("\"missing\":1");
        sync.Should().Contain("\"extra\":1");
    }

    // ==================== 4. Вердикты (§8.1.4) ====================

    [Fact]
    public async Task Вердикт_ОчередьНепуста_Lagging()
    {
        // Arrange: устойчивое расхождение (2/2 как в прошлом прогоне), НО
        // fetch-очередь приёмника east2 непуста → свидетельство прогресса.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        SeedDiffScenario(rig);
        rig.Etcd.Seed("/s3/clusters/demo/sync",
            """{"state":"LAGGING","checked_unix":1,"pairs":[{"from":"east1","to":"east2","missing":2,"extra":0}]}""");
        rig.NodeClient.StatusResultByPort[18002] = new OwnS3ReplicationStatus(true, ["east1"])
        {
            Neighbors = [new OwnS3ReplicationNeighbor("east1", 5, null, false, false)],
        };

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: lagging (очередь), lag_objects=5 в каноне; агрегат LAGGING.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var sync = rig.Etcd.Store["/s3/clusters/demo/sync"].Value;
        sync.Should().Contain("\"state\":\"LAGGING\"");
        sync.Should().Contain("\"lag_objects\":5");
    }

    [Fact]
    public async Task Вердикт_Сокращение_ПрошлогоЛаггинг()
    {
        // Arrange: прошлый прогон 5 расхождений, сейчас 2 — догон сокращается.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        SeedDiffScenario(rig);
        rig.Etcd.Seed("/s3/clusters/demo/sync",
            """{"state":"LAGGING","checked_unix":1,"pairs":[{"from":"east1","to":"east2","missing":5,"extra":0}]}""");

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: сокращение missing+extra → lagging (очередь пуста).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/sync"].Value.Should().Contain("\"state\":\"LAGGING\"");
        rig.Etcd.Store["/s3/clusters/demo/sync"].Value.Should().NotContain("diverged");
    }

    [Fact]
    public async Task Вердикт_НетПрогресса_Diverged()
    {
        // Arrange: расхождение устойчиво (2 → 2), очередь пуста — «дыра».
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        SeedDiffScenario(rig);
        rig.Etcd.Seed("/s3/clusters/demo/sync",
            """{"state":"LAGGING","checked_unix":1,"pairs":[{"from":"east1","to":"east2","missing":2,"extra":0}]}""");

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: diverged → агрегат DIVERGED (строгий канон, lag-полей нет).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var now = rig.Clock.Utc.ToUnixTimeSeconds();
        rig.Etcd.Store["/s3/clusters/demo/sync"].Value.Should().Be(
            "{\"state\":\"DIVERGED\",\"checked_unix\":" + now +
            ",\"pairs\":[{\"from\":\"east1\",\"to\":\"east2\",\"missing\":2,\"extra\":0,\"lag_bytes\":50}]}");
    }

    [Fact]
    public async Task Вердикт_ПервыйПрогонБезПрошлого_Lagging()
    {
        // Arrange: прошлого значения нет, очередь пуста — свидетельства
        // устойчивости нет → lagging (НЕ diverged).
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        SeedDiffScenario(rig);

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/sync"].Value.Should().Contain("\"state\":\"LAGGING\"");
    }

    // ==================== 5. Бюджет страниц (§8.1.5) ====================

    [Fact]
    public async Task Бюджет_ЦепочкаСтраниц_ПараНеСверена_Lagging()
    {
        // Arrange: 21+ равных страниц с непустым nextMarker при SyncMaxPages=20 —
        // бюджет исчерпан, пара не сверена.
        var rig = Rig.New(syncMaxPages: 20);
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        SeedMirrorProbe(rig.NodeClient);
        var chain = new Dictionary<string, OwnS3SyncReportPage>();
        for (var i = 0; i <= 21; i++)
        {
            var marker = i == 0 ? "" : $"m{i}";
            chain[marker] = Rig.Page(1, "k1", 100, "k1", 100, 10, "h1", $"m{i + 1}");
        }

        rig.NodeClient.SyncReportPages[18001] = chain;
        rig.NodeClient.SyncReportPages[18002] = chain;
        var now = rig.Clock.Utc.ToUnixTimeSeconds();

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: пары [], last_error с бюджетом, агрегат LAGGING (несвёрнутая пара).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/sync"].Value.Should().Be(
            "{\"state\":\"LAGGING\",\"checked_unix\":" + now +
            ",\"pairs\":[],\"last_error\":\"листинг пары east1↔east2 превышает бюджет страниц\"}");
    }

    // ==================== 6. Гейт/заявка (§8.1.6) ====================

    [Fact]
    public async Task Гейт_СвежийКлючБезЗаявки_ТихийТик()
    {
        // Arrange: connectivity свежий (checked_unix = now), заявки нет.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        var now = rig.Clock.Utc.ToUnixTimeSeconds();
        rig.Etcd.Seed("/s3/clusters/demo/connectivity", $$"""{"state":"OK","checked_unix":{{now}}}""");

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: тихий тик — put connectivity/sync нет, journal-записи нет.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Ops.Should().NotContain(op => op.StartsWith("put:/s3/clusters/demo/connectivity", StringComparison.Ordinal));
        rig.Etcd.Ops.Should().NotContain(op => op.StartsWith("put:/s3/clusters/demo/sync", StringComparison.Ordinal));
        var journal = await rig.Journal.ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value.Should().BeNull();
    }

    [Fact]
    public async Task Гейт_Истёк_Прогон()
    {
        // Arrange: checked_unix старше SyncIntervalSec (60 с) на 61.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        SeedMirrorProbe(rig.NodeClient);
        var now = rig.Clock.Utc.ToUnixTimeSeconds();
        rig.Etcd.Seed("/s3/clusters/demo/connectivity", "{\"state\":\"OK\",\"checked_unix\":" + (now - 61) + "}");

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: прогон — оба ключа переписаны.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Ops.Should().Contain(op => op.StartsWith("put:/s3/clusters/demo/connectivity", StringComparison.Ordinal));
        rig.Etcd.Ops.Should().Contain(op => op.StartsWith("put:/s3/clusters/demo/sync", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Заявка_ПрогонИDel()
    {
        // Arrange: заявка /s3worker/probes/demo стоит (гейт неважен).
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        SeedMirrorProbe(rig.NodeClient);
        var now = rig.Clock.Utc.ToUnixTimeSeconds();
        rig.Etcd.Seed("/s3worker/probes/demo",
            "{\"requested_unix\":" + (now - 5) + ",\"requested_by\":\"e2e-op\"}");

        // Act
        var result = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert: прогон исполнен, заявка снесена ПОСЛЕ записи ключей.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store.ContainsKey("/s3worker/probes/demo").Should().BeFalse();
        rig.Etcd.Ops.Should().Contain(op => op.StartsWith("put:/s3/clusters/demo/connectivity", StringComparison.Ordinal));
        rig.Etcd.Ops.Should().Contain(op => op.StartsWith("put:/s3/clusters/demo/sync", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Заявка_FailedПрогонаЖива()
    {
        // Arrange: отказ put connectivity — прогон Failed, заявка обязана жить.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        SeedMirrorProbe(rig.NodeClient);
        rig.Etcd.Seed("/s3worker/probes/demo", """{"requested_unix":1,"requested_by":"e2e-op"}""");
        rig.Etcd.PutFault = key => key == "/s3/clusters/demo/connectivity"
            ? new ApplicationException("etcd недоступен")
            : null;

        // Act 1
        var failed = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert 1: Failed, заявка жива, ключей нет.
        failed.IsSuccess.Should().BeFalse();
        rig.Etcd.Store.ContainsKey("/s3worker/probes/demo").Should().BeTrue();
        rig.Etcd.Store.ContainsKey("/s3/clusters/demo/connectivity").Should().BeFalse();
        var journal = await rig.Journal.ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("probe-failed");

        // Act 2: fault снят — следующий тик пишет ключи и сносит заявку.
        rig.Etcd.PutFault = null;
        var retry = await rig.Probe.TickAsync(Snap(), TestContext.Current.CancellationToken);

        // Assert 2
        retry.IsSuccess.Should().BeTrue(retry.Error?.Message);
        rig.Etcd.Store.ContainsKey("/s3/clusters/demo/connectivity").Should().BeTrue();
        rig.Etcd.Store.ContainsKey("/s3worker/probes/demo").Should().BeFalse();
    }

    // ==================== 8. 0 RUNNING (§3.5 п.1) ====================

    [Fact]
    public async Task НольRunning_Idle_ЗаявкаСнесена_КлючиНеПишутся()
    {
        // Arrange: инстансы NOT_INITIALIZED; заявка стоит.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.Etcd.Seed("/s3worker/probes/demo", """{"requested_unix":1,"requested_by":"e2e-op"}""");

        // Act
        var result = await rig.Probe.TickAsync(
            Snap(nodes: [("east1", S3NodeStates.NotInitialized), ("east2", S3NodeStates.NotInitialized)]),
            TestContext.Current.CancellationToken);

        // Assert: idle, заявка исполнена («нечем зондировать»), ключей нет,
        // probe-вызовов не было.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store.ContainsKey("/s3worker/probes/demo").Should().BeFalse();
        rig.Etcd.Store.ContainsKey("/s3/clusters/demo/connectivity").Should().BeFalse();
        rig.NodeClient.ProbeCalls.Should().BeEmpty();
        var journal = await rig.Journal.ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Op.Should().Be("sync-probe");
        journal.Value.Phase.Should().Be("idle");
    }

    // ==================== 9. Профиль недоступен ====================

    [Fact]
    public async Task ПрофильНедоступен_Idle_ЗаявкаЖива()
    {
        // Arrange: профиль секретов не задан в env — грани недостижимы.
        var rig = Rig.New();
        SeedPortAlloc(rig.Etcd, "east1", "east2");
        rig.Etcd.Seed("/s3worker/probes/demo", """{"requested_unix":1,"requested_by":"e2e-op"}""");

        // Act
        var result = await rig.Probe.TickAsync(Snap(secretProfile: "missing"), TestContext.Current.CancellationToken);

        // Assert: idle + warning, заявка жива (исполнится после починки env).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store.ContainsKey("/s3worker/probes/demo").Should().BeTrue();
        rig.NodeClient.ProbeCalls.Should().BeEmpty();
        var journal = await rig.Journal.ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("idle");
        journal.Value.LastError.Should().Contain("профиль секретов недоступен");
    }

    // Расходящаяся пара: зеркальные probe-ответы (пара east1/east2), sync-report
    // первая страница равна, вторая различается; листинги — 3 записи у east1
    // (k1,k2,k3), 1 у east2 (k1) → missing 2, extra 0.
    private static void SeedDiffScenario(Rig rig)
    {
        SeedMirrorProbe(rig.NodeClient);
        rig.NodeClient.SyncReportPages[18001] = new Dictionary<string, OwnS3SyncReportPage>
        {
            [""] = Rig.Page(1, "k1", 100, "k1", 100, 10, "h1", "m1"),
            ["m1"] = Rig.Page(3, "k1", 100, "k3", 300, 60, "h2f", null),
        };
        rig.NodeClient.SyncReportPages[18002] = new Dictionary<string, OwnS3SyncReportPage>
        {
            [""] = Rig.Page(1, "k1", 100, "k1", 100, 10, "h1", "m1"),
            ["m1"] = Rig.Page(1, "k1", 100, "k1", 100, 10, "h2t", null),
        };
        rig.NodeClient.Listings[18001] =
            [new OwnS3ListEntry("k1", 100, 10), new OwnS3ListEntry("k2", 200, 20), new OwnS3ListEntry("k3", 300, 30)];
        rig.NodeClient.Listings[18002] = [new OwnS3ListEntry("k1", 100, 10)];
    }

    // Равные sync-report страницы с немедленным концом (для матричных тестов,
    // где пары сверяются в 0/0).
    private static void SeedMirrorSyncPages(Fakes.FakeNodeClient nodeClient)
    {
        nodeClient.SyncReportPages[18001] = new Dictionary<string, OwnS3SyncReportPage> { [""] = Rig.Page(0, null, 0, null, 0, 0, "0", null) };
        nodeClient.SyncReportPages[18002] = new Dictionary<string, OwnS3SyncReportPage> { [""] = Rig.Page(0, null, 0, null, 0, 0, "0", null) };
    }
}
