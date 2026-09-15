using Shared.Etcd.Coordination;
using Xunit;

namespace Shared.Etcd.UnitTests;

// WorkJournal: событие PhaseWritten (t04, seam S2 — arch/18 §2.2), JSON-контракт
// {prefix}/work/<C>, серия ретраев и unreachable-трек. t09: объединение копий
// WorkJournalPhaseEventTests (Pg/Kfw) и journal-кейсов CoordinationTests +
// новые регресс-кейсы сохранения трека (§7.1 — фазовая запись не сбрасывает трек).
public class WorkJournalTests
{
    private const string Prefix = "/unit";

    private static WorkJournal NewJournal(FakeCoordinationGateway gateway)
        => new(Prefix, gateway, ["http://fake"]);

    // --- Событие PhaseWritten (перенос WorkJournalPhaseEventTests) ---

    [Fact]
    public async Task WritePhaseAsync_Success_EmitsEvent_WithClusterOpPhase()
    {
        // Arrange: журнал над живым фейком etcd; подписка собирает события
        var gateway = new FakeCoordinationGateway();
        var journal = NewJournal(gateway);
        var events = new List<WorkJournal.WorkPhaseEntry>();
        journal.PhaseWritten += e => events.Add(e);

        // Act
        var result = await journal.WritePhaseAsync("demo", "provision", "started", "i1", null, TestContext.Current.CancellationToken);

        // Assert: успешная фазовая запись → событие с фактическим (cluster, op, phase)
        result.IsSuccess.Should().BeTrue();
        events.Should().ContainSingle(e => e.Cluster == "demo" && e.Op == "provision" && e.Phase == "started");
    }

    [Fact]
    public async Task WritePhaseAsync_FailedPut_DoesNotEmitEvent()
    {
        // Arrange: Put падает (etcd недоступен)
        var gateway = new FakeCoordinationGateway
        {
            PutFault = _ => Result.Failed(new HttpRequestException("etcd недоступен")),
        };
        var journal = NewJournal(gateway);
        var emitted = false;
        journal.PhaseWritten += _ => emitted = true;

        // Act
        var result = await journal.WritePhaseAsync("demo", "provision", "started", "i1", null, TestContext.Current.CancellationToken);

        // Assert: событие после НЕуспешной записи не эмитится — метрики не врут
        result.IsSuccess.Should().BeFalse();
        emitted.Should().BeFalse();
    }

    [Fact]
    public async Task WriteSupervisionAsync_Success_DoesNotEmitEvent()
    {
        // Arrange: надзор — не фазовый процесс (arch/18 §2.2, ревью Ф4-2):
        // стационарные записи мимо события; подавление дублируется на потребителе.
        var gateway = new FakeCoordinationGateway();
        var journal = NewJournal(gateway);
        var emitted = false;
        journal.PhaseWritten += _ => emitted = true;

        // Act
        var result = await journal.WriteSupervisionAsync("demo", "i1", new Dictionary<string, long>(), null, TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue();
        emitted.Should().BeFalse();
    }

    [Fact]
    public async Task WritePhaseAsync_TerminalCrashed_EmitsEvent()
    {
        // Arrange: терминальные фазы (crashed — фактический словарь журнала)
        // приходят тем же путём WritePhaseAsync (ReconcileLoop.LogCrashAsync)
        var gateway = new FakeCoordinationGateway();
        var journal = NewJournal(gateway);
        var events = new List<WorkJournal.WorkPhaseEntry>();
        journal.PhaseWritten += events.Add;

        // Act
        var result = await journal.WritePhaseAsync("demo", "adopt", "crashed", "i1", "boom", TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue();
        events.Should().ContainSingle(e => e.Cluster == "demo" && e.Op == "adopt" && e.Phase == "crashed");
    }

    // --- JSON-контракт и серия ретраев (перенос CoordinationTests) ---

    [Fact]
    public async Task WritePhaseAsync_PutsCamelCaseJson()
    {
        // Arrange
        var gateway = new FakeCoordinationGateway();
        var journal = NewJournal(gateway);

        // Act
        var result = await journal.WritePhaseAsync("shop", "provision", "planned", "inst-1", null, CancellationToken.None);

        // Assert: ключ {prefix}/work/<C>, camelCase-поля
        result.IsSuccess.Should().BeTrue();
        gateway.Store.Should().ContainKey($"{Prefix}/work/shop");
        var raw = gateway.Store[$"{Prefix}/work/shop"];
        raw.Should().Contain("\"op\":\"provision\"");
        raw.Should().Contain("\"phase\":\"planned\"");
        raw.Should().Contain("\"instance\":\"inst-1\"");
        raw.Should().Contain("\"updated_unix\":");
    }

    [Fact]
    public async Task RoundTrip_WorkState()
    {
        // Arrange
        var gateway = new FakeCoordinationGateway();
        var journal = NewJournal(gateway);
        await journal.WritePhaseAsync("shop", "deprovision", "removing-nodes", "inst-2", "boom", CancellationToken.None);

        // Act
        var result = await journal.ReadAsync("shop", CancellationToken.None);

        // Assert: все поля пережили запись/чтение без потерь
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeNull();
        var state = result.Value!;
        state.Op.Should().Be("deprovision");
        state.Phase.Should().Be("removing-nodes");
        state.Instance.Should().Be("inst-2");
        state.LastError.Should().Be("boom");
        state.UpdatedUnix.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task WritePhaseAsync_WithSeries_CarriesRetryFields()
    {
        // Arrange: журнал с контекстом серии ретраев.
        var gateway = new FakeCoordinationGateway();
        var journal = NewJournal(gateway);
        var series = new RetrySeries(FailCount: 3, FailFirstUnix: 1756000000, RetryNotBeforeUnix: 1756000035);

        // Act: запись фазы с переносом серии.
        var result = await journal.WritePhaseAsync(
            "shop", "provision", "waiting-patroni", "inst-1", "boom", CancellationToken.None, series);

        // Assert: round-trip сохраняет серию (фазы прогресса не стирают контекст неудачи).
        result.IsSuccess.Should().BeTrue();
        var state = await journal.ReadAsync("shop", CancellationToken.None);
        state.Value!.FailCount.Should().Be(3);
        state.Value.FailFirstUnix.Should().Be(1756000000);
        state.Value.RetryNotBeforeUnix.Should().Be(1756000035);
    }

    [Fact]
    public async Task WritePhaseAsync_WithoutSeries_OmitsRetryFields()
    {
        // Arrange: серия была; успех пишет фазу без контекста (сброс).
        var gateway = new FakeCoordinationGateway();
        var journal = NewJournal(gateway);
        await journal.WritePhaseAsync("shop", "provision", "failed", "inst-1", "boom", CancellationToken.None,
            new RetrySeries(2, 1756000000, 1756000010));

        // Act: запись Done без серии.
        await journal.WritePhaseAsync("shop", "provision", "done", "inst-1", null, CancellationToken.None);

        // Assert: поля серии отсутствуют в JSON и в модели.
        var raw = gateway.Store[$"{Prefix}/work/shop"];
        raw.Should().NotContain("fail_count");
        var state = await journal.ReadAsync("shop", CancellationToken.None);
        state.Value!.FailCount.Should().BeNull();
    }

    [Fact]
    public async Task ReadLegacyFormat_RetryFieldsNull()
    {
        // Arrange: журнал старого формата (до полей серии) — честный JSON без них.
        var gateway = new FakeCoordinationGateway();
        gateway.Store[$"{Prefix}/work/old"] =
            """{"op":"provision","phase":"planned","instance":"i","updated_unix":1756000000}""";
        var journal = NewJournal(gateway);

        // Act
        var state = await journal.ReadAsync("old", CancellationToken.None);

        // Assert: обратная совместимость — поля null, чтение не падает.
        state.Value!.FailCount.Should().BeNull();
        state.Value.RetryNotBeforeUnix.Should().BeNull();
    }

    // --- Unreachable-трек (новые регресс-кейсы t09 §7.1) ---

    [Fact]
    public async Task WritePhaseAsync_WithoutTrack_PreservesExistingUnreachableTrack()
    {
        // Arrange: supervise записал трек молчания (фаза без явного unreachable)
        var gateway = new FakeCoordinationGateway();
        var journal = new WorkJournal(Prefix, gateway, ["http://fake"]);
        await journal.WriteSupervisionAsync("demo", "i1",
            new Dictionary<string, long> { ["b1"] = 100 }, null, TestContext.Current.CancellationToken);

        // Act: фазовая запись процесса БЕЗ трека (путь Kfw-процессов до t09)
        var result = await journal.WritePhaseAsync("demo", "reassign", "running", "i1", null, TestContext.Current.CancellationToken);

        // Assert: трек сохранён — пороги NodeDead/BrokerDead не сбрасываются фазами
        result.IsSuccess.Should().BeTrue();
        var state = await journal.ReadAsync("demo", TestContext.Current.CancellationToken);
        state.Value!.Unreachable.Should().ContainKey("b1").WhoseValue.Should().Be(100);
    }

    [Fact]
    public async Task WritePhaseAsync_WithExplicitTrack_OverwritesExisting()
    {
        // Arrange: трек надзора записан
        var gateway = new FakeCoordinationGateway();
        var journal = NewJournal(gateway);
        await journal.WriteSupervisionAsync("demo", "i1",
            new Dictionary<string, long> { ["b1"] = 100 }, null, CancellationToken.None);

        // Act: фазовая запись с ЯВНЫМ треком — владелец трека перезаписывает
        var result = await journal.WritePhaseAsync("demo", "supervise-extra", "running", "i1", null,
            CancellationToken.None, unreachable: new Dictionary<string, long> { ["b2"] = 200 });

        // Assert: явный трек заменил существующий
        result.IsSuccess.Should().BeTrue();
        var state = await journal.ReadAsync("demo", CancellationToken.None);
        state.Value!.Unreachable.Should().ContainKey("b2").WhoseValue.Should().Be(200);
        state.Value.Unreachable.Should().NotContainKey("b1");
    }

    [Fact]
    public async Task WriteSupervisionAsync_OverwritesTrackCompletely()
    {
        // Arrange: трек с брокером b1; надзор фиксирует, что b1 ожил, b2 молчит
        var gateway = new FakeCoordinationGateway();
        var journal = NewJournal(gateway);
        await journal.WriteSupervisionAsync("demo", "i1",
            new Dictionary<string, long> { ["b1"] = 100 }, null, CancellationToken.None);

        // Act: надзор перезаписывает трек актуальным множеством (только b2)
        var result = await journal.WriteSupervisionAsync("demo", "i1",
            new Dictionary<string, long> { ["b2"] = 300 }, null, CancellationToken.None);

        // Assert: трек — ровно актуальное множество (b1 исчез)
        result.IsSuccess.Should().BeTrue();
        var state = await journal.ReadAsync("demo", CancellationToken.None);
        state.Value!.Unreachable.Should().ContainKey("b2").WhoseValue.Should().Be(300);
        state.Value.Unreachable.Should().HaveCount(1);
    }

    [Fact]
    public async Task WriteSupervisionAsync_EmptyTrack_ClearsUnreachable()
    {
        // Arrange: трек был; надзор видит все ноды живыми
        var gateway = new FakeCoordinationGateway();
        var journal = NewJournal(gateway);
        await journal.WriteSupervisionAsync("demo", "i1",
            new Dictionary<string, long> { ["b1"] = 100 }, null, CancellationToken.None);

        // Act: пустой словарь = актуальное множество «никто не молчит»
        var result = await journal.WriteSupervisionAsync("demo", "i1",
            new Dictionary<string, long>(), null, CancellationToken.None);

        // Assert: трек очищен
        result.IsSuccess.Should().BeTrue();
        var state = await journal.ReadAsync("demo", CancellationToken.None);
        state.Value!.Unreachable.Should().BeEmpty();
    }
}
