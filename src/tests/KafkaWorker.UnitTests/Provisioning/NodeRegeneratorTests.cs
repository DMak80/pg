using System.Text.Json;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Etcd.Parsing;
using KafkaWorker.Provisioning;
using KafkaWorker.Provisioning.Processes;
using Xunit;
using static KafkaWorker.UnitTests.Provisioning.Fakes;

namespace KafkaWorker.UnitTests.Provisioning;

// Процесс J (t06, spec §5.2): автоконверге лимитов — один брокер за тик,
// guard'ы передержки, прогресс-ключ /kafkaworker/regens/<C> ТОЛЬКО при живой
// операции (чужие недоведённые ноды фантома не рисуют), del по сходимости.
public class NodeRegeneratorTests : IAsyncLifetime
{
    private readonly FakeEtcd _etcd = new();
    private readonly FakeKafkaDriver _driver = new();
    private readonly FixedTimeProvider _time = new()
    {
        Utc = new DateTimeOffset(2026, 9, 2, 12, 0, 0, TimeSpan.Zero),
    };
    private readonly ClaimStore _claims;
    private readonly WorkJournal _journal;
    private readonly NodeRegenerator _regen;
    private const string Cluster = "events";

    public NodeRegeneratorTests()
    {
        _etcd.Seed($"/kafka/clusters/{Cluster}/config",
            """{"brokers":2,"replication_factor":2,"min_insync_replicas":1,"default_partitions":3,"default_retention_ms":604800000,"created_unix":1756500000}""");
        for (var k = 1; k <= 2; k++)
        {
            _etcd.Seed($"/kafka/clusters/{Cluster}/brokers/broker{k}/state", "RUNNING");
            _etcd.Seed($"/kafka/clusters/{Cluster}/brokers/broker{k}/role", k == 1 ? "controller" : "broker");
            _etcd.Seed($"/kafka/clusters/{Cluster}/brokers/broker{k}/resources", """{"cpu":"2","mem":"4Gi","disk":"40Gi"}""");
        }

        _etcd.Seed($"/kafka/clusters/{Cluster}/app_user", "app");
        _etcd.Seed($"/kafka/clusters/{Cluster}/app_password", "p1");
        _etcd.Seed($"/kafkaworker/portalloc/{Cluster}",
            """{"broker1":{"host":"h1","client":16001},"broker2":{"host":"h1","client":16002}}""");
        _driver.NodeObjects.AddRange(["kfw-events-broker1", "kfw-events-broker2"]);
        // Факт: broker1 разошёлся (cpu 1 против 2), broker2 сходится.
        // Арифметика — как в записи/сверке: (long)((double)1 * 1e9).
        _driver.Limits["kfw-events-broker1"] = new(1_000_000_000L, 4L << 30);
        _driver.Limits["kfw-events-broker2"] = new(2_000_000_000L, 4L << 30);

        var endpoints = new[] { "http://etcd" };
        _claims = new ClaimStore(endpoints, _etcd, _time);
        _journal = new WorkJournal(_etcd, endpoints);
        _regen = new NodeRegenerator(_etcd, endpoints, _driver, _claims, _journal,
            new ProvisioningOptions(16000, 16999, BrokerBootSec: 100, NodeDeadSec: 90, null, "apache/kafka:4.0.0"));
    }

    // Клэйм держит _claims (владелец _regen); «чужие» инстансы строятся
    // отдельным ClaimStore (см. RunAsync_NoClaim_Fails).
    public async ValueTask InitializeAsync()
    {
        TestContext.Current.CancellationToken.ThrowIfCancellationRequested();
        await _claims.TryClaimClusterAsync(Cluster, TestContext.Current.CancellationToken);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private async Task<KafkaClusterSnapshot> SnapshotAsync()
    {
        var range = await _etcd.RangeAsync("http://etcd", "/kafka/clusters/", CancellationToken.None);
        return KafkaSnapshotParser.Parse(range.Value).Value.Single(c => c.Cluster == Cluster);
    }

    private async Task<string?> KeyAsync(string key)
        => (await _etcd.GetAsync("http://etcd", key, CancellationToken.None)).Value?.Value;

    private NodeRegenerator Stranger()
        => new(_etcd, ["http://etcd"], _driver,
            new ClaimStore(["http://etcd"], _etcd, _time), _journal,
            new ProvisioningOptions(16000, 16999, BrokerBootSec: 100, NodeDeadSec: 90, null, "apache/kafka:4.0.0"));

    [Fact]
    public async Task RunAsync_LimitsDiverged_RecreatesOneBrokerPerTick()
    {
        // Arrange
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert — ровно один рестарт за тик (брокер1 — первый по имени),
        // том сохранён, state=PROVISIONING, прогресс-ключ поставлен.
        result.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().ContainSingle().Which.Should().Be(("broker1", false));
        _driver.AllEnsured.Should().ContainSingle(e => e.NodeName == "broker1");
        (await KeyAsync($"/kafka/clusters/{Cluster}/brokers/broker1/state")).Should().Be("PROVISIONING");
        var progress = await KeyAsync($"/kafkaworker/regens/{Cluster}");
        progress.Should().NotBeNull();
        using var doc = JsonDocument.Parse(progress!);
        doc.RootElement.GetProperty("brokers_total").GetInt32().Should().Be(1);
        doc.RootElement.GetProperty("brokers_remaining").GetInt32().Should().Be(1);
        doc.RootElement.GetProperty("current_broker").GetString().Should().Be("broker1");
    }

    [Fact]
    public async Task RunAsync_NotRunningBrokerExists_WaitsWithoutRecreate()
    {
        // Arrange — broker2 ещё PROVISIONING (возврат — зона AddBrokerProcess)
        // при ЖИВОЙ операции (broker1 разошёлся) — передержка с прогрессом
        _etcd.Seed($"/kafka/clusters/{Cluster}/brokers/broker2/state", "PROVISIONING");
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert — передержка: никаких пересозданий, журнал waiting-return,
        // прогресс-ключ жив (операция идёт — remaining пересчитан)
        result.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().BeEmpty();
        var state = await _journal.ReadAsync(Cluster, CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-return");
        (await KeyAsync($"/kafkaworker/regens/{Cluster}")).Should().NotBeNull();
    }

    [Fact]
    public async Task RunAsync_ForeignNotRunningBroker_NoPhantomProgressKey()
    {
        // Arrange — broker2 PROVISIONING (чужой add-broker/надзор),
        // расхождений НЕТ и операции не было — ключа тоже нет
        _driver.Limits["kfw-events-broker1"] = new(2_000_000_000L, 4L << 30); // сходится
        _etcd.Seed($"/kafka/clusters/{Cluster}/brokers/broker2/state", "PROVISIONING");
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert — фантомный прогресс запрещён (spec §4.1: put при старте
        // первого пересоздания; отсутствие ключа = операции нет — ревью
        // Фазы 4 раунд 2, замечание 1): no-op, ключ не появляется
        result.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().BeEmpty();
        (await KeyAsync($"/kafkaworker/regens/{Cluster}")).Should().BeNull();
    }

    [Fact]
    public async Task RunAsync_LiveRotation_WaitsWithoutRecreate()
    {
        // Arrange — живая заявка ротации (фазы A–B)
        _etcd.Seed($"/kafkaworker/rotations/{Cluster}", """{"requested_unix":1756500000,"requested_by":"admin"}""");
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().BeEmpty();
        var state = await _journal.ReadAsync(Cluster, CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-rotation");
    }

    [Fact]
    public async Task RunAsync_LiveReassignment_WaitsWithoutRecreate()
    {
        // Arrange — живой прогресс reassignment
        _etcd.Seed($"/kafkaworker/reassignments/{Cluster}",
            """{"mode":"drain","drain_broker":"broker2","partitions_total":3,"partitions_remaining":2,"submitted_unix":1756500000,"updated_unix":1756500000,"instance":"i1"}""");
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().BeEmpty();
        var state = await _journal.ReadAsync(Cluster, CancellationToken.None);
        state.Value!.Phase.Should().Be("waiting-reassign");
    }

    [Fact]
    public async Task RunAsync_InspectFails_FailsTickWithoutRecreate()
    {
        // Arrange — слепой docker: сверка невозможна → никаких действий
        _driver.ResourcesFaultByNode = _ => Result<NodeLimits?>.Failed(
            new ApplicationException("docker: connection refused"));
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert — ошибка тика (следующий тик повторит), брокеры не тронуты
        result.IsSuccess.Should().BeFalse();
        _driver.Removed.Should().BeEmpty();
    }

    [Fact]
    public async Task RunAsync_EnsureFailsOnce_NextTickRecoversWithoutHarm()
    {
        // Arrange — сбой между Remove и Ensure (spec §5.4: идемпотентность)
        var snap = await SnapshotAsync();
        _driver.EnsureResultByNode = _ => Result.Failed(new ApplicationException("docker: create failed"));

        // Act — первый тик падает на ensure; второй (docker «ожил») проходит
        var first = await _regen.RunAsync(snap, CancellationToken.None);
        _driver.EnsureResultByNode = null;
        var second = await _regen.RunAsync(await SnapshotAsync(), CancellationToken.None);

        // Assert — первый Failed; второй сходится безопасно: контейнера broker1
        // нет (Remove сработал) → инспект null → пропуск, никаких повторных
        // Remove/Ensure (контейнер восстановит надзор; state сойдётся по факту)
        first.IsSuccess.Should().BeFalse();
        second.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().ContainSingle(); // ровно один Remove за два тика
    }

    [Fact]
    public async Task RunAsync_AllConverged_DropsProgressKey()
    {
        // Arrange — расхождений нет (лимиты broker1 сходятся с декларацией),
        // но прогресс-ключ висит (последний рестарт)
        _driver.Limits["kfw-events-broker1"] = new(2_000_000_000L, 4L << 30); // сходится
        _etcd.Seed($"/kafkaworker/regens/{Cluster}",
            """{"brokers_total":1,"brokers_remaining":1,"current_broker":"broker1","updated_unix":1756500000,"instance":"i1"}""");
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert — сходимость: ключ удалён, журнал done
        result.IsSuccess.Should().BeTrue();
        (await KeyAsync($"/kafkaworker/regens/{Cluster}")).Should().BeNull();
        var state = await _journal.ReadAsync(Cluster, CancellationToken.None);
        state.Value!.Phase.Should().Be("done");
    }

    [Fact]
    public async Task RunAsync_MissingContainer_SkipsNode()
    {
        // Arrange — контейнера broker1 нет (надзор восстановит) — не кандидат
        _driver.NodeObjects.Remove("kfw-events-broker1");
        var snap = await SnapshotAsync();

        // Act
        var result = await _regen.RunAsync(snap, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        _driver.Removed.Should().BeEmpty();
    }

    [Fact]
    public async Task RunAsync_NoClaim_Fails()
    {
        // Arrange — «чужой» инстанс: свой ClaimStore НЕ держит клэйм кластера
        // (клэйм захвачен _claims в InitializeAsync и FakeEtcd-txn не отдаст
        // его повторным TryClaim — поэтому чужой строится без захвата;
        // ревью Фазы 4, замечание 3)
        var stranger = Stranger();
        var snap = await SnapshotAsync();

        // Act
        var result = await stranger.RunAsync(snap, CancellationToken.None);

        // Assert — клэйм не наш: мутации запрещены, брокеры не тронуты
        result.IsSuccess.Should().BeFalse();
        _driver.Removed.Should().BeEmpty();
    }
}
