using AdminPanel.Core;
using AdminPanel.Core.Kafka.KafkaAlerting;
using AdminPanel.Core.Valkey;
using AdminPanel.Core.Valkey.ValkeyAlerting;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Панель против живого etcd (t10, spec §6 п.13): живая заявка старше
// stale-порога → warning-алерт; после экспирации → ticket-expired; после
// повторной успешной ротации → исход done, алерт погашен (AC13-тройка).
// Свои guid-кластеры; teardown чистит владельческие префиксы и ассертит чистоту.
public class RotationOutcomeSnapshotTests : IClassFixture<RotationOutcomeEtcdFixture>
{
    private readonly RotationOutcomeEtcdFixture _fx;

    public RotationOutcomeSnapshotTests(RotationOutcomeEtcdFixture fx) => _fx = fx;

    [Fact]
    public async Task RotationOutcomeAlerts_StaleThenExpiredThenDone()
    {
        // ===== Valkey-часть: stale → expired → done =====

        // Arrange 1: refresher против живого etcd; заявка старше stale-порога
        // (возраст 2000 c > 1800), исходов нет.
        var store = new ValkeySnapshotStore();
        var refresher = _fx.NewValkeyRefresher(store);

        // Act 1
        var result1 = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert 1: исходов нет; stale-алерт (warning) горит.
        result1.IsSuccess.Should().BeTrue();
        var snap1 = store.Current!;
        snap1.TicketOutcomes.Should().BeEmpty();
        snap1.Alerts.Should().Contain(a =>
            a.Kind == "valkey-rotation-stale" && a.Target == _fx.ValkeyCluster
            && a.Severity == AlertSeverity.Warning);

        // Act 2: экспирация воркером — исход expired + заявка снята; тик.
        await EtcdSeed.PutAsync(_fx.Endpoint, $"/valkeyworker/ticket_outcomes/{_fx.ValkeyCluster}",
            """{"kind":"password-app","outcome":"expired","reason":"waiting-cluster","requested_unix":1750000000,"requested_by":"it","finished_unix":1750003600}""",
            CancellationToken.None);
        (await EtcdTestHarness.NewGateway().DeleteAsync(
                _fx.Endpoint, $"/valkeyworker/rotations/{_fx.ValkeyCluster}",
                prefix: false, CancellationToken.None))
            .IsSuccess.Should().BeTrue();
        var result2 = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert 2: заявки нет → stale погас; ticket-expired (warning) горит.
        result2.IsSuccess.Should().BeTrue();
        var snap2 = store.Current!;
        snap2.Alerts.Should().NotContain(a => a.Kind == "valkey-rotation-stale");
        snap2.Alerts.Should().Contain(a =>
            a.Kind == "valkey-ticket-expired" && a.Target == _fx.ValkeyCluster
            && a.Severity == AlertSeverity.Warning);

        // Act 3: повторная успешная ротация — исход done перезаписью; тик.
        await EtcdSeed.PutAsync(_fx.Endpoint, $"/valkeyworker/rotations/{_fx.ValkeyCluster}",
            """{"role":"app","requested_unix":1750004000,"requested_by":"it"}""", CancellationToken.None);
        await EtcdSeed.PutAsync(_fx.Endpoint, $"/valkeyworker/ticket_outcomes/{_fx.ValkeyCluster}",
            """{"kind":"password-app","outcome":"done","requested_unix":1750004000,"finished_unix":1750004100}""",
            CancellationToken.None);
        var result3 = await refresher.RefreshOnceAsync(CancellationToken.None);

        // Assert 3: expired погашен перезаписью (done не алертится).
        result3.IsSuccess.Should().BeTrue();
        store.Current!.Alerts.Should().NotContain(a => a.Kind == "valkey-ticket-expired");

        // ===== Kafka-часть: rotation-stale + ca-rotation-pending + ticket-expired =====

        // Arrange: живой guid-кластер kafka + старая заявка + ca-заявка +
        // исход expired.
        var kafkaStore = new KafkaSnapshotStore();
        var kafkaRefresher = _fx.NewKafkaRefresher(kafkaStore);

        // Act
        var kafkaResult = await kafkaRefresher.RefreshOnceAsync(CancellationToken.None);

        // Assert: все три kinds на живом кластере.
        kafkaResult.IsSuccess.Should().BeTrue();
        var kafkaSnap = kafkaStore.Current!;
        kafkaSnap.Alerts.Should().Contain(a =>
            a.Kind == "kafka-rotation-stale" && a.Target == _fx.KafkaCluster
            && a.Severity == AlertSeverity.Warning);
        kafkaSnap.Alerts.Should().Contain(a =>
            a.Kind == "kafka-ca-rotation-pending" && a.Target == _fx.KafkaCluster
            && a.Severity == AlertSeverity.Info);
        kafkaSnap.Alerts.Should().Contain(a =>
            a.Kind == "kafka-ticket-expired" && a.Target == _fx.KafkaCluster
            && a.Severity == AlertSeverity.Warning);
        kafkaSnap.CaRotations.Should().ContainSingle(t => t.Cluster == _fx.KafkaCluster);
        kafkaSnap.TicketOutcomes.Should().ContainSingle(t => t.Cluster == _fx.KafkaCluster);
    }
}

// Фикстура: свой etcd-контейнер (динамический порт) + guid-кластеры valkey и
// kafka с заявками/исходами t10. Teardown при любом исходе: prefix-delete
// владельческих доменов + ассерт чистоты + удаление контейнера.
public sealed class RotationOutcomeEtcdFixture : IAsyncLifetime
{
    private readonly EtcdContainerFixture _etcd = new();

    // Управляемое время: stale-порог оценивается от BuiltAtUtc снапшота —
    // возраст заявок считается от фиксированного «сейчас».
    private readonly FixedTimeProvider _clock = new()
    {
        Utc = new DateTimeOffset(2026, 10, 8, 12, 0, 0, TimeSpan.Zero),
    };

    public string Endpoint => _etcd.Endpoint;

    // guid-префикс владельческих ключей (изоляция сценариев, AGENTS.md).
    public string Prefix { get; } = $"t10{Guid.NewGuid():N}";

    public string ValkeyCluster => $"{Prefix}v";

    public string KafkaCluster => $"{Prefix}k";

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _etcd.InitializeAsync();
        var nowUnix = _clock.GetUtcNow().ToUnixTimeSeconds();

        // Valkey: Active-кластер + заявка старше stale-порога (1800 c).
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{ValkeyCluster}/config",
            """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{ValkeyCluster}/endpoints", "host.docker.internal:17001", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{ValkeyCluster}/nodes/node1/state", "RUNNING", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkey/clusters/{ValkeyCluster}/ca_pem",
            "-----BEGIN CERTIFICATE-----\nMII\n-----END CERTIFICATE-----", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/valkeyworker/rotations/{ValkeyCluster}",
            $$"""{"role":"app","requested_unix":{{nowUnix - 2000}},"requested_by":"it"}""", ct);

        // Kafka: кластер-скелет (живой для движка) + старая заявка + ca-заявка
        // + исход expired.
        await EtcdSeed.PutAsync(Endpoint, $"/kafka/clusters/{KafkaCluster}/config",
            """{"brokers":1,"replication_factor":1,"min_insync_replicas":1,"default_partitions":3,"default_retention_ms":604800000,"created_unix":1756500000}""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/kafkaworker/rotations/{KafkaCluster}",
            $$"""{"requested_unix":{{nowUnix - 2000}},"requested_by":"it"}""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/kafkaworker/ca_rotations/{KafkaCluster}",
            $$"""{"requested_unix":{{nowUnix - 100}},"requested_by":"it"}""", ct);
        await EtcdSeed.PutAsync(Endpoint, $"/kafkaworker/ticket_outcomes/{KafkaCluster}",
            """{"kind":"ca","outcome":"expired","reason":"waiting-password-rotation","requested_unix":1750000000,"requested_by":"it","finished_unix":1750003600}""",
            ct);
    }

    public async ValueTask DisposeAsync()
    {
        // Полный teardown при любом исходе: владельческие префиксы удаляются,
        // чистота ассертится; затем контейнер. Контур свой (чужих ключей нет).
        var gateway = EtcdTestHarness.NewGateway();
        foreach (var prefix in new[] { "/valkey/", "/valkeyworker/", "/kafka/", "/kafkaworker/" })
            await gateway.DeleteAsync(Endpoint, prefix, prefix: true, CancellationToken.None);

        var leftoversValkey = await gateway.RangeAsync(Endpoint, "/valkey", CancellationToken.None);
        var leftoversKafka = await gateway.RangeAsync(Endpoint, "/kafka", CancellationToken.None);
        if ((leftoversValkey.IsSuccess && leftoversValkey.Value.Count > 0)
            || (leftoversKafka.IsSuccess && leftoversKafka.Value.Count > 0))
            throw new InvalidOperationException(
                "teardown-ассерт чистоты: остались ключи доменов после prefix-delete");

        await _etcd.DisposeAsync();
    }

    // Refresher'ы против живого etcd на управляемых часах (конструирование
    // без host'а — паттерн EtcdTestHarness/ValkeyEtcdFixture).
    public ValkeySnapshotRefresher NewValkeyRefresher(ValkeySnapshotStore store)
        => new(
            EtcdTestHarness.NewGateway(),
            new ValkeyAlertEngine(Options.Create(new ValkeyAlertsOptions())),
            store,
            new ValkeySecretsStore(),
            Options.Create(new EtcdOptions { Endpoints = [Endpoint] }),
            Options.Create(new ValkeyPanelOptions()),
            _clock,
            NullLogger<ValkeySnapshotRefresher>.Instance,
            new ValkeyWorkerHealthStore());

    public KafkaSnapshotRefresher NewKafkaRefresher(KafkaSnapshotStore store)
        => new(
            EtcdTestHarness.NewGateway(),
            new KafkaAlertEngine(Options.Create(new KafkaAlertsOptions())),
            store,
            new KafkaSecretsStore(),
            Options.Create(new EtcdOptions { Endpoints = [Endpoint] }),
            Options.Create(new KafkaPanelOptions()),
            _clock,
            NullLogger<KafkaSnapshotRefresher>.Instance,
            new KafkaWorkerHealthStore());
}
