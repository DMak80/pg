using Microsoft.Extensions.Diagnostics.HealthChecks;
using PgWorker.App;
using PgWorker.App.HealthChecks;
using PgWorker.Core;
using PgWorker.Etcd.Coordination;
using PgWorker.UnitTests.Provisioning;

namespace PgWorker.UnitTests.App;

// Health checks (задача 24; spec §8): PgWorkerHealth отдаёт все секции §8 из
// fake-провайдера состояний; недоступный etcd → Degraded; обёртка
// HealthCheckAbstract транслирует Inited/Working/StatusError.
public class HealthTests
{
    private static readonly FixedOptionsMonitor Options = new(new PgWorkerOptions
    {
        Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
        Docker = new DockerOptions { Hosts = [] },
    });

    private static ServiceProbes Probes(PgWorker.Etcd.Client.IEtcdGateway etcd)
        => new(etcd, Options, new PgWorker.Docker.Engine.DockerEngineFactory());

    [Fact]
    public async Task Check_AllSectionsPresentInData()
    {
        // Arrange — живой etcd (fake), тики циклов и снапшот есть
        var etcd = new Fakes.FakeEtcd();
        var health = new HealthState(TimeProvider.System);
        health.MarkEtcdOk();
        health.MarkReconcileTick(ok: true, claimsHeld: 1);
        health.MarkKeepaliveTick();
        health.MarkSnapshotTick();
        health.MarkSnapshotTaken();
        var claims = new ClaimStore(["http://etcd:2379"], etcd, TimeProvider.System);
        var check = new PgWorkerHealth(Probes(etcd), health, claims, Options, TimeProvider.System);

        // Act
        var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        // Assert — все пять секций §8 в Data, статус Healthy
        result.Status.Should().Be(HealthStatus.Healthy);
        result.Data.Keys.Should().Contain("etcd");
        result.Data.Keys.Should().Contain("docker-hosts");
        result.Data.Keys.Should().Contain("loops");
        result.Data.Keys.Should().Contain("claims");
        result.Data.Keys.Should().Contain("snapshot");
        result.Data["etcd"].ToString().Should().Be("reachable");
        result.Data["claims"].ToString().Should().Contain("held=1");
    }

    [Fact]
    public async Task Check_EtcdUnreachable_Degraded()
    {
        // Arrange — etcd не отвечает ни на одном endpoint
        var health = new HealthState(TimeProvider.System);
        health.MarkEtcdOk();
        health.MarkReconcileTick(ok: true, claimsHeld: 0);
        health.MarkKeepaliveTick();
        health.MarkSnapshotTick();
        health.MarkSnapshotTaken();
        var claims = new ClaimStore(["http://etcd:2379"], new DeadEtcd(), TimeProvider.System);
        var check = new PgWorkerHealth(Probes(new DeadEtcd()), health, claims, Options, TimeProvider.System);

        // Act
        var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        // Assert — сервис жив, но требует внимания
        result.Status.Should().Be(HealthStatus.Degraded);
        result.Data["etcd"].ToString().Should().Contain("недоступен");
    }

    [Fact]
    public async Task Check_NoLoopTicks_Degraded()
    {
        // Arrange — etcd жив, но циклы ещё не тикали (старт)
        var etcd = new Fakes.FakeEtcd();
        var check = new PgWorkerHealth(
            Probes(etcd), new HealthState(TimeProvider.System),
            new ClaimStore(["http://etcd:2379"], etcd, TimeProvider.System), Options, TimeProvider.System);

        // Act
        var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        // Assert — не тикавшие циклы дают Degraded
        result.Status.Should().Be(HealthStatus.Degraded);
        result.Data["loops"].ToString().Should().Contain("reconcile=нет тиков");
    }

    [Fact]
    public async Task Abstract_StatusError_Unhealthy()
    {
        // Arrange — цикл с ошибкой последнего тика
        var service = new FakeLoopState
        {
            Inited = true,
            Working = true,
            StatusError = Result.Failed(new ApplicationException("тик не прошёл")),
        };

        // Act
        var result = await new HealthCheckAbstract<FakeLoopState>(service).CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        // Assert
        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Contain("FakeLoopState");
    }

    [Theory]
    [InlineData(false, true, HealthStatus.Degraded)]   // старт: Inited=false
    [InlineData(true, true, HealthStatus.Healthy)]     // работает
    [InlineData(true, false, HealthStatus.Unhealthy)]  // остановлен
    public async Task Abstract_Lifecycle_TranslatedToStatus(
        bool inited, bool working, HealthStatus expected)
    {
        // Arrange
        var service = new FakeLoopState { Inited = inited, Working = working };

        // Act
        var result = await new HealthCheckAbstract<FakeLoopState>(service).CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        // Assert
        result.Status.Should().Be(expected);
    }

    private sealed class FakeLoopState : IHealthCheckService
    {
        public bool Inited { get; init; }

        public bool Working { get; init; }

        public Result StatusError { get; init; } = Result.Success();
    }
}
