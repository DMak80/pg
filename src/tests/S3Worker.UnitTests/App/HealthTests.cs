using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using S3Worker.App;
using S3Worker.App.HealthChecks;
using Shared.Etcd.Client;

namespace S3Worker.UnitTests.App;

// Catch-all проб и чека (arch/24 §7): сетевое исключение шлюза → Result.Failed
// (Degraded с секциями), чек никогда не падает исключением.
public class HealthTests
{
    private static readonly FixedOptionsMonitor Options = new(new S3WorkerOptions
    {
        Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
        Docker = new DockerOptions { Hosts = [] },
    });

    // Пустой провайдер: watchdog не зарегистрирован → секция "disabled".
    private static readonly IServiceProvider EmptyServices =
        new ServiceCollection().BuildServiceProvider();

    private static ServiceProbes Probes(IEtcdGateway etcd)
        => new(etcd, Options, new DockerEngineFactory());

    [Fact]
    public async Task EtcdProbe_GatewayThrows_ReturnsFailedNotThrows()
    {
        // Arrange: шлюз бросает HttpRequestException (DNS-флейп).
        var probes = Probes(new ThrowingEtcd());

        // Act
        var result = await probes.EtcdReachableAsync(TestContext.Current.CancellationToken);

        // Assert: структура, не исключение — секция etcd отдаст Degraded с данными.
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeAssignableTo<Exception>();
        result.Error!.Message.Should().Contain("etcd-проба");
    }

    [Fact]
    public async Task EtcdProbe_HealthyGateway_ReturnsSuccess()
    {
        // Arrange: живой fake-шлюз.
        var probes = Probes(new Fakes.FakeEtcd());

        // Act
        var result = await probes.EtcdReachableAsync(TestContext.Current.CancellationToken);

        // Assert
        result.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task DockerPing_NoHosts_EmptyDictionary()
    {
        // Arrange: plain-режим без хостов (конфигурация по умолчанию в тестах).
        var probes = Probes(new Fakes.FakeEtcd());

        // Act
        var hosts = await probes.PingDockerHostsAsync(TestContext.Current.CancellationToken);

        // Assert — секция «нет настроенных хостов», не отказ
        hosts.Should().BeEmpty();
    }

    [Fact]
    public async Task HealthCheck_AlwaysReturnsStructuredResult()
    {
        // Arrange — ThrowingEtcd: etcd-проба падает; чек обязан отдать Degraded-структуру
        var health = new HealthState(TimeProvider.System);
        var claims = new ClaimStore("/s3worker", ["http://etcd:2379"], new ThrowingEtcd(),
            TimeProvider.System, "https://localhost:8080", null);
        var sut = new S3WorkerHealth(
            Probes(new ThrowingEtcd()), health, claims, Options, TimeProvider.System, EmptyServices);

        // Act
        var result = await sut.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        // Assert — Degraded с секциями в Data (не исключение)
        result.Status.Should().Be(HealthStatus.Degraded);
        result.Data.Should().ContainKey("etcd");
        result.Data.Should().ContainKey("docker-hosts");
        result.Data.Should().ContainKey("loops");
        result.Data.Should().ContainKey("watchdog");
        result.Data.Should().ContainKey("claims");
    }
}
