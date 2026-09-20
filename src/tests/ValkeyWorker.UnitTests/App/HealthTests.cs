using Microsoft.Extensions.Diagnostics.HealthChecks;
using FluentAssertions;
using ValkeyWorker.App;
using Shared.Etcd.Client;
using ValkeyWorker.App.HealthChecks;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.App;

// Catch-all проб и чека (t09; arch/21 §7): сетевое исключение шлюза → Result.Failed
// (Degraded с секциями), чек никогда не падает исключением.
public class HealthTests
{
    private static readonly FixedOptionsMonitor Options = new(new ValkeyWorkerOptions
    {
        Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
        Docker = new DockerOptions { Hosts = [] },
    });

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
        // Arrange: plain-режим без хостов (стендовая конфигурация по умолчанию).
        var probes = Probes(new Fakes.FakeEtcd());

        // Act
        var hosts = await probes.PingDockerHostsAsync(TestContext.Current.CancellationToken);

        // Assert: нет хостов — нет записей, не Degraded.
        hosts.Should().BeEmpty();
    }

    // Фабрика docker-клиентов, бросающая при создании (t09): пер-хостовая
    // проба оборачивает исключение в Failed — структура, не бросок.
    private sealed class ThrowingFactory : DockerEngineFactory
    {
        public override IDockerEngine Create(string endpoint, string? hostAlias = null)
            => throw new ApplicationException("docker engine недоступен");
    }

    [Fact]
    public async Task DockerPing_ThrowingFactory_PerHostFailed()
    {
        // Arrange: один настроенный docker-хост; фабрика бросает при создании клиента.
        var options = new FixedOptionsMonitor(new ValkeyWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
            Docker = new DockerOptions
            {
                Hosts = [new DockerHostOptions { Name = "h1", Endpoint = "unix:///var/run/docker.sock" }],
            },
        });
        var probes = new ServiceProbes(new Fakes.FakeEtcd(), options, new ThrowingFactory());

        // Act
        var hosts = await probes.PingDockerHostsAsync(TestContext.Current.CancellationToken);

        // Assert: per-host Failed (catch в PingAsync) — секция docker-hosts отдаст
        // Degraded с именем хоста, не исключение.
        hosts.Should().ContainKey("h1");
        hosts["h1"].IsSuccess.Should().BeFalse();
        hosts["h1"].Error!.Message.Should().Contain("docker h1");
    }

    // Опции, бросающие при чтении — единственный seam, которым можно уронить
    // тело чека целиком (после catch-all проб): ValkeyWorkerHealth обязан
    // вернуть Degraded со структурой, а не исключение.
    private sealed class ThrowingOptionsMonitor : Microsoft.Extensions.Options.IOptionsMonitor<ValkeyWorkerOptions>
    {
        public ValkeyWorkerOptions CurrentValue => throw new ApplicationException("конфигурация недоступна");

        public ValkeyWorkerOptions Get(string? name) => throw new ApplicationException("конфигурация недоступна");

        public IDisposable? OnChange(Action<ValkeyWorkerOptions, string?> listener) => null;
    }

    [Fact]
    public async Task Check_UnexpectedExceptionInside_DegradedWithStructure()
    {
        // Arrange: любая непредвиденная ошибка тела чека (тут — опции).
        var check = new ValkeyWorkerHealth(
            Probes(new Fakes.FakeEtcd()), new HealthState(TimeProvider.System),
            new ClaimStore("/valkeyworker", ["http://etcd:2379"], new Fakes.FakeEtcd(), TimeProvider.System),
            new ThrowingOptionsMonitor(), TimeProvider.System);

        // Act
        var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        // Assert: Degraded с данными секции error — не исключение чека.
        result.Status.Should().Be(HealthStatus.Degraded);
        result.Data.Keys.Should().Contain("error");
    }
}
