using Microsoft.Extensions.Diagnostics.HealthChecks;
using FluentAssertions;
using KafkaWorker.App;
using KafkaWorker.App.HealthChecks;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.UnitTests.Provisioning;
using Xunit;

namespace KafkaWorker.UnitTests.App;

// Catch-all проб и чека (t09; spec §3.2): сетевое исключение шлюза → Result.Failed
// (Degraded с секциями), чек никогда не падает исключением (DefaultHealthCheckService[103]).
public class HealthTests
{
    private static readonly FixedOptionsMonitor Options = new(new KafkaWorkerOptions
    {
        Etcd = new EtcdOptions { Endpoints = ["http://etcd:2379"] },
        Docker = new DockerOptions { Hosts = [] },
    });

    private static ServiceProbes Probes(KafkaWorker.Etcd.Client.IEtcdGateway etcd)
        => new(etcd, Options, new KafkaWorker.Docker.Engine.DockerEngineFactory());

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

    // Фабрика docker-клиентов, бросающая при создании (t09; spec §3.2: пер-хостовая
    // проба оборачивает исключение в Failed — структура, не бросок).
    private sealed class ThrowingFactory : KafkaWorker.Docker.Engine.DockerEngineFactory
    {
        public override KafkaWorker.Docker.Engine.IDockerEngine Create(string endpoint, string? hostAlias = null)
            => throw new ApplicationException("docker engine недоступен");
    }

    [Fact]
    public async Task DockerPing_ThrowingFactory_PerHostFailed()
    {
        // Arrange: один настроенный docker-хост; фабрика бросает при создании клиента.
        var options = new FixedOptionsMonitor(new KafkaWorkerOptions
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
    // тело чека целиком (после catch-all проб): KafkaWorkerHealth обязан
    // вернуть Degraded со структурой, а не исключение.
    private sealed class ThrowingOptionsMonitor : Microsoft.Extensions.Options.IOptionsMonitor<KafkaWorkerOptions>
    {
        public KafkaWorkerOptions CurrentValue => throw new ApplicationException("конфигурация недоступна");

        public KafkaWorkerOptions Get(string? name) => throw new ApplicationException("конфигурация недоступна");

        public IDisposable? OnChange(Action<KafkaWorkerOptions, string?> listener) => null;
    }

    [Fact]
    public async Task Check_UnexpectedExceptionInside_DegradedWithStructure()
    {
        // Arrange: любая непредвиденная ошибка тела чека (тут — опции).
        var check = new KafkaWorkerHealth(
            Probes(new Fakes.FakeEtcd()), new HealthState(TimeProvider.System),
            new ClaimStore(["http://etcd:2379"], new Fakes.FakeEtcd(), TimeProvider.System),
            new ThrowingOptionsMonitor(), TimeProvider.System);

        // Act
        var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        // Assert: Degraded с данными секции error — не исключение чека.
        result.Status.Should().Be(HealthStatus.Degraded);
        result.Data.Keys.Should().Contain("error");
    }
}
