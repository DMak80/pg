using System.Net;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ValkeyWorker.IntegrationTests.Etcd;
using Xunit;

namespace ValkeyWorker.IntegrationTests.Api;

// POST /api/restart (порт RestartApiTests kfw): 202 {"restarting":true};
// отложенный StopApplication. Проверяем РЕАЛЬНЫЙ StopApplication.
public class RestartApiTests
{
    private sealed class RestartHost : IAsyncLifetime
    {
        public ValkeyEtcdFixture Etcd { get; } = new();
        private ValkeyApiFactory? _factory;

        public async ValueTask InitializeAsync()
        {
            await Etcd.InitializeAsync();
            _factory = new ValkeyApiFactory(Etcd);
        }

        public HttpClient CreateClient() => _factory!.CreateClient();

        public IHostApplicationLifetime Lifetime =>
            _factory!.Services.GetRequiredService<IHostApplicationLifetime>();

        public async ValueTask DisposeAsync()
        {
            if (_factory is { } factory)
                await factory.DisposeAsync();
            await Etcd.DisposeAsync();
        }
    }

    [Fact]
    public async Task Restart_202ХостЖивПослеОтветаЗатемСтоп()
    {
        // Arrange: своя фабрика; маркер стопа — ApplicationStopping.
        await using var host = new RestartHost();
        await host.InitializeAsync();
        using var client = host.CreateClient();
        var stopping = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        host.Lifetime.ApplicationStopping.Register(() => stopping.TrySetResult());

        // Act: POST без тела.
        using var response = await client.PostAsync(
            "/api/restart", null, TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 202 сразу; StopApplication — отложенно (~1 c).
        response.StatusCode.Should().Be(HttpStatusCode.Accepted);
        body.Should().Contain("\"restarting\":true");
        stopping.Task.IsCompleted.Should().BeFalse();
        var winner = await Task.WhenAny(stopping.Task, Task.Delay(3000, TestContext.Current.CancellationToken));
        winner.Should().Be(stopping.Task);
    }
}
