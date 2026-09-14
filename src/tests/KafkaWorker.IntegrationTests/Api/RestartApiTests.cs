using System.Net;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using KafkaWorker.IntegrationTests.Etcd;
using Xunit;

namespace KafkaWorker.IntegrationTests.Api;

// POST /api/restart (spec §3.2 п.2/§4.4, симметрично PgWorker): 202
// {"restarting":true}; заголовок X-Requested-By — в лог; отложенный
// StopApplication. Подмена IHostApplicationLifetime в DI запрещена хостом —
// проверяем РЕАЛЬНЫЙ StopApplication на собственной фабрике.
public class RestartApiTests
{
    // Свой etcd + своя фабрика на тест-класс (teardown при любом исходе).
    private sealed class RestartHost : IAsyncLifetime
    {
        public EtcdFixture Etcd { get; } = new();
        private KafkaApiFactory? _factory;

        public async ValueTask InitializeAsync()
        {
            await Etcd.InitializeAsync();
            _factory = new KafkaApiFactory(Etcd);
        }

        public HttpClient CreateClient() => _factory!.CreateClient();

        // Реальный lifetime построенного хоста (после CreateClient хост собран).
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
    public async Task Restart_Returns202_AndStopsHostAfterDelay()
    {
        // Arrange: своя фабрика; реальный lifetime — ApplicationStopping и есть маркер стопа
        await using var host = new RestartHost();
        await host.InitializeAsync();
        using var client = host.CreateClient();
        var stopping = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        host.Lifetime.ApplicationStopping.Register(() => stopping.TrySetResult());

        // Act: POST без тела (spec §3.2 п.2 — тело отсутствует)
        using var response = await client.PostAsync(
            "/api/restart", null, TestContext.Current.CancellationToken);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);

        // Assert: 202 {"restarting":true} сразу; StopApplication — отложенно (~1 c)
        response.StatusCode.Should().Be(HttpStatusCode.Accepted);
        body.Should().Contain("\"restarting\":true");
        stopping.Task.IsCompleted.Should().BeFalse(); // ответ ушёл ДО стопа
        var winner = await Task.WhenAny(stopping.Task, Task.Delay(3000, TestContext.Current.CancellationToken));
        winner.Should().Be(stopping.Task); // graceful stop после ~1 c
    }
}
