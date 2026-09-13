using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Probes.S3;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Серия грани «Хранилище бэкапов» (t08): классы серии выполняются последовательно
// (коллекция без общих фикстур — контейнеры свои у каждого класса, изоляция
// e2e-isolation §1 за счёт IClassFixture на класс).
[CollectionDefinition("backups")]
public sealed class BackupsCollection;

// Логин в хосте BackupsWebFactory: реальное время — окно rate-limiter'а общее,
// на класс не более 5 логинов (LoginRateLimiter.MaxAttempts).
internal static class BackupsLogin
{
    public static async Task<HttpClient> LoginAsync(BackupsWebFactory factory)
    {
        factory.EnsureBuilt();
        var client = factory.CreateClient();
        var login = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);
        login.StatusCode.Should().Be(HttpStatusCode.NoContent);
        return client;
    }
}

// AC1 (t08 spec §7.1): без AdminPanel:Backups:S3:Endpoint панель стартует,
// GET /api/backups/storage → 200 {configured:false}, инвентарь-тик — no-op
// (стоп пуст), алерт backup-s3-unreachable не горит.
[Collection("backups")]
public class BackupsStorageDisabledApiTests(EtcdContainerFixture etcd)
    : IClassFixture<EtcdContainerFixture>, IAsyncDisposable
{
    private readonly BackupsWebFactory _factory = BackupsWebFactory.WithoutMinio(etcd.Endpoint);

    [Fact]
    public async Task Storage_Disabled_WhenEndpointEmpty()
    {
        // Arrange: хост без секции AdminPanel:Backups; один etcd-тик — снапшот
        // собран, MinioStorage нет (тик loop ещё не двигали); тик loop — no-op.
        var refresher = _factory.Services.GetRequiredService<SnapshotRefresher>();
        (await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        var loop = _factory.Services.GetRequiredService<MinioInventoryLoop>();
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        _factory.Services.GetRequiredService<MinioInventoryStore>().Current
            .Should().BeNull("тик без конфигурации не пишет стор");
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act
        using var response = await client.GetAsync(
            "/api/backups/storage", TestContext.Current.CancellationToken);
        using var alerts = await client.GetAsync(
            "/api/alerts", TestContext.Current.CancellationToken);

        // Assert: 200; configured=false; кластеров нет; штампа инвентаря нет;
        // алерт backup-s3-unreachable отсутствует (MinioStorage null).
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(
            TestContext.Current.CancellationToken);
        dto.GetProperty("configured").GetBoolean().Should().BeFalse();
        dto.GetProperty("clusters").GetArrayLength().Should().Be(0);
        dto.GetProperty("inventoryUpdatedUnix").GetInt64().Should().Be(0);
        alerts.StatusCode.Should().Be(HttpStatusCode.OK);
        var alertList = await alerts.Content.ReadFromJsonAsync<JsonElement>(
            TestContext.Current.CancellationToken);
        alertList.EnumerateArray()
            .Should().NotContain(
                a => a.GetProperty("kind").GetString() == "backup-s3-unreachable",
                "грань выключена — алерт молчит");
    }

    public async ValueTask DisposeAsync() => await _factory.DisposeAsync();
}

// Байндинг простых листьев AdminPanel:Backups:S3:* (план Task 9 Step 4,
// Решение 9: тот же класс байндинга, что env AdminPanel__Backups__S3__* —
// митигация урока WORKERTLS). Хост не тикает (hosted сняты), контейнеры
// MinIO/etcd не нужны — строки endpoint'ов произвольные.
[Collection("backups")]
public class BackupsOptionsBindingApiTests : IAsyncDisposable
{
    private readonly BackupsWebFactory _factory = new()
    {
        EtcdEndpoint = "http://localhost:59997",
        MinioEndpoint = "http://localhost:59998",
        MinioBucket = "apm-bind-bucket",
    };

    [Fact]
    public void Options_Bind_FromConfiguration()
    {
        // Arrange: хост с настройками MinIO-грани через UseSetting
        // (env-эквивалент AdminPanel__Backups__S3__*).
        _factory.EnsureBuilt();

        // Act
        var options = _factory.Services.GetRequiredService<IOptions<MinioOptions>>().Value;

        // Assert: все листья доехали до [Config]-POCO, грань помечена настроенной.
        options.IsConfigured.Should().BeTrue();
        options.S3.Endpoint.Should().Be("http://localhost:59998");
        options.S3.Bucket.Should().Be("apm-bind-bucket");
        options.S3.AccessKey.Should().Be(MinioContainerFixture.AccessKey);
        options.S3.SecretKey.Should().Be(MinioContainerFixture.SecretKey);
        options.S3.PathStyle.Should().BeTrue();
        options.IntervalSec.Should().Be(60);
        options.TimeoutSec.Should().Be(5);
    }

    public async ValueTask DisposeAsync() => await _factory.DisposeAsync();
}
