using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Etcd;
using AdminPanel.Probes.S3;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AdminPanel.IntegrationTests;

// On-demand objects (AC7, t08 spec §7.7): пагинация list-v2 (maxKeys +
// nextContinuationToken), гварды prefix (форма + принадлежность кластерам
// снапшота/S3-дерева — Решение 11) и maxKeys, 401 без cookie.
[Collection("backups")]
public class BackupsObjectsApiTests(
    EtcdContainerFixture etcd, MinioContainerFixture minio)
    : IClassFixture<EtcdContainerFixture>, IClassFixture<MinioContainerFixture>,
      IAsyncDisposable
{
    private readonly BackupsWebFactory _factory = new()
    {
        EtcdEndpoint = etcd.Endpoint,
        MinioEndpoint = minio.HostEndpoint,
        MinioBucket = minio.Bucket,
    };

    // Общая раскладка: 4 объекта под demo/s1/ (две страницы по 2) + сирота в дереве.
    private async Task SeedAndTickAsync()
    {
        _factory.EnsureBuilt(); // до любого Services — иначе ленивый build без очистки кеша
        await EtcdSeed.SeedBackupsAsync(
            etcd.Endpoint, TestContext.Current.CancellationToken);
        await minio.SeedObjectsAsync(
        [
            ("demo/s1/pag/a", "a"),
            ("demo/s1/pag/b", "b"),
            ("demo/s1/pag/c", "c"),
            ("demo/s1/pag/d", "d"),
            ("ghost_shard/s9/full/b9/base.tar", new string('a', 50)),
        ], TestContext.Current.CancellationToken);
        var loop = _factory.Services.GetRequiredService<MinioInventoryLoop>();
        var refresher = _factory.Services.GetRequiredService<SnapshotRefresher>();
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        (await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task Objects_Pagination()
    {
        // Arrange: 4 объекта под demo/s1/, снапшот с деревом.
        await SeedAndTickAsync();
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act: первая страница — maxKeys=2.
        using var page1 = await client.GetAsync(
            "/api/backups/objects?prefix=demo/s1/&maxKeys=2",
            TestContext.Current.CancellationToken);

        // Assert: 2 объекта + токен продолжения.
        page1.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto1 = await page1.Content.ReadFromJsonAsync<JsonElement>(
            TestContext.Current.CancellationToken);
        dto1.GetProperty("items").GetArrayLength().Should().Be(2);
        var token = dto1.GetProperty("nextContinuationToken").GetString();
        token.Should().NotBeNullOrEmpty();

        // Act: вторая страница с токеном.
        using var page2 = await client.GetAsync(
            $"/api/backups/objects?prefix=demo/s1/&maxKeys=2&continuationToken={Uri.EscapeDataString(token!)}",
            TestContext.Current.CancellationToken);

        // Assert: оставшиеся 2, токена нет (конец списка).
        page2.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto2 = await page2.Content.ReadFromJsonAsync<JsonElement>(
            TestContext.Current.CancellationToken);
        dto2.GetProperty("items").GetArrayLength().Should().Be(2);
        dto2.GetProperty("nextContinuationToken").ValueKind
            .Should().Be(JsonValueKind.Null);
    }

    [Fact]
    public async Task Objects_Guards_And_OrphanPrefix()
    {
        // Arrange: 4 объекта + сирота ghost_shard/s9 в дереве.
        await SeedAndTickAsync();
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act + Assert: форма prefix — 400 (заглавная/цифра первой).
        using var badForm = await client.GetAsync(
            "/api/backups/objects?prefix=Bad%2Fx", TestContext.Current.CancellationToken);
        badForm.StatusCode.Should().Be(HttpStatusCode.BadRequest);

        // Act + Assert: maxKeys вне 1..1000 — 400.
        using var maxKeysHigh = await client.GetAsync(
            "/api/backups/objects?prefix=demo/s1/&maxKeys=1001",
            TestContext.Current.CancellationToken);
        maxKeysHigh.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        using var maxKeysZero = await client.GetAsync(
            "/api/backups/objects?prefix=demo/s1/&maxKeys=0",
            TestContext.Current.CancellationToken);
        maxKeysZero.StatusCode.Should().Be(HttpStatusCode.BadRequest);

        // Act + Assert: форма валидна, но кластера freepath нет ни в etcd,
        // ни в S3-дереве — 400 «префикс вне грани» (AC7).
        using var unknown = await client.GetAsync(
            "/api/backups/objects?prefix=freepath/", TestContext.Current.CancellationToken);
        unknown.StatusCode.Should().Be(HttpStatusCode.BadRequest);

        // Act + Assert: сирота просматриваема — кластер из S3-дерева (Решение 11).
        using var ghost = await client.GetAsync(
            "/api/backups/objects?prefix=ghost_shard/s9/", TestContext.Current.CancellationToken);
        ghost.StatusCode.Should().Be(HttpStatusCode.OK);
        var ghostDto = await ghost.Content.ReadFromJsonAsync<JsonElement>(
            TestContext.Current.CancellationToken);
        ghostDto.GetProperty("items").GetArrayLength().Should().Be(1);
    }

    [Fact]
    public async Task Objects_WithoutCookie_Unauthorized()
    {
        // Arrange: хост без логина (свежий клиент, cookie нет).
        await SeedAndTickAsync();
        _factory.EnsureBuilt();
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.GetAsync(
            "/api/backups/objects?prefix=demo/s1/", TestContext.Current.CancellationToken);

        // Assert: default-deny guard /api/* — 401.
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    public async ValueTask DisposeAsync() => await _factory.DisposeAsync();
}

// Секреты (AC8, t08 spec §7.8): ни один ответ API грани не несёт AccessKey/
// SecretKey и их значений — проверка сериализованных тел всех трёх эндпоинтов.
[Collection("backups")]
public class BackupsSecretsApiTests(
    EtcdContainerFixture etcd, MinioContainerFixture minio)
    : IClassFixture<EtcdContainerFixture>, IClassFixture<MinioContainerFixture>,
      IAsyncDisposable
{
    private readonly BackupsWebFactory _factory = new()
    {
        EtcdEndpoint = etcd.Endpoint,
        MinioEndpoint = minio.HostEndpoint,
        MinioBucket = minio.Bucket,
    };

    [Fact]
    public async Task Responses_Contain_NoSecrets()
    {
        _factory.EnsureBuilt(); // до любого Services — иначе ленивый build без очистки кеша
        // Arrange: полный контур + тики; логин — креды ходят только в запросах панели к MinIO.
        await EtcdSeed.SeedBackupsAsync(
            etcd.Endpoint, TestContext.Current.CancellationToken);
        await minio.SeedObjectsAsync(
        [
            ("demo/s1/full/b1/base.tar", new string('a', 100)),
            ("demo/s1/wal/000000010000000000000002", new string('a', 16)),
        ], TestContext.Current.CancellationToken);
        var loop = _factory.Services.GetRequiredService<MinioInventoryLoop>();
        var refresher = _factory.Services.GetRequiredService<SnapshotRefresher>();
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        (await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act: тела всех трёх эндпоинтов грани.
        var bodies = new List<string>();
        foreach (var url in new[]
                 {
                     "/api/backups/storage",
                     "/api/backups/storage/demo/s1",
                     "/api/backups/objects?prefix=demo/s1/",
                 })
        {
            using var response = await client.GetAsync(
                url, TestContext.Current.CancellationToken);
            response.StatusCode.Should().Be(HttpStatusCode.OK, url);
            bodies.Add(await response.Content.ReadAsStringAsync(
                TestContext.Current.CancellationToken));
        }

        // Assert: ни значений кредов сида, ни имён полей ключей в телах (AC8).
        foreach (var body in bodies)
        {
            body.Should().NotContain("minioadmin");
            body.Should().NotContain("AccessKey");
            body.Should().NotContain("SecretKey");
            body.Should().NotContain("accessKey");
            body.Should().NotContain("secretKey");
        }
    }

    public async ValueTask DisposeAsync() => await _factory.DisposeAsync();
}
