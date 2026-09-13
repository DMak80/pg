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

// AC2+AC6 (t08 spec §7.2/§7.6): MinIO с сеянными объектами <C>/<X>/full/<id>/…,
// wal/<segment>, wal/<tli>.history, посторонний корневой префикс → после тиков
// сводка отдаёт buckets, health (api/live/cluster), дерево <C>/<X> с размерами/
// счётчиками, foreign-префикс; usedBytes = сумме; ключ /pgworker/backups/storage
// (WARN) — рядом с live-инвентарём.
[Collection("backups")]
public class BackupsStorageInventoryApiTests(
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
    public async Task Storage_Summary_Tree_Health_And_Quota()
    {
        // Arrange: бэкап-ключи в etcd + сеяные объекты в своём MinIO; тики
        // двигает тест: инвентарь (loop) → снапшот с MinioStorage (refresher).
        await EtcdSeed.SeedBackupsAsync(
            etcd.Endpoint, TestContext.Current.CancellationToken);
        await minio.SeedObjectsAsync(
        [
            ("demo/s1/full/b1/base.tar", new string('a', 100)),
            ("demo/s1/full/b1/pg_wal/000000010000000000000001", new string('a', 16)),
            ("demo/s1/wal/000000010000000000000002", new string('a', 16)),
            ("demo/s1/wal/00000002.history", new string('a', 4)),
            ("ghost-shard/s9/full/b9/base.tar", new string('a', 50)),
            ("loose/file.bin", new string('a', 10)),
        ], TestContext.Current.CancellationToken);
        var loop = _factory.Services.GetRequiredService<MinioInventoryLoop>();
        var refresher = _factory.Services.GetRequiredService<SnapshotRefresher>();
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        (await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act
        using var response = await client.GetAsync(
            "/api/backups/storage", TestContext.Current.CancellationToken);

        // Assert: 200; configured + health по всем трём пробам; bucket в списке.
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(
            TestContext.Current.CancellationToken);
        dto.GetProperty("configured").GetBoolean().Should().BeTrue();
        var health = dto.GetProperty("health");
        health.GetProperty("apiOk").GetBoolean().Should().BeTrue();
        health.GetProperty("liveOk").GetBoolean().Should().BeTrue();
        health.GetProperty("clusterOk").GetBoolean().Should().BeTrue();
        dto.GetProperty("buckets")
            .EnumerateArray().Select(b => b.GetString())
            .Should().Contain(minio.Bucket);

        // Assert: дерево <C>/<X> — demo/s1 (136 = full 116 + wal 16 + history 4,
        // 1 сегмент WAL), сирота ghost-shard/s9 не скрыта; foreign-префикс loose.
        var clusters = dto.GetProperty("clusters").EnumerateArray().ToList();
        var demo = clusters.Single(c => c.GetProperty("cluster").GetString() == "demo");
        var s1 = demo.GetProperty("shards").EnumerateArray()
            .Single(s => s.GetProperty("shard").GetString() == "s1");
        s1.GetProperty("sizeBytes").GetInt64().Should().Be(136);
        s1.GetProperty("walSegmentCount").GetInt64().Should().Be(1);
        s1.GetProperty("fullsCount").GetInt32().Should().Be(1);
        s1.GetProperty("orphan").GetBoolean().Should().BeFalse();
        var ghost = clusters.Single(c => c.GetProperty("cluster").GetString() == "ghost-shard");
        var s9 = ghost.GetProperty("shards").EnumerateArray()
            .Single(s => s.GetProperty("shard").GetString() == "s9");
        s9.GetProperty("orphan").GetBoolean().Should().BeTrue();
        s9.GetProperty("hasS3Only").GetBoolean().Should().BeTrue();
        dto.GetProperty("foreignPrefixes")
            .EnumerateArray().Select(f => f.GetString())
            .Should().Contain("loose");

        // Assert: место — live-факт инвентаря (196) рядом с вердиктом воркера
        // (ключ storage: used 1000 / quota 10000 / WARN); штамп тика свежий.
        dto.GetProperty("liveUsedBytes").GetInt64().Should().Be(196);
        var etcdStorage = dto.GetProperty("etcd");
        etcdStorage.GetProperty("usedBytes").GetInt64().Should().Be(1000);
        etcdStorage.GetProperty("quotaBytes").GetInt64().Should().Be(10000);
        etcdStorage.GetProperty("state").GetString().Should().Be("WARN");
        dto.GetProperty("inventoryUpdatedUnix").GetInt64().Should().BeGreaterThan(0);
        dto.GetProperty("inventoryError").ValueKind.Should().Be(JsonValueKind.Null);
    }

    public async ValueTask DisposeAsync() => await _factory.DisposeAsync();
}

// AC3 (t08 spec §7.3): остановленный MinIO → apiOk=false/liveOk=false, инвентарь
// устаревает (штамп прежний, inventoryError не пуст), алерт backup-s3-unreachable
// после 2 неудачных тиков подряд (ручной прогон RunOnceAsync — spec §5 фаза 2).
[Collection("backups")]
public class BackupsStorageMinioDownApiTests(
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
    public async Task Storage_StopMinio_ApiDown_Alert_AfterTwoTicks()
    {
        // Arrange: как AC2 — успешный первый тик (инвентарь + снапшот).
        await EtcdSeed.SeedBackupsAsync(
            etcd.Endpoint, TestContext.Current.CancellationToken);
        await minio.SeedObjectsAsync(
            [("demo/s1/full/b1/base.tar", new string('a', 100))],
            TestContext.Current.CancellationToken);
        var loop = _factory.Services.GetRequiredService<MinioInventoryLoop>();
        var refresher = _factory.Services.GetRequiredService<SnapshotRefresher>();
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        (await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        using var client = await BackupsLogin.LoginAsync(_factory);
        long stampBefore;
        using (var ok = await client.GetAsync(
                   "/api/backups/storage", TestContext.Current.CancellationToken))
        {
            ok.StatusCode.Should().Be(HttpStatusCode.OK);
            var okDto = await ok.Content.ReadFromJsonAsync<JsonElement>(
                TestContext.Current.CancellationToken);
            stampBefore = okDto.GetProperty("inventoryUpdatedUnix").GetInt64();
            stampBefore.Should().BeGreaterThan(0);
        }

        // Act: MinIO остановлен → два неудачных тика подряд → снапшот обновлён.
        await minio.StopAsync(TestContext.Current.CancellationToken);
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        (await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();

        // Assert: грань видит недоступность (apiOk/liveOk=false), инвентарь
        // устаревающий — штамп прежний, ошибка тика видна; алерт warning горит.
        using var response = await client.GetAsync(
            "/api/backups/storage", TestContext.Current.CancellationToken);
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(
            TestContext.Current.CancellationToken);
        dto.GetProperty("configured").GetBoolean().Should().BeTrue();
        var health = dto.GetProperty("health");
        health.GetProperty("apiOk").GetBoolean().Should().BeFalse();
        health.GetProperty("liveOk").GetBoolean().Should().BeFalse();
        dto.GetProperty("inventoryUpdatedUnix").GetInt64().Should().Be(stampBefore);
        dto.GetProperty("inventoryError").GetString().Should().NotBeNullOrEmpty();
        using var alerts = await client.GetAsync(
            "/api/alerts", TestContext.Current.CancellationToken);
        alerts.StatusCode.Should().Be(HttpStatusCode.OK);
        var alertList = await alerts.Content.ReadFromJsonAsync<JsonElement>(
            TestContext.Current.CancellationToken);
        var s3Alerts = alertList.EnumerateArray()
            .Where(a => a.GetProperty("kind").GetString() == "backup-s3-unreachable")
            .ToList();
        s3Alerts.Should().HaveCount(1, "порог — 2 тика подряд, алерт один");
        s3Alerts[0].GetProperty("severity").GetString().Should().Be("warning");
        s3Alerts[0].GetProperty("target").GetString()
            .Should().Contain(minio.Bucket);
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
