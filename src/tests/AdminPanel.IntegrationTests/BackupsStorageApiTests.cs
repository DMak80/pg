using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Etcd;
using AdminPanel.Probes.S3;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Серия грани «Хранилище бэкапов» : классы серии выполняются последовательно
// (коллекция без общих фикстур — контейнеры свои у каждого класса, изоляция
// e2e-isolation §1 за счёт IClassFixture на класс).
[CollectionDefinition("backups")]
public sealed class BackupsCollection;

// Логин в хосте BackupsWebFactory: реальное время — окно rate-limiter'а общее,
// на класс не более 5 логинов (LoginRateLimiter.MaxAttempts).
internal static class BackupsLogin
{
    // Тестовый пароль админа хоста фабрики (литералы — только в константах).
    private const string AdminPassword = "adminpw";

    public static async Task<HttpClient> LoginAsync(BackupsWebFactory factory)
    {
        factory.EnsureBuilt();
        var client = factory.CreateClient();
        var login = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = AdminPassword },
            TestContext.Current.CancellationToken);
        login.StatusCode.Should().Be(HttpStatusCode.NoContent);
        return client;
    }
}

// AC1 (spec §7.1): без S3-пула в /s3/clusters/ (нет Active-кластера
// с endpoints и secrets) панель стартует,
// GET /api/backups/storage → 200 {configured:false}, инвентарь-тик — no-op
// (стоп пуст), алерт backup-s3-unreachable не горит.
[Collection("backups")]
public class BackupsStorageDisabledApiTests(EtcdContainerFixture etcd)
    : IClassFixture<EtcdContainerFixture>, IAsyncDisposable
{
    private readonly BackupsWebFactory _factory = BackupsWebFactory.WithoutStorage(etcd.Endpoint);

    [Fact]
    public async Task Storage_Disabled_WhenEndpointEmpty()
    {
        // хост строится под замком до любого Services (см. EnsureBuilt)
        _factory.EnsureBuilt();
        // Arrange: хост без секции AdminPanel:Backups; один etcd-тик — снапшот
        // собран, S3Storage нет (тик loop ещё не двигали); тик loop — no-op.
        var refresher = _factory.Services.GetRequiredService<SnapshotRefresher>();
        (await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        var loop = _factory.Services.GetRequiredService<S3InventoryLoop>();
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        _factory.Services.GetRequiredService<S3InventoryStore>().Current
            .Should().BeNull("tick without configuration does not write the store");
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act
        using var response = await client.GetAsync(
            "/api/backups/storage", TestContext.Current.CancellationToken);
        using var alerts = await client.GetAsync(
            "/api/alerts", TestContext.Current.CancellationToken);

        // Assert: 200; configured=false; кластеров нет; штампа инвентаря нет;
        // алерт backup-s3-unreachable отсутствует (S3Storage null).
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
                "facet disabled — alert stays silent");
    }

    public async ValueTask DisposeAsync() => await _factory.DisposeAsync();
}

// AC2+AC6 (spec §7.2/§7.6): ownS3-нода с сеянными объектами <C>/<X>/full/<id>/…,
// wal/<segment>, wal/<tli>.history, посторонний корневой префикс → после тиков
// сводка отдаёт buckets, health (api/live/cluster), дерево <C>/<X> с размерами/
// счётчиками, foreign-префикс; usedBytes = сумме; ключ /pgworker/backups/storage
// (WARN) — рядом с live-инвентарём.
[Collection("backups")]
public class BackupsStorageInventoryApiTests(
    EtcdContainerFixture etcd, S3NodeContainerFixture s3)
    : IClassFixture<EtcdContainerFixture>, IClassFixture<S3NodeContainerFixture>,
      IAsyncDisposable
{
    private readonly BackupsWebFactory _factory = new()
    {
        EtcdEndpoint = etcd.Endpoint,
        StorageEndpoint = s3.HostEndpoint,
        StorageBucket = s3.Bucket,
    };

    [Fact]
    public async Task Storage_Summary_Tree_Health_And_Quota()
    {
        _factory.EnsureBuilt(); // до любого Services — иначе ленивый build без очистки кеша
        // Arrange: бэкап-ключи + S3-пул в etcd (цели грани — per-cluster secrets
        // + endpoints пула) + сеяные объекты в своей ownS3-ноде; тики двигает
        // тест: снапшот с пулом (цели резолвит луп) → инвентарь (loop) →
        // снапшот с инвентарём (S3Storage вносит refresher из стора).
        await EtcdSeed.SeedBackupsAsync(
            etcd.Endpoint, TestContext.Current.CancellationToken);
        await EtcdSeed.SeedS3PoolAsync(
            etcd.Endpoint, s3.HostEndpoint,
            S3NodeContainerFixture.AccessKey, S3NodeContainerFixture.SecretKey,
            TestContext.Current.CancellationToken);
        await s3.SeedObjectsAsync(
        [
            ("demo/s1/full/b1/base.tar", new string('a', 100)),
            ("demo/s1/full/b1/pg_wal/000000010000000000000001", new string('a', 16)),
            ("demo/s1/wal/000000010000000000000002", new string('a', 16)),
            ("demo/s1/wal/00000002.history", new string('a', 4)),
            ("ghost_shard/s9/full/b9/base.tar", new string('a', 50)),
            ("loose/file.bin", new string('a', 10)),
        ], TestContext.Current.CancellationToken);
        var loop = _factory.Services.GetRequiredService<S3InventoryLoop>();
        var refresher = _factory.Services.GetRequiredService<SnapshotRefresher>();
        // S3-домен — отдельный стор: его тик S3SnapshotRefresher (цели резолвит
        // луп), pg-снапшот — после инвентаря (S3Storage вносит refresher).
        (await _factory.Services.GetRequiredService<S3SnapshotRefresher>()
            .RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        (await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act
        using var response = await client.GetAsync(
            "/api/backups/storage", TestContext.Current.CancellationToken);

        // Assert: 200; configured + health; bucket в списке. ClusterOk —
        // true (02 §2.5): фикстура поднимает ownS3 с /s3/health/cluster —
        // здоровая нода отвечает 200.
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(
            TestContext.Current.CancellationToken);
        dto.GetProperty("configured").GetBoolean().Should().BeTrue();
        var health = dto.GetProperty("health");
        health.GetProperty("apiOk").GetBoolean().Should().BeTrue();
        health.GetProperty("liveOk").GetBoolean().Should().BeTrue();
        health.GetProperty("clusterOk").GetBoolean().Should().BeTrue(
            "ownS3 node responds 200 on /s3/health/cluster — ClusterOk true (02 §2.5)");
        dto.GetProperty("buckets")
            .EnumerateArray().Select(b => b.GetString())
            .Should().Contain(s3.Bucket);

        // Assert: дерево <C>/<X> — demo/s1 (136 = full 116 + wal 16 + history 4,
        // 1 сегмент WAL), сирота ghost_shard/s9 не скрыта; foreign-префикс loose.
        var clusters = dto.GetProperty("clusters").EnumerateArray().ToList();
        var demo = clusters.Single(c => c.GetProperty("cluster").GetString() == "demo");
        var s1 = demo.GetProperty("shards").EnumerateArray()
            .Single(s => s.GetProperty("shard").GetString() == "s1");
        s1.GetProperty("sizeBytes").GetInt64().Should().Be(136);
        s1.GetProperty("walSegmentCount").GetInt64().Should().Be(1);
        s1.GetProperty("fullsCount").GetInt32().Should().Be(1);
        s1.GetProperty("orphan").GetBoolean().Should().BeFalse();
        var ghost = clusters.Single(c => c.GetProperty("cluster").GetString() == "ghost_shard");
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

    // AAA : ключ /pgworker/etcd-snapshots попадает в DTO грани (панель
    // читает, пишет только PgWorker); карточка живёт независимо от инвентаря.
    [Fact]
    public async Task Storage_EtcdSnapshots_InDto()
    {
        _factory.EnsureBuilt(); // до любого Services — иначе ленивый build без очистки кеша
        // Arrange — свой etcd-класс серии: сид статус-ключа + тик снапшота
        await EtcdSeed.PutAsync(etcd.Endpoint, "/pgworker/etcd-snapshots",
            """{"enabled":true,"state":"OK","last_uploaded_unix":1759330000,"last_object":"etcd/snapshot-20261001-120000.db","last_sha256":"abc123","size_bytes":2048,"interval_min":360}""",
            TestContext.Current.CancellationToken);
        var refresher = _factory.Services.GetRequiredService<SnapshotRefresher>();
        (await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act
        using var response = await client.GetAsync(
            "/api/backups/storage", TestContext.Current.CancellationToken);

        // Assert — статус выгрузки доезжает в DTO с полями факта
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(
            TestContext.Current.CancellationToken);
        var export = dto.GetProperty("etcdSnapshots");
        export.GetProperty("enabled").GetBoolean().Should().BeTrue();
        export.GetProperty("state").GetString().Should().Be("OK");
        export.GetProperty("lastUploadedUnix").GetInt64().Should().Be(1759330000);
        export.GetProperty("lastSha256").GetString().Should().Be("abc123");
        export.GetProperty("intervalMin").GetInt32().Should().Be(360);
        export.GetProperty("sizeBytes").GetInt64().Should().Be(2048);
    }
}

// AC3 (spec §7.3): остановленное хранилище → apiOk=false/liveOk=false, инвентарь
// устаревает (штамп прежний, inventoryError не пуст), алерт backup-s3-unreachable
// после 2 неудачных тиков подряд (ручной прогон RunOnceAsync — spec §5 фаза 2).
[Collection("backups")]
public class BackupsStorageDownApiTests(
    EtcdContainerFixture etcd, S3NodeContainerFixture s3)
    : IClassFixture<EtcdContainerFixture>, IClassFixture<S3NodeContainerFixture>,
      IAsyncDisposable
{
    private readonly BackupsWebFactory _factory = new()
    {
        EtcdEndpoint = etcd.Endpoint,
        StorageEndpoint = s3.HostEndpoint,
        StorageBucket = s3.Bucket,
    };

    [Fact]
    public async Task Storage_StorageDown_ApiDown_Alert_AfterTwoTicks()
    {
        _factory.EnsureBuilt(); // до любого Services — иначе ленивый build без очистки кеша
        // Arrange: как AC2 (пул в etcd, снапшот → инвентарь → снапшот) — успешный
        // первый тик.
        await EtcdSeed.SeedBackupsAsync(
            etcd.Endpoint, TestContext.Current.CancellationToken);
        await EtcdSeed.SeedS3PoolAsync(
            etcd.Endpoint, s3.HostEndpoint,
            S3NodeContainerFixture.AccessKey, S3NodeContainerFixture.SecretKey,
            TestContext.Current.CancellationToken);
        await s3.SeedObjectsAsync(
            [("demo/s1/full/b1/base.tar", new string('a', 100))],
            TestContext.Current.CancellationToken);
        var loop = _factory.Services.GetRequiredService<S3InventoryLoop>();
        var refresher = _factory.Services.GetRequiredService<SnapshotRefresher>();
        (await _factory.Services.GetRequiredService<S3SnapshotRefresher>()
            .RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
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

        // Act: хранилище остановлено → два неудачных тика подряд → снапшот обновлён.
        await s3.StopAsync(TestContext.Current.CancellationToken);
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
        s3Alerts.Should().HaveCount(1, "threshold — 2 consecutive ticks, single alert");
        s3Alerts[0].GetProperty("severity").GetString().Should().Be("warning");
        s3Alerts[0].GetProperty("target").GetString()
            .Should().Contain(s3.Bucket);
    }

    public async ValueTask DisposeAsync() => await _factory.DisposeAsync();
}

// Байндинг простых листьев AdminPanel:Backups:S3:* (план Task 9 Step 4,
// Решение 9: тот же класс байндинга, что env AdminPanel__Backups__S3__* —
// митигация урока WORKERTLS). Хост не тикает (hosted сняты), контейнеры
// Хранилище/etcd не нужны — строки endpoint'ов произвольные.
[Collection("backups")]
public class BackupsOptionsBindingApiTests : IAsyncDisposable
{
    private readonly BackupsWebFactory _factory = new()
    {
        EtcdEndpoint = "http://localhost:59997",
        StorageEndpoint = "http://localhost:59998",
        StorageBucket = "apm-bind-bucket",
    };

    [Fact]
    public void Options_Bind_FromConfiguration()
    {
        // Arrange: хост с настройками S3-грани через UseSetting
        // (env-эквивалент AdminPanel__Backups__S3__*).
        _factory.EnsureBuilt();

        // Act
        var options = _factory.Services.GetRequiredService<IOptions<BackupsS3Options>>().Value;

        // Assert: конфиг-листья доехали до [Config]-POCO. Креды/endpoint в
        // конфиге больше нет — грань резолвит их из снапшота S3-домена
        // (per-cluster secrets, arch/adminpanel/02 §12.4).
        options.S3.Bucket.Should().Be("apm-bind-bucket");
        options.S3.PathStyle.Should().BeTrue();
        options.IntervalSec.Should().Be(60);
        options.TimeoutSec.Should().Be(5);
    }

    public async ValueTask DisposeAsync() => await _factory.DisposeAsync();
}

// Сироты с защитой (reliability, AC7): сводка orphans несёт hasValidFull/
// held/deleteRequested + hold/заявки из etcd (реестр без S3-факта — вторая
// ветка слияния); TtlLeftSec null у защищённых.
[Collection("backups")]
public class BackupsStorageOrphanProtectApiTests(
    EtcdContainerFixture etcd, S3NodeContainerFixture s3)
    : IClassFixture<EtcdContainerFixture>, IClassFixture<S3NodeContainerFixture>,
      IAsyncDisposable
{
    private readonly BackupsWebFactory _factory = new()
    {
        EtcdEndpoint = etcd.Endpoint,
        StorageEndpoint = s3.HostEndpoint,
        StorageBucket = s3.Bucket,
    };

    [Fact]
    public async Task Storage_Orphans_CarryProtectionBadges()
    {
        _factory.EnsureBuilt();
        // Arrange: реестр сирот (g/s1 с валидным полным + заявка; g/s2 под hold)
        // + hold/заявка per-префиксными ключами канона + S3-пул (цели грани);
        // тики: снапшот с пулом → инвентарь → снапшот с инвентарём.
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        await EtcdSeed.PutAsync(etcd.Endpoint, "/pgworker/backups/orphans",
            $$"""{"orphans":[{"prefix":"g/s1","kind":"cluster","size_bytes":10,"first_seen_unix":{{now - 100}},"state":"OBSERVED","has_valid_full":true},{"prefix":"g/s2","kind":"cluster","size_bytes":20,"first_seen_unix":{{now - 100}},"state":"OBSERVED"}],"updated_unix":{{now}}}""",
            TestContext.Current.CancellationToken);
        await EtcdSeed.PutAsync(etcd.Endpoint, "/pgworker/backups/orphan-holds/g/s2",
            """{"set_unix":1757760000,"set_by":"panel"}""", TestContext.Current.CancellationToken);
        await EtcdSeed.PutAsync(etcd.Endpoint, "/pgworker/backups/orphan-deletes/g/s1",
            """{"requested_unix":1757760000,"requested_by":"operator"}""", TestContext.Current.CancellationToken);
        await EtcdSeed.SeedS3PoolAsync(
            etcd.Endpoint, s3.HostEndpoint,
            S3NodeContainerFixture.AccessKey, S3NodeContainerFixture.SecretKey,
            TestContext.Current.CancellationToken);
        var loop = _factory.Services.GetRequiredService<S3InventoryLoop>();
        var refresher = _factory.Services.GetRequiredService<SnapshotRefresher>();
        (await _factory.Services.GetRequiredService<S3SnapshotRefresher>()
            .RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        (await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act
        using var response = await client.GetAsync(
            "/api/backups/storage", TestContext.Current.CancellationToken);

        // Assert: orphans несут бейджи защиты; ttlLeftSec null у защищённых.
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(
            TestContext.Current.CancellationToken);
        var orphans = dto.GetProperty("orphans").EnumerateArray().ToList();
        var s1 = orphans.Single(o => o.GetProperty("prefix").GetString() == "g/s1");
        s1.GetProperty("hasValidFull").GetBoolean().Should().BeTrue();
        s1.GetProperty("deleteRequested").GetBoolean().Should().BeTrue();
        s1.GetProperty("deleteRequestedBy").GetString().Should().Be("operator");
        s1.GetProperty("ttlLeftSec").ValueKind.Should().Be(JsonValueKind.Null, "protected by valid full + delete request");
        var s2 = orphans.Single(o => o.GetProperty("prefix").GetString() == "g/s2");
        s2.GetProperty("held").GetBoolean().Should().BeTrue();
        s2.GetProperty("heldBy").GetString().Should().Be("panel");
        s2.GetProperty("ttlLeftSec").ValueKind.Should().Be(JsonValueKind.Null, "under hold");
    }

    public async ValueTask DisposeAsync() => await _factory.DisposeAsync();
}
