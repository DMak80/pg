using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Etcd;
using AdminPanel.Probes.S3;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Детали шарда (AC9, t08 spec §7.9): etcd-статусы (full state/verify, wal-статус,
// активный restore state/phase) джойнятся с S3-фактами; сверка per-full
// (Ok/S3Only/InProgress); 404 шарда, которого нет ни в etcd, ни в S3-дереве.
[Collection("backups")]
public class BackupsShardDetailsApiTests(
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
    public async Task ShardDetails_JoinsEtcdAndS3()
    {
        _factory.EnsureBuilt(); // до любого Services — иначе ленивый build без очистки кеша
        // Arrange: сид Task 10 + объекты без ключа (full/gone — «мусор upload'а»);
        // тики: инвентарь → снапшот.
        await EtcdSeed.SeedBackupsAsync(
            etcd.Endpoint, TestContext.Current.CancellationToken);
        await minio.SeedObjectsAsync(
        [
            ("demo/s1/full/b1/base.tar", new string('a', 100)),
            ("demo/s1/full/b1/pg_wal/000000010000000000000001", new string('a', 16)),
            ("demo/s1/full/gone/base.tar", new string('a', 60)),
            ("demo/s1/wal/000000010000000000000002", new string('a', 16)),
            ("demo/s1/wal/00000002.history", new string('a', 4)),
        ], TestContext.Current.CancellationToken);
        var loop = _factory.Services.GetRequiredService<MinioInventoryLoop>();
        var refresher = _factory.Services.GetRequiredService<SnapshotRefresher>();
        await loop.RunOnceAsync(TestContext.Current.CancellationToken);
        (await refresher.RefreshOnceAsync(TestContext.Current.CancellationToken))
            .IsSuccess.Should().BeTrue();
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act
        using var response = await client.GetAsync(
            "/api/backups/storage/demo/s1", TestContext.Current.CancellationToken);

        // Assert: b1 — Ok (объекты + ключ, оба факта), gone — S3Only, b2 —
        // InProgress (PLANNED без объектов — ожидаемо, не EtcdOnly).
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(
            TestContext.Current.CancellationToken);
        dto.GetProperty("cluster").GetString().Should().Be("demo");
        dto.GetProperty("shard").GetString().Should().Be("s1");
        var fulls = dto.GetProperty("fulls").EnumerateArray()
            .ToDictionary(f => f.GetProperty("id").GetString()!);
        fulls.Should().HaveCount(3);
        var b1 = fulls["b1"];
        b1.GetProperty("reconcile").GetString().Should().Be("Ok");
        b1.GetProperty("etcdState").GetString().Should().Be("COMPLETED");
        b1.GetProperty("verifyState").GetString().Should().Be("OK");
        b1.GetProperty("sizeBytes").GetInt64().Should().Be(116);
        b1.GetProperty("objectCount").GetInt64().Should().Be(2);
        b1.GetProperty("etcdSizeBytes").GetInt64().Should().Be(100);
        var gone = fulls["gone"];
        gone.GetProperty("reconcile").GetString().Should().Be("S3Only");
        gone.GetProperty("etcdState").ValueKind.Should().Be(JsonValueKind.Null);
        gone.GetProperty("sizeBytes").GetInt64().Should().Be(60);
        var b2 = fulls["b2"];
        b2.GetProperty("reconcile").GetString().Should().Be("InProgress");
        b2.GetProperty("etcdState").GetString().Should().Be("PLANNED");
        b2.GetProperty("sizeBytes").ValueKind.Should().Be(JsonValueKind.Null);

        // Assert: WAL — etcd-факт (ACTIVE + last_uploaded_segment) рядом с
        // S3-фактом (1 сегмент + 1 история, последний сегмент по Ordinal).
        var wal = dto.GetProperty("wal");
        wal.GetProperty("etcdState").GetString().Should().Be("ACTIVE");
        wal.GetProperty("etcdLastSegment").GetString()
            .Should().Be("000000010000000000000001");
        wal.GetProperty("s3SegmentCount").GetInt64().Should().Be(1);
        wal.GetProperty("s3HistoryCount").GetInt64().Should().Be(1);
        wal.GetProperty("s3LastObject").GetString()
            .Should().Be("000000010000000000000002");

        // Assert: бейдж активной restore-заявки (PLANNED + phase downloading).
        var restore = dto.GetProperty("activeRestore");
        restore.GetProperty("state").GetString().Should().Be("PLANNED");
        restore.GetProperty("phase").GetString().Should().Be("downloading");

        // Assert: 404 шарда без etcd-ключей и без S3-узла.
        using var missing = await client.GetAsync(
            "/api/backups/storage/demo/nope", TestContext.Current.CancellationToken);
        missing.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    public async ValueTask DisposeAsync() => await _factory.DisposeAsync();
}

// Сироты (AC5-инт, t08 spec §7.5): блок сирот сводки слит — панельная сверка
// с джойном на реестр воркера (OBSERVED + TTL) и «панель видит, в реестре нет».
[Collection("backups")]
public class BackupsOrphansApiTests(
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
    public async Task Orphans_Block_JoinsRegistryAndPanel()
    {
        _factory.EnsureBuilt(); // до любого Services — иначе ленивый build без очистки кеша
        // Arrange: ghost_shard/s9 — есть и в S3, и в реестре (сид etcd);
        // noowner/s1 — есть только в S3, в реестре записи нет.
        await EtcdSeed.SeedBackupsAsync(
            etcd.Endpoint, TestContext.Current.CancellationToken);
        await minio.SeedObjectsAsync(
        [
            ("ghost_shard/s9/full/b9/base.tar", new string('a', 50)),
            ("noowner/s1/wal/000000010000000000000003", new string('a', 16)),
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

        // Assert: сирота с записью реестра — OBSERVED, firstSeen/TTL видны
        // (TTL 7 сут от first_seen=now-100 — остаток заведомо положителен);
        // сирота без записи — «панель видит, в реестре нет».
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(
            TestContext.Current.CancellationToken);
        var orphans = dto.GetProperty("orphans").EnumerateArray()
            .ToDictionary(o => o.GetProperty("prefix").GetString()!);
        var ghost = orphans["ghost_shard/s9"];
        ghost.GetProperty("inWorkerRegistry").GetBoolean().Should().BeTrue();
        ghost.GetProperty("registryState").GetString().Should().Be("OBSERVED");
        ghost.GetProperty("firstSeenUnix").GetInt64().Should().BeGreaterThan(0);
        ghost.GetProperty("ttlLeftSec").GetInt64().Should().BeGreaterThan(0);
        var noowner = orphans["noowner/s1"];
        noowner.GetProperty("inWorkerRegistry").GetBoolean().Should().BeFalse();
        noowner.GetProperty("registryState").ValueKind.Should().Be(JsonValueKind.Null);
        noowner.GetProperty("kind").GetString().Should().Be("shard");
    }

    public async ValueTask DisposeAsync() => await _factory.DisposeAsync();
}
