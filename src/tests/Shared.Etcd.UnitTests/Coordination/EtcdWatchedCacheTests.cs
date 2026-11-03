using Shared.Etcd.Coordination;

namespace Shared.Etcd.UnitTests;

// Кэш дельт etcd-префикса (arch/17 S10): первичная загрузка → apply put/delete
// по revision → compacted-reset → восстановление после разрыва (догрузка с
// последнего применённого revision, следующий endpoint).
public class EtcdWatchedCacheTests
{
    // Детерминированное ожидание фонового цикла без пассивных sleep'ов: короткий
    // полл с бюджетом 5 с (AGENTS.base §12).
    private static async Task WaitForAsync(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException(what);
            await Task.Delay(10);
        }
    }

    private static EtcdWatchedCache NewCache(FakeWatchGateway gateway, string[] endpoints)
    {
        var cache = new EtcdWatchedCache("/p/", gateway, endpoints);
        cache.ReconnectDelay = TimeSpan.FromMilliseconds(1);
        return cache;
    }

    [Fact]
    public async Task StartAsync_PrimaryLoad_FillsSnapshotAndStartsWatchFromNextRevision()
    {
        // Arrange
        var gateway = new FakeWatchGateway();
        gateway.Seed("/p/a", "a1", 5);
        gateway.Seed("/p/b", "b1", 6);
        var cache = NewCache(gateway, ["http://e1:2379"]);

        // Act
        await cache.StartAsync(CancellationToken.None);

        // Assert — кэш заполнен первичной загрузкой, watch стартует с последнего+1
        cache.Snapshot.Should().HaveCount(2);
        cache.Snapshot["/p/a"].Value.Should().Be("a1");
        cache.Snapshot["/p/a"].ModRevision.Should().Be(5);
        cache.Snapshot["/p/b"].Value.Should().Be("b1");
        cache.Snapshot["/p/b"].ModRevision.Should().Be(6);
        gateway.RangeCalls.Should().Be(1);
        await WaitForAsync(() => gateway.WatchAttempts.Count == 1, "watch не стартовал");
        gateway.WatchAttempts[0].Endpoint.Should().Be("http://e1:2379");
        gateway.WatchAttempts[0].StartRevision.Should().Be(7);
        await cache.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Events_AppliedInRevisionOrder_ChangedCarriesDelta()
    {
        // Arrange — PUT rev7, DELETE rev8, затем устаревшее событие rev6 (дубликат:
        // rev6 уже применён первичной загрузкой)
        var gateway = new FakeWatchGateway();
        gateway.Seed("/p/a", "old", 5);
        gateway.Seed("/p/b", "b1", 6);
        var changes = new List<EtcdWatchedCacheChange>();
        var cache = NewCache(gateway, ["http://e1:2379"]);
        cache.Changed += changes.Add;
        gateway.Scripts.Enqueue(async ctx =>
        {
            await ctx.OnEvent(new EtcdWatchEvent("/p/a", "new", 7, EtcdWatchEventType.Put));
            await ctx.OnEvent(new EtcdWatchEvent("/p/b", null, 8, EtcdWatchEventType.Delete));
            await ctx.OnEvent(new EtcdWatchEvent("/p/a", "stale", 6, EtcdWatchEventType.Put));
            return new EtcdWatchStop(EtcdWatchStopReason.Closed);
        });

        // Act
        await cache.StartAsync(CancellationToken.None);
        await WaitForAsync(() => changes.Count == 2, "дельты не доставлены");

        // Assert — кэш по порядку revision; устаревшее не применилось; дельты корректны
        cache.Snapshot["/p/a"].Value.Should().Be("new");
        cache.Snapshot["/p/a"].ModRevision.Should().Be(7);
        cache.Snapshot.Should().NotContainKey("/p/b");
        changes[0].ChangedKeys.Should().Equal(["/p/a"]);
        changes[0].RemovedKeys.Should().BeEmpty();
        changes[0].Revision.Should().Be(7);
        changes[1].ChangedKeys.Should().BeEmpty();
        changes[1].RemovedKeys.Should().Equal(["/p/b"]);
        changes[1].Revision.Should().Be(8);
        await WaitForAsync(() => gateway.WatchAttempts.Count == 2, "reconnect после разрыва не выполнен");
        gateway.WatchAttempts[1].StartRevision.Should().Be(9); // последний применённый + 1
        await cache.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Compacted_CacheRebuiltByFullReload()
    {
        // Arrange — первая попытка сразу compacted; внешние изменения увидит
        // только повторный Range
        var gateway = new FakeWatchGateway();
        gateway.Seed("/p/a", "a1", 5);
        gateway.Seed("/p/b", "b1", 6);
        var cache = NewCache(gateway, ["http://e1:2379"]);
        gateway.Scripts.Enqueue(_ =>
        {
            gateway.Store.Remove("/p/a");
            gateway.Store["/p/b"] = ("b2", 8);
            gateway.Store["/p/c"] = ("c1", 9);
            return Task.FromResult(new EtcdWatchStop(EtcdWatchStopReason.Compacted, 7));
        });

        // Act
        await cache.StartAsync(CancellationToken.None);
        await WaitForAsync(() => gateway.RangeCalls == 2, "повторная первичная загрузка не выполнена");
        await WaitForAsync(() => gateway.WatchAttempts.Count == 2, "watch после compacted не перезапущен");

        // Assert — кэш пересобран ЦЕЛИКОМ: /p/a удалён из состояния, свежие значения на месте
        cache.Snapshot.Should().HaveCount(2);
        cache.Snapshot.Should().NotContainKey("/p/a");
        cache.Snapshot["/p/b"].Value.Should().Be("b2");
        cache.Snapshot["/p/b"].ModRevision.Should().Be(8);
        cache.Snapshot["/p/c"].Value.Should().Be("c1");
        gateway.WatchAttempts[1].StartRevision.Should().Be(10); // max revision перезагрузки + 1
        await cache.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task StreamBreak_ReconnectsNextEndpoint_AndResumesFromLastApplied()
    {
        // Arrange — два endpoint: разрыв на первом → переподключение ко второму
        var gateway = new FakeWatchGateway();
        gateway.Seed("/p/k", "v1", 5);
        var cache = NewCache(gateway, ["http://e1:2379", "http://e2:2379"]);
        gateway.Scripts.Enqueue(async ctx =>
        {
            await ctx.OnEvent(new EtcdWatchEvent("/p/k", "v2", 6, EtcdWatchEventType.Put));
            return new EtcdWatchStop(EtcdWatchStopReason.Closed); // разрыв потока
        });
        // вторая попытка без сценария — блокирующая подписка

        // Act
        await cache.StartAsync(CancellationToken.None);
        await WaitForAsync(() => gateway.WatchAttempts.Count == 2, "переподключение не выполнено");

        // Assert — следующий endpoint; догрузка с последнего применённого + 1
        gateway.WatchAttempts[0].Endpoint.Should().Be("http://e1:2379");
        gateway.WatchAttempts[0].StartRevision.Should().Be(6);
        gateway.WatchAttempts[1].Endpoint.Should().Be("http://e2:2379");
        gateway.WatchAttempts[1].StartRevision.Should().Be(7);
        cache.Snapshot["/p/k"].Value.Should().Be("v2");
        cache.Snapshot["/p/k"].ModRevision.Should().Be(6);
        await cache.StopAsync(TestContext.Current.CancellationToken);
    }
}
