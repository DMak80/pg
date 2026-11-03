using Shared.Etcd.Coordination;

namespace Shared.Etcd.UnitTests;

// Опросный кэш etcd-префикса (arch/17 S10): первичная загрузка → периодический
// Range → diff с кэшем → Changed только при различиях (add/change/delete;
// различий нет — события нет; сбой опроса — кэш сохраняется, ретрай).
public class EtcdPrefixPollCacheTests
{
    // Детерминированное ожидание фонового цикла без пассивных sleep'ов: короткий
    // полл с бюджетом 5 с (AGENTS.base §12).
    private static async Task WaitForAsync(Func<bool> condition, string what, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException(what);
            await Task.Delay(10, ct);
        }
    }

    // Шаг опроса в тестах — минимальный (10 мс, ≤5 с): цикл крутится быстро.
    private static EtcdPrefixPollCache NewCache(FakePollGateway gateway)
    {
        var cache = new EtcdPrefixPollCache(
            "/p/", gateway, ["http://e1:2379"], TimeSpan.FromMilliseconds(10));
        return cache;
    }

    [Fact]
    public async Task StartAsync_PrimaryLoad_FillsSnapshotWithoutEvent()
    {
        // Arrange
        var gateway = new FakePollGateway();
        gateway.Seed("/p/a", "a1", 5);
        gateway.Seed("/p/b", "b1", 6);
        var changes = new List<EtcdPrefixPollChange>();
        var cache = NewCache(gateway);
        cache.Changed += changes.Add;

        // Act
        await cache.StartAsync(TestContext.Current.CancellationToken);
        await WaitForAsync(() => gateway.RangeCalls >= 2, "цикл опроса не стартовал", TestContext.Current.CancellationToken);

        // Assert — кэш заполнен первичной загрузкой; первичная загрузка — БЕЗ события
        cache.Snapshot.Should().HaveCount(2);
        cache.Snapshot["/p/a"].Value.Should().Be("a1");
        cache.Snapshot["/p/a"].ModRevision.Should().Be(5);
        cache.Snapshot["/p/b"].Value.Should().Be("b1");
        cache.Snapshot["/p/b"].ModRevision.Should().Be(6);
        changes.Should().BeEmpty();
        await cache.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Poll_NoDifferences_NoEvent()
    {
        // Arrange — Store не меняется: опросы читают то же состояние
        var gateway = new FakePollGateway();
        gateway.Seed("/p/a", "a1", 5);
        var changes = new List<EtcdPrefixPollChange>();
        var cache = NewCache(gateway);
        cache.Changed += changes.Add;

        // Act
        await cache.StartAsync(TestContext.Current.CancellationToken);
        await WaitForAsync(() => gateway.RangeCalls >= 3, "опрос не повторился", TestContext.Current.CancellationToken);

        // Assert — различий нет: события нет, кэш не тронут
        changes.Should().BeEmpty();
        cache.Snapshot.Should().HaveCount(1);
        cache.Snapshot["/p/a"].Value.Should().Be("a1");
        await cache.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Poll_DetectsAddChangeAndDelete_OneDeltaPerPoll()
    {
        // Arrange — на втором опросе: k2 изменён, k4 добавлен, k3 удалён
        var gateway = new FakePollGateway();
        gateway.Seed("/p/k1", "v1", 5);
        gateway.Seed("/p/k2", "v2", 6);
        gateway.Seed("/p/k3", "v3", 7);
        var changes = new List<EtcdPrefixPollChange>();
        var cache = NewCache(gateway);
        cache.Changed += changes.Add;
        gateway.OnRange = () =>
        {
            if (gateway.RangeCalls != 2)
                return;
            gateway.Store["/p/k2"] = ("v2b", 8);
            gateway.Store["/p/k4"] = ("v4", 9);
            gateway.Store.Remove("/p/k3");
        };

        // Act
        await cache.StartAsync(TestContext.Current.CancellationToken);
        await WaitForAsync(() => changes.Count >= 1, "дельта не доставлена", TestContext.Current.CancellationToken);

        // Assert — одна пачка дельты на опрос: change+add в ChangedKeys, delete в RemovedKeys
        changes.Should().HaveCount(1);
        var delta = changes[0];
        delta.ChangedKeys.Should().BeEquivalentTo(["/p/k2", "/p/k4"]);
        delta.RemovedKeys.Should().Equal(["/p/k3"]);
        delta.Revision.Should().Be(9); // max mod_revision прочитанного
        cache.Snapshot.Should().HaveCount(3);
        cache.Snapshot.Should().NotContainKey("/p/k3");
        cache.Snapshot["/p/k2"].Value.Should().Be("v2b");
        cache.Snapshot["/p/k2"].ModRevision.Should().Be(8);
        cache.Snapshot["/p/k4"].Value.Should().Be("v4");
        await cache.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Poll_Failure_KeepsCacheWithoutEvent_AndRetriesNextStep()
    {
        // Arrange — второй опрос не удался (endpoints недоступны), третий видит изменение
        var gateway = new FakePollGateway();
        gateway.Seed("/p/k1", "v1", 5);
        gateway.RangeFault = call => call == 2
            ? Result<IReadOnlyList<Kv>>.Failed(new ApplicationException("etcd endpoints are down"))
            : Result<IReadOnlyList<Kv>>.Success([]);
        var changes = new List<EtcdPrefixPollChange>();
        var cache = NewCache(gateway);
        cache.Changed += changes.Add;
        gateway.OnRange = () =>
        {
            if (gateway.RangeCalls == 3)
                gateway.Store["/p/k1"] = ("v1b", 6);
        };

        // Act
        await cache.StartAsync(TestContext.Current.CancellationToken);
        await WaitForAsync(
            () => gateway.RangeCalls >= 3, "ретрай после сбоя не выполнен", TestContext.Current.CancellationToken);
        await WaitForAsync(
            () => changes.Count >= 1, "изменение после сбоя не обнаружено", TestContext.Current.CancellationToken);

        // Assert — после сбоя кэш сохранён и событие не поднималось; ретрай увидел изменение
        changes.Should().HaveCount(1);
        changes[0].ChangedKeys.Should().Equal(["/p/k1"]);
        changes[0].Revision.Should().Be(6);
        cache.Snapshot["/p/k1"].Value.Should().Be("v1b");
        cache.Snapshot["/p/k1"].ModRevision.Should().Be(6);
        await cache.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public void Ctor_Interval_DefaultAndClamp()
    {
        // Arrange
        var gateway = new FakePollGateway();

        // Act/Assert — дефолт 5 с; больше границы — ужат к ≤5 с; заданный — как есть;
        // не-положительный — дефолт
        new EtcdPrefixPollCache("/p/", gateway, ["http://e1:2379"])
            .Interval.Should().Be(TimeSpan.FromSeconds(5));
        new EtcdPrefixPollCache("/p/", gateway, ["http://e1:2379"], TimeSpan.FromSeconds(60))
            .Interval.Should().Be(TimeSpan.FromSeconds(5));
        new EtcdPrefixPollCache("/p/", gateway, ["http://e1:2379"], TimeSpan.FromMilliseconds(250))
            .Interval.Should().Be(TimeSpan.FromMilliseconds(250));
        new EtcdPrefixPollCache("/p/", gateway, ["http://e1:2379"], TimeSpan.Zero)
            .Interval.Should().Be(TimeSpan.FromSeconds(5));
    }
}
