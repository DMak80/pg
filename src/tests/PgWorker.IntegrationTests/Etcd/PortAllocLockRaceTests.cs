using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using Xunit;

namespace PgWorker.IntegrationTests.Etcd;

// t90: гонка ПАРАЛЛЕЛЬНОГО provisioning на реальном etcd — две критические
// секции «ReadBusy → Allocate → put portalloc» под глобальным клэймом дают
// НЕПЕРЕСЕКАЮЩИЕСЯ порты; без клэйма обе читали бы пустой префикс (воспроизведение
// dev-стенда 2026-08-25: «port is already allocated»).
[Collection(EtcdCollection.Name)]
public class PortAllocLockRaceTests(EtcdFixture fixture)
{
    private EtcdGateway Gateway => fixture.Gateway;

    private string Endpoint => fixture.Endpoint;

    // Критическая секция довыделения (мини-P1): под PortAllocLock читает busy
    // из префикса portalloc (кроме своего кластера), аллоцирует тройку, пишет ключ.
    // Ретрай-цикл «пока не acquired» имитирует тики (~200 мс) с бюджетом 10 с.
    private async Task<Result> CriticalSectionAsync(PortAllocLock portLock, string cluster, CancellationToken ct)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var acquired = await portLock.TryAcquireAsync(ct);
            if (!acquired.IsSuccess)
                return acquired;
            if (!acquired.Value)
            {
                await Task.Delay(200, ct); // «занят другим» — следующий тик
                continue;
            }

            try
            {
                // busy = portalloc-записи ВСЕХ чужих кластеров (как PortAllocIndex)
                var range = await Gateway.RangeAsync(Endpoint, "/pgworker/portalloc/", ct);
                if (!range.IsSuccess)
                    return range;
                var busy = new HashSet<(string, int)>();
                foreach (var kv in range.Value)
                {
                    if (kv.Key.EndsWith($"/{cluster}", StringComparison.Ordinal))
                        continue;
                    var parsed = Portalloc.Parse(kv.Key.Split('/')[^1], kv.Value);
                    if (!parsed.IsSuccess)
                        continue;
                    foreach (var addr in parsed.Value.Values)
                    {
                        busy.Add((addr.Host, addr.Ports.Pg));
                        busy.Add((addr.Host, addr.Ports.Patroni));
                        busy.Add((addr.Host, addr.Ports.Doorman));
                    }
                }

                // Аллокация одной ноды (тройка pg/patroni/doorman)
                var plan = new PlacementPlan([new NodePlacement("shard1", "n1", "h1")]);
                var allocated = PortAllocator.Allocate(
                    plan, new Dictionary<string, NodeAddress>(), busy, 15000, 15100);
                if (!allocated.IsSuccess)
                    return allocated;
                var put = await Gateway.PutAsync(
                    Endpoint, $"/pgworker/portalloc/{cluster}", Portalloc.Serialize(allocated.Value), null, ct);
                if (!put.IsSuccess)
                    return put;
                return Result.Success();
            }
            finally
            {
                await portLock.ReleaseAsync();
            }
        }

        return Result.Failed(new ApplicationException("порт-клэйм не освобождался 10 с — гонка/дедлок"));
    }

    private static HashSet<(string, int)> PortsOf(IReadOnlyDictionary<string, NodeAddress> addresses)
    {
        var ports = new HashSet<(string, int)>();
        foreach (var addr in addresses.Values)
        {
            ports.Add((addr.Host, addr.Ports.Pg));
            ports.Add((addr.Host, addr.Ports.Patroni));
            ports.Add((addr.Host, addr.Ports.Doorman));
        }
        return ports;
    }

    // AAA: две параллельные секции (барьер одновременного старта) — порты двух
    // кластеров не пересекаются; ключ клэйма исчезает после release обеих.
    [Fact]
    public async Task ParallelSections_AllocateDisjointPorts()
    {
        // Arrange — «два инстанса» с независимыми клэймами
        var ct = TestContext.Current.CancellationToken;
        var first = new PortAllocLock([Endpoint], Gateway, TimeProvider.System, "inst-1");
        var second = new PortAllocLock([Endpoint], Gateway, TimeProvider.System, "inst-2");
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var task1 = Task.Run(async () => { await start.Task; return await CriticalSectionAsync(first, "shop1", ct); });
        var task2 = Task.Run(async () => { await start.Task; return await CriticalSectionAsync(second, "shop2", ct); });

        // Act — одновременный старт
        start.SetResult();
        var results = await Task.WhenAll(task1, task2);

        // Assert: обе секции дошли до конца
        results.Should().OnlyContain(r => r.IsSuccess);

        // Порты двух кластеров НЕ пересекаются (без клэйма обе получили бы 15000)
        var firstAlloc = Portalloc.Parse("shop1",
            (await Gateway.GetAsync(Endpoint, "/pgworker/portalloc/shop1", ct)).Value!.Value);
        var secondAlloc = Portalloc.Parse("shop2",
            (await Gateway.GetAsync(Endpoint, "/pgworker/portalloc/shop2", ct)).Value!.Value);
        var intersection = PortsOf(firstAlloc.Value).Intersect(PortsOf(secondAlloc.Value)).ToList();
        intersection.Should().BeEmpty("клэйм сериализует выбор троек — повторная секция видит запись соседа");

        // Ключ клэйма исчез после release обеих секций
        var lockKey = await Gateway.GetAsync(Endpoint, PortAllocLock.Key, ct);
        lockKey.Value.Should().BeNull();
    }

    // AAA: захват/занятость/release на реальном txn-примитиве etcd.
    [Fact]
    public async Task TryAcquire_MutualExclusionAndRelease()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        var first = new PortAllocLock([Endpoint], Gateway, TimeProvider.System, "inst-1");
        var second = new PortAllocLock([Endpoint], Gateway, TimeProvider.System, "inst-2");

        // Act
        var firstAcquired = await first.TryAcquireAsync(ct);
        var secondAcquired = await second.TryAcquireAsync(ct);
        await first.ReleaseAsync();
        var reclaimed = await second.TryAcquireAsync(ct);
        await second.ReleaseAsync();

        // Assert
        firstAcquired.Value.Should().BeTrue();
        secondAcquired.Value.Should().BeFalse();
        reclaimed.Value.Should().BeTrue();
        (await Gateway.GetAsync(Endpoint, PortAllocLock.Key, ct)).Value.Should().BeNull();
    }
}
