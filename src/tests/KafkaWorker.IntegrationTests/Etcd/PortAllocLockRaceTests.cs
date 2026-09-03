using System.Text;
using FluentAssertions;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using Xunit;

namespace KafkaWorker.IntegrationTests.Etcd;

// t91: гонка ПАРАЛЛЕЛЬНОГО provisioning на реальном etcd — две критические
// секции «busy из префикса portalloc → Allocate → put portalloc» под глобальным
// клэймом дают НЕПЕРЕСЕКАЮЩИЕСЯ порты; без клэйма+индекса обе читали бы пустой
// префикс (класс гонки t90: «port is already allocated»; порт PgWorker-теста).
[Collection(EtcdCollection.Name)]
public class PortAllocLockRaceTests(EtcdFixture fixture)
{
    private EtcdGateway Gateway => fixture.Gateway;

    private string Endpoint => fixture.Endpoint;

    // Критическая секция довыделения (мини-K1): под PortAllocLock читает busy из
    // префикса portalloc (кроме своего кластера — PortAllocIndex-паттерн),
    // аллоцирует порт, пишет ключ. Ретрай-цикл «пока не acquired» имитирует тики
    // (~200 мс) с бюджетом 10 с.
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
                // busy = portalloc-записи ВСЕХ чужих кластеров (как PortAllocIndex):
                // кортежи (host из записи, client) — именно так их сравнивает
                // PortAllocator по placement.Host.
                var range = await Gateway.RangeAsync(Endpoint, "/kafkaworker/portalloc/", ct);
                if (!range.IsSuccess)
                    return range;
                var busy = new HashSet<(string, int)>();
                foreach (var kv in range.Value)
                {
                    if (kv.Key.EndsWith($"/{cluster}", StringComparison.Ordinal))
                        continue;
                    foreach (var (host, port) in ParseEntries(kv.Value))
                        busy.Add((host, port));
                }

                // Аллокация одного брокера (1 клиентский порт); диапазон — значения
                // в etcd, не host-биндинги: литералы допустимы (AGENTS.md — про
                // хост-порты docker).
                var plan = new PlacementPlan([new NodePlacement("broker1", "h1")]);
                var allocated = PortAllocator.Allocate(
                    plan, new Dictionary<string, NodeAddress>(), busy, 16000, 16100);
                if (!allocated.IsSuccess)
                    return allocated;
                var put = await Gateway.PutAsync(
                    Endpoint, $"/kafkaworker/portalloc/{cluster}", Serialize(allocated.Value), null, ct);
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

    // Формат arch/15 §4: {"broker<k>":{"host":"h","client":P}} — пары (host, client).
    private static IEnumerable<(string Host, int Port)> ParseEntries(string json)
    {
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        foreach (var node in doc.RootElement.EnumerateObject())
            yield return (node.Value.GetProperty("host").GetString()!,
                node.Value.GetProperty("client").GetInt32());
    }

    private static string Serialize(IReadOnlyDictionary<string, NodeAddress> addresses)
    {
        var sb = new StringBuilder("{");
        foreach (var (node, addr) in addresses.OrderBy(p => p.Key, StringComparer.Ordinal))
            sb.Append($"\"{node}\":{{\"host\":\"{addr.Host}\",\"client\":{addr.ClientPort}}},");
        return (sb.Length > 1 ? sb.ToString()[..^1] : "{") + "}";
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
        var task1 = Task.Run(async () => { await start.Task; return await CriticalSectionAsync(first, "events1", ct); });
        var task2 = Task.Run(async () => { await start.Task; return await CriticalSectionAsync(second, "events2", ct); });

        // Act — одновременный старт
        start.SetResult();
        var results = await Task.WhenAll(task1, task2);

        // Assert: обе секции дошли до конца
        results.Should().OnlyContain(r => r.IsSuccess);

        // Порты двух кластеров НЕ пересекаются (без клэйма обе получили бы 16000)
        var firstPorts = ParseEntries(
            (await Gateway.GetAsync(Endpoint, "/kafkaworker/portalloc/events1", ct)).Value!.Value)
            .Select(e => e.Port).ToList();
        var secondPorts = ParseEntries(
            (await Gateway.GetAsync(Endpoint, "/kafkaworker/portalloc/events2", ct)).Value!.Value)
            .Select(e => e.Port).ToList();
        firstPorts.Should().NotBeEmpty();
        secondPorts.Should().NotBeEmpty();
        firstPorts.Intersect(secondPorts).Should().BeEmpty(
            "клэйм сериализует секции — повторная видит запись соседа в busy");

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
