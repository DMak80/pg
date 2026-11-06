using System.Text.Json;
using EtcdNode.Core;
using Xunit;

namespace EtcdNode.IntegrationTests;

// Поведенческие кейсы ЖИВОГО агента в узлах контура (arch/etcdnode/02-snapshots.md,
// процессы/c-snapshot-export.md): лидер снимает/выгружает, фильтр важности
// отсекает пульсы, ключи домена /etcdnode/ — один лидер + живость всех узлов.
// Все ожидания — поллинг с бюджетом ≤ 30 с; помехи соседних кейсов исключены
// последовательным исполнением коллекции (DisableParallelization).
[Collection(EtcdNodeCollection.Name)]
public sealed class SnapshotAgentTests(EtcdNodeEnvironment env)
{
    // Поллинг статуса DR-выгрузки до state=OK (бюджет 30 с): первый слепок
    // лидера (нет эталона — внеочередной) либо доводка после посева цели.
    private static async Task WaitStatusOkAsync(EtcdNodeEnvironment env)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var kv = await env.GetAsync(EtcdSnapshotStatusJson.Key);
            if (kv.Value is { } found
                && EtcdSnapshotStatusJson.Parse(found.Value) is { State: "OK" })
                return;
            await Task.Delay(500, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException("agent did not write OK status to /etcdnode/snapshots in 30 s");
    }

    // .db-объекты слепкового bucket (число пар для детекции новых слепков).
    private static async Task<IReadOnlyList<EtcdS3Object>> ListDbObjectsAsync(EtcdNodeEnvironment env)
    {
        var ct = TestContext.Current.CancellationToken;
        var target = await new EtcdBackupTargetResolver(env.Gateway, [env.Endpoint]).ResolveAsync(ct);
        target.IsSuccess.Should().BeTrue(target.Error?.Message);
        await using var s3 = new EtcdSnapshotS3(
            EtcdNodeEnvironment.HostS3Endpoint(target.Value), target.Value.Bucket,
            target.Value.AccessKey, target.Value.SecretKey);
        var objects = await s3.ListPrefixAsync(EtcdExportRetention.Prefix, ct);
        objects.IsSuccess.Should().BeTrue(objects.Error?.Message);
        return objects.Value;
    }

    private static int DbCount(IReadOnlyList<EtcdS3Object> objects)
        => objects.Count(o => o.Key.EndsWith(".db", StringComparison.Ordinal));

    // AAA: важное изменение (ключ вне пульсов) — внеочередной слепок растит
    // число .db-объектов в bucket (MinIntervalSec=1 у агентов окружения)
    [Fact]
    public async Task ImportantChange_Triggers_Snapshot()
    {
        var ct = TestContext.Current.CancellationToken;
        await env.SeedEtcdBackupClusterAsync();
        await WaitStatusOkAsync(env);
        var before = DbCount(await ListDbObjectsAsync(env));

        // Act: запись ВАЖНОГО ключа (routing-ключ HA-контура — вне PulseKeys)
        await env.PutAsync("/clusters/shop/shards/s1/routing", """{"to":"s2"}""");

        // Assert: в бюджете 30 с появился новый слепок (тик скана 5 с + выгрузка)
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        var grown = false;
        while (DateTimeOffset.UtcNow < deadline)
        {
            var after = DbCount(await ListDbObjectsAsync(env));
            if (after > before)
            {
                grown = true;
                break;
            }

            await Task.Delay(1000, ct);
        }

        grown.Should().BeTrue("important change outside pulse keys must trigger an extra snapshot");
    }

    // AAA: лидер избран, статус OK, пара db+meta в bucket; мета несёт имя узла
    [Fact]
    public async Task Leader_Elected_StatusWritten_ObjectsInS3()
    {
        var ct = TestContext.Current.CancellationToken;
        await env.SeedEtcdBackupClusterAsync();
        await WaitStatusOkAsync(env);

        // Act: объекты слепкового bucket
        var objects = await ListDbObjectsAsync(env);

        // Assert: пара db+meta; instance меты — имя узла контура
        objects.Should().Contain(o => o.Key.EndsWith(".db", StringComparison.Ordinal));
        var meta = objects.FirstOrDefault(o => o.Key.EndsWith(".meta.json", StringComparison.Ordinal));
        meta.Should().NotBeNull();
        var target = await new EtcdBackupTargetResolver(env.Gateway, [env.Endpoint]).ResolveAsync(ct);
        await using var s3 = new EtcdSnapshotS3(
            EtcdNodeEnvironment.HostS3Endpoint(target.Value), target.Value.Bucket,
            target.Value.AccessKey, target.Value.SecretKey);
        var metaJson = await s3.GetObjectAsync(meta!.Key, ct);
        metaJson.IsSuccess.Should().BeTrue(metaJson.Error?.Message);
        using var doc = JsonDocument.Parse(metaJson.Value!);
        var instance = doc.RootElement.GetProperty("instance").GetString();
        instance.Should().BeOneOf("node1", "node2",
            "meta.instance is the node name of the exporting leader agent");
    }

    // AAA: ключи домена — ровно один лидер; живость обоих агентов
    // (instanceId ClaimStore = имя узла, arch/etcdnode/00-etcdnode-contract.md §1)
    [Fact]
    public async Task LeaderKey_Single_Instance_Keys_Both()
    {
        var ct = TestContext.Current.CancellationToken;

        // Act: поллинг появления ключей (оба агента стартуют со своими
        // instance-ключами; лидер ставит leader-клэйм)
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        Kv? leaderKey = null;
        Kv? node1Key = null;
        Kv? node2Key = null;
        while (DateTimeOffset.UtcNow < deadline)
        {
            leaderKey = (await env.GetAsync("/etcdnode/leader")).Value;
            node1Key = (await env.GetAsync("/etcdnode/instances/node1")).Value;
            node2Key = (await env.GetAsync("/etcdnode/instances/node2")).Value;
            if (leaderKey is not null && node1Key is not null && node2Key is not null)
                break;
            await Task.Delay(500, ct);
        }

        // Assert: один leader-ключ (значение — один лидер), живость обоих узлов
        leaderKey.Should().NotBeNull("leader claim must exist");
        node1Key.Should().NotBeNull("node1 liveness key must exist");
        node2Key.Should().NotBeNull("node2 liveness key must exist");
    }

    // AAA: непрерывная перезапись пульсов (/service/*, мастер-ключ HA) НЕ
    // порождает новых слепков сверх уже снятых (плановый интервал 360 мин)
    [Fact]
    public async Task Pulse_DoesNotTrigger_Snapshot()
    {
        var ct = TestContext.Current.CancellationToken;
        await env.SeedEtcdBackupClusterAsync();
        await WaitStatusOkAsync(env);
        var before = DbCount(await ListDbObjectsAsync(env));

        // Act: 15 с пульс-перезаписи — каждую секунду оба пульс-ключа
        var deadline = DateTimeOffset.UtcNow.AddSeconds(15);
        while (DateTimeOffset.UtcNow < deadline)
        {
            await env.PutAsync("/service/x/leader", DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString());
            await env.PutAsync("/clusters/shop/shards/s1/master",
                DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString());
            await Task.Delay(1000, ct);
        }

        // Assert: число .db не выросло (пульсы не важны, планового не было)
        var after = DbCount(await ListDbObjectsAsync(env));
        after.Should().Be(before,
            "pulse rewrites (leader/master timestamps) must not trigger snapshots");
    }
}
