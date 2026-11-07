using System.Security.Cryptography;
using System.Text.Json;
using EtcdNode.Core;
using Xunit;

namespace EtcdNode.IntegrationTests;

// Поведенческие кейсы ЖИВОГО агента в узлах контура (arch/etcdnode/02-snapshots.md,
// процессы/c-snapshot-export.md): лидер снимает/выгружает, фильтр важности
// отсекает пульсы, ключи домена /etcdnode/ — один лидер + живость всех узлов,
// ветка F — смерть узла-лидера: takeover ≤ TTL + тик. Все ожидания — поллинг
// с бюджетом ≤ 30 с; помехи соседних кейсов исключены последовательным
// исполнением коллекции (DisableParallelization).
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

    // .db-объекты слепкового bucket (число пар для детекции новых слепков);
    // endpoint — грань чтения /s3/clusters/ (дефолт node1; кейс смерти
    // лидера передаёт грань ЖИВОГО узла — грань node1 умирает с лидером).
    private static async Task<IReadOnlyList<EtcdS3Object>> ListDbObjectsAsync(
        EtcdNodeEnvironment env, string? endpoint = null)
    {
        var ct = TestContext.Current.CancellationToken;
        var target = await new EtcdBackupTargetResolver(env.Gateway, [endpoint ?? env.Endpoint]).ResolveAsync(ct);
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

    // «Тихая» фаза стационарности (барьер Act): два замера с интервалом
    // 2×тик скана (~10 с) без роста .db — лидер установил эталон и в тишине
    // не снимает; иначе Pulse ложно падает (смена лидера/baseline-null), а
    // ImportantChange зелёный не по своей причине. Бюджет 30 с.
    private static async Task AwaitQuietPhaseAsync(EtcdNodeEnvironment env)
    {
        var ct = TestContext.Current.CancellationToken;
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var first = DbCount(await ListDbObjectsAsync(env));
            await Task.Delay(10000, ct);
            var second = DbCount(await ListDbObjectsAsync(env));
            if (second == first)
                return;
        }

        throw new TimeoutException("agent did not reach a quiet phase (db count keeps growing) in 30 s");
    }

    // Put-хелпер с ассертом: молчаливая потеря put'а (транзиент) — падение
    // кейса, а не ghost-проход.
    private static async Task PutAssertedAsync(EtcdNodeEnvironment env, string key, string value)
    {
        var put = await env.PutAsync(key, value);
        put.IsSuccess.Should().BeTrue(put.Error?.Message);
    }

    // AAA: важное изменение (ключ вне пульсов) — внеочередной слепок
    // появляется НОВЫМ .db-ключом в bucket (MinIntervalSec=1 у агентов
    // окружения); не счётчиком: bucket один на коллекцию, ретенция агента
    // (N=3) сносит старейшую пару при новой выгрузке — рост счётчика
    // не детерминирован
    [Fact]
    public async Task ImportantChange_Triggers_Snapshot()
    {
        var ct = TestContext.Current.CancellationToken;
        await env.SeedEtcdBackupClusterAsync();
        await WaitStatusOkAsync(env);
        await AwaitQuietPhaseAsync(env);

        // Arrange: отсечка известных .db-ключей (паттерн кейса
        // Leader_Elected_StatusWritten_ObjectsInS3)
        var knownKeys = (await ListDbObjectsAsync(env)).Select(o => o.Key).ToHashSet();

        // Act: запись ВАЖНОГО ключа (routing-ключ HA-контура — вне PulseKeys)
        await PutAssertedAsync(env, "/clusters/shop/shards/s1/routing", """{"to":"s2"}""");

        // Assert: в бюджете 30 с появился новый слепок (тик скана 5 с + выгрузка)
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        var fresh = default(EtcdS3Object);
        while (DateTimeOffset.UtcNow < deadline)
        {
            fresh = (await ListDbObjectsAsync(env)).FirstOrDefault(o =>
                o.Key.EndsWith(".db", StringComparison.Ordinal) && !knownKeys.Contains(o.Key));
            if (fresh is not null)
                break;

            await Task.Delay(1000, ct);
        }

        fresh.Should().NotBeNull("important change outside pulse keys must trigger an extra snapshot");
    }

    // AAA: лидер избран, статус OK, пара db+meta в bucket; мета НОВОГО слепка
    // (появившегося ПОСЛЕ отсечки старта кейса — общий bucket коллекции не
    // валиден: экспортёр-кейсы кладут пары с чужим instance) несёт имя узла;
    // свежий слепок гарантирован важным изменением (routing вне пульсов)
    [Fact]
    public async Task Leader_Elected_StatusWritten_ObjectsInS3()
    {
        var ct = TestContext.Current.CancellationToken;
        await env.SeedEtcdBackupClusterAsync();
        await WaitStatusOkAsync(env);
        await AwaitQuietPhaseAsync(env);

        // Act-1: отсечка .db-объектов; важный ключ гарантирует свежий слепок
        var knownKeys = (await ListDbObjectsAsync(env)).Select(o => o.Key).ToHashSet();
        await PutAssertedAsync(env, "/clusters/shop/shards/s1/routing", """{"to":"node-leader-check"}""");
        IReadOnlyList<EtcdS3Object> objects = [];
        var freshDb = default(EtcdS3Object);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        while (DateTimeOffset.UtcNow < deadline)
        {
            objects = await ListDbObjectsAsync(env);
            freshDb = objects.FirstOrDefault(o =>
                o.Key.EndsWith(".db", StringComparison.Ordinal) && !knownKeys.Contains(o.Key));
            if (freshDb is not null)
                break;
            await Task.Delay(1000, ct);
        }

        freshDb.Should().NotBeNull("a fresh snapshot pair must appear after the important change");

        // Assert: пара db+meta у нового слепка; instance меты — имя узла контура
        var metaKey = $"{EtcdExportRetention.Prefix}{Path.GetFileName(freshDb!.Key)[..^".db".Length]}.meta.json";
        objects.Should().Contain(o => o.Key == metaKey);
        var target = await new EtcdBackupTargetResolver(env.Gateway, [env.Endpoint]).ResolveAsync(ct);
        await using var s3 = new EtcdSnapshotS3(
            EtcdNodeEnvironment.HostS3Endpoint(target.Value), target.Value.Bucket,
            target.Value.AccessKey, target.Value.SecretKey);
        var metaJson = await s3.GetObjectAsync(metaKey, ct);
        metaJson.IsSuccess.Should().BeTrue(metaJson.Error?.Message);
        using var doc = JsonDocument.Parse(metaJson.Value!);
        var instance = doc.RootElement.GetProperty("instance").GetString();
        instance.Should().BeOneOf("node1", "node2", "node3",
            "meta.instance is the node name of the exporting leader agent");

        // Assert (продолжение): пара db+meta самодостаточна — мета согласована
        // с фактическим содержимым скачанного .db (полный автономный бэкап,
        // arch/etcdnode/02-snapshots.md §2): size/sha256 точные, ревизия
        // кластера присутствует и положительна.
        var db = await s3.GetObjectBytesAsync(freshDb!.Key, ct);
        db.IsSuccess.Should().BeTrue(db.Error?.Message);
        doc.RootElement.GetProperty("size_bytes").GetInt64().Should().Be(db.Value.Length,
            "meta.size_bytes must match the downloaded .db object size");
        doc.RootElement.GetProperty("sha256").GetString().Should().Be(
            Convert.ToHexString(SHA256.HashData(db.Value)).ToLowerInvariant(),
            "meta.sha256 must match the downloaded .db bytes (hex-lower)");
        doc.RootElement.GetProperty("revision").ValueKind.Should().Be(JsonValueKind.Number,
            "a full snapshot carries the cluster revision");
        doc.RootElement.GetProperty("revision").GetInt64().Should().BeGreaterThan(0,
            "a full snapshot carries a positive cluster revision");
    }

    // AAA: ключи домена — leader-ключ существует (единственность garant
    // txn-захватом version==0 ClaimStore: второй писатель не проходит
    // compare — здесь ассертим НАЛИЧИЕ ключа и живости ВСЕХ узлов);
    // instanceId ClaimStore = имя узла (arch/etcdnode/00-etcdnode-contract.md §1)
    [Fact]
    public async Task LeaderKey_Single_Instance_Keys_Both()
    {
        var ct = TestContext.Current.CancellationToken;

        // Act: поллинг появления ключей (все агенты стартуют со своими
        // instance-ключами; лидер ставит leader-клэйм)
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        Kv? leaderKey = null;
        Kv? node1Key = null;
        Kv? node2Key = null;
        Kv? node3Key = null;
        while (DateTimeOffset.UtcNow < deadline)
        {
            leaderKey = (await env.GetAsync("/etcdnode/leader")).Value;
            node1Key = (await env.GetAsync("/etcdnode/instances/node1")).Value;
            node2Key = (await env.GetAsync("/etcdnode/instances/node2")).Value;
            node3Key = (await env.GetAsync("/etcdnode/instances/node3")).Value;
            if (leaderKey is not null && node1Key is not null && node2Key is not null && node3Key is not null)
                break;
            await Task.Delay(500, ct);
        }

        // Assert: leader-ключ есть, живость всех трёх узлов
        leaderKey.Should().NotBeNull("leader claim must exist");
        node1Key.Should().NotBeNull("node1 liveness key must exist");
        node2Key.Should().NotBeNull("node2 liveness key must exist");
        node3Key.Should().NotBeNull("node3 liveness key must exist");
    }

    // AAA: непрерывная перезапись пульсов (/service/*, мастер-ключ HA, домен
    // /etcdnode/ — статус-puts самого агента) НЕ порождает новых слепков
    // сверх уже снятых (плановый интервал 360 мин)
    [Fact]
    public async Task Pulse_DoesNotTrigger_Snapshot()
    {
        var ct = TestContext.Current.CancellationToken;
        await env.SeedEtcdBackupClusterAsync();
        await WaitStatusOkAsync(env);
        await AwaitQuietPhaseAsync(env);
        var before = DbCount(await ListDbObjectsAsync(env));

        // Act: 15 с пульс-перезаписи — каждую секунду все пульс-паттерны
        var deadline = DateTimeOffset.UtcNow.AddSeconds(15);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
            await PutAssertedAsync(env, "/service/x/leader", now);
            await PutAssertedAsync(env, "/clusters/shop/shards/s1/master", now);
            await Task.Delay(1000, ct);
        }

        // Assert: число .db не выросло (пульсы не важны, планового не было)
        var after = DbCount(await ListDbObjectsAsync(env));
        after.Should().Be(before,
            "pulse rewrites (leader/master timestamps, agent domain keys) must not trigger snapshots");
    }

    // AAA: ветка F (arch/etcdnode/01-agent.md §4) — смерть узла-ЛИДЕРА:
    // (0) до смерти: слепки снимает ТОЛЬКО лидер — тома /snapshots не-лидеров
    // пусты (инвариант arch/etcdnode/02-snapshots.md §2);
    // (1) пока lease лидера жив (TTL ≤ 15 с), статус-ключ НЕ переписывается
    // другими (координация localhost-only: умерший лидер не пишет FAILED,
    // новый не может захватить лидерство до истечения TTL);
    // (2) после TTL — takeover: новый лидер (baseline-null) снимает
    // внеочередной и продвигает статус-ключ (mod_revision растёт) в бюджете
    // ≤ 30 с;
    // (3) свежесть: новая пара .db+meta с taken_unix строго новее kill-времени,
    // instance меты — новый лидер по /etcdnode/leader (инвариант
    // arch/etcdnode/02-snapshots.md §2).
    // Контур 3 узлов — кворум переживает остановку члена.
    [Fact]
    public async Task LeaderDown_StatusFrozenUntilTtl_ThenTakeover()
    {
        var ct = TestContext.Current.CancellationToken;
        await env.SeedEtcdBackupClusterAsync();
        await WaitStatusOkAsync(env);
        await AwaitQuietPhaseAsync(env);

        // Arrange: лидер по значению leader-ключа (instance = имя узла);
        // чтение после смерти лидера — через client-грань ЖИВОГО узла.
        var leader = (await env.GetAsync("/etcdnode/leader")).Value
            ?? throw new InvalidOperationException("leader key is missing");
        var leaderNode = new[] { "node1", "node2", "node3" }
            .First(n => leader.Value.Contains(n, StringComparison.Ordinal));
        var aliveNode = new[] { "node1", "node2", "node3" }.First(n => n != leaderNode);
        var aliveEndpoint = env.EndpointOf(aliveNode);

        // Чтение ревизии статус-ключа через грань ЖИВОГО узла — ТОЛЬКО
        // успешное: отказ чтения (окно raft-переизбрания после kill:
        // linearizable read 503/timeout пока новый raft-лидер не избран) —
        // НЕ «ревизия»: null от Failed-чтения, трактованный как значение,
        // даёт ложное «изменилась» (Failed null → затем успешное число).
        // Ключ статуса существует с первого слепка лидера — успешное
        // чтение без ключа в этом кейсе невозможно.
        async Task<ulong> ReadStatusRevisionCheckedAsync()
        {
            var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
            while (DateTimeOffset.UtcNow < deadline)
            {
                var kv = await env.Gateway.GetAsync(aliveEndpoint, EtcdSnapshotStatusJson.Key, ct);
                if (kv.IsSuccess && kv.Value is { } read)
                    return read.ModRevision;
                await Task.Delay(500, ct);
            }

            throw new TimeoutException("status key is unreadable via the alive node endpoint in 10 s");
        }

        // Assert-0: слепки снимает ТОЛЬКО лидер — тома /snapshots не-лидеров
        // не содержат файлов слепков. Единственный источник смены лидера в
        // коллекции — kill этого же кейса: на момент его Arrange лидер
        // неизменен с сотворения контура, тома не-лидеров пусты при ЛЮБОМ
        // порядке исполнения кейсов сборки.
        var nonLeaders = new[] { "node1", "node2", "node3" }.Where(n => n != leaderNode).ToArray();
        nonLeaders.Should().HaveCount(2, "the circuit has exactly two non-leader nodes");
        foreach (var name in nonLeaders)
        {
            var files = await env.SnapshotFilesOfAsync(name);
            files.Should().NotContain("snapshot-",
                $"non-leader {name} must not take snapshots (leader-only snapshotting)");
        }

        try
        {
            // Act-1: смерть узла-лидера. Замороженная мод-ревизия снимается
            // ПЕРВЫМ чтением ПОСЛЕ смерти: между пред-килл-чтением и kill
            // могла завершиться выгрузка (статус-put) — ложное «изменение».
            var killUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var knownDb = (await ListDbObjectsAsync(env)).Select(o => o.Key).ToHashSet();
            await env.KillNodeAsync(leaderNode);
            await Task.Delay(1000, ct); // осесть in-flight статус-put'ам лидера
            var frozenRevision = await ReadStatusRevisionCheckedAsync();
            var stillFrozen = true;
            var freezeDeadline = DateTimeOffset.UtcNow.AddSeconds(5);
            while (DateTimeOffset.UtcNow < freezeDeadline)
            {
                if (await ReadStatusRevisionCheckedAsync() != frozenRevision)
                {
                    stillFrozen = false;
                    break;
                }

                await Task.Delay(500, ct);
            }

            // Assert-1: до истечения TTL ≤ 15 с статус-ключ не переписан
            // (срез 5 с после смерти — заведомо внутри TTL-окна).
            stillFrozen.Should().BeTrue(
                "a dead leader keeps its lease ≤ TTL 15 s — nobody may rewrite the status key in that window");

            // Act-2/Assert-2: takeover ≤ TTL + тик скана + выгрузка (бюджет 30 с):
            // новый лидер снимает внеочередной (baseline-null) и пишет статус.
            var takeover = false;
            var takeoverDeadline = DateTimeOffset.UtcNow.AddSeconds(30);
            while (DateTimeOffset.UtcNow < takeoverDeadline)
            {
                if (await ReadStatusRevisionCheckedAsync() != frozenRevision)
                {
                    takeover = true;
                    break;
                }

                await Task.Delay(500, ct);
            }

            takeover.Should().BeTrue("a live node must take over the leadership and rewrite the status within TTL + tick");

            // Act-3/Assert-3: смена лидера → СВЕЖИЙ бэкап. Новый лидер
            // (baseline-null) снимает первым тиком: в S3 появляется НОВАЯ
            // пара .db вне knownDb с taken_unix СТРОГО новее kill-времени;
            // instance меты — имя узла из ПЕРЕЧИТАННОГО leader-ключа. Чтения
            // после смерти лидера — через client-грань ЖИВОГО узла: грань
            // node1 (endpoint окружения) могла умереть вместе с лидером.
            var freshDb = default(EtcdS3Object);
            var freshDeadline = DateTimeOffset.UtcNow.AddSeconds(30);
            while (DateTimeOffset.UtcNow < freshDeadline)
            {
                freshDb = (await ListDbObjectsAsync(env, aliveEndpoint)).FirstOrDefault(o =>
                    o.Key.EndsWith(".db", StringComparison.Ordinal) && !knownDb.Contains(o.Key));
                if (freshDb is not null)
                    break;

                await Task.Delay(1000, ct);
            }

            freshDb.Should().NotBeNull("a new leader must export a fresh snapshot pair after the takeover");
            var metaKey = $"{EtcdExportRetention.Prefix}{Path.GetFileName(freshDb!.Key)[..^".db".Length]}.meta.json";
            var target = await new EtcdBackupTargetResolver(env.Gateway, [aliveEndpoint]).ResolveAsync(ct);
            target.IsSuccess.Should().BeTrue(target.Error?.Message);
            await using var s3 = new EtcdSnapshotS3(
                EtcdNodeEnvironment.HostS3Endpoint(target.Value), target.Value.Bucket,
                target.Value.AccessKey, target.Value.SecretKey);
            var metaJson = await s3.GetObjectAsync(metaKey, ct);
            metaJson.IsSuccess.Should().BeTrue(metaJson.Error?.Message);
            using var meta = JsonDocument.Parse(metaJson.Value!);
            var takenUnix = meta.RootElement.GetProperty("taken_unix").GetInt64();
            takenUnix.Should().BeGreaterThan(killUnix,
                "the takeover snapshot must be taken strictly after the leader death");
            var leaderAfter = await env.Gateway.GetAsync(aliveEndpoint, "/etcdnode/leader", ct);
            leaderAfter.IsSuccess.Should().BeTrue(leaderAfter.Error?.Message);
            leaderAfter.Value.Should().NotBeNull("the leader key must exist after the takeover");
            var metaInstance = meta.RootElement.GetProperty("instance").GetString();
            leaderAfter.Value!.Value.Should().Contain(metaInstance,
                "meta.instance of the fresh pair is the new leader node read from /etcdnode/leader");
        }
        finally
        {
            // Возврат узла: контур снова полный для последующих кейсов серии.
            await env.StartNodeAsync(leaderNode);
        }
    }
}
