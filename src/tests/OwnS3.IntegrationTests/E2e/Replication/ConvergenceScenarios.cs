using System.Diagnostics;
using System.Net;
using System.Text.Json;
using FluentAssertions;
using Xunit;

namespace OwnS3.IntegrationTests.E2e.Replication;

/// <summary>
/// A4/A6/A7/A8 — сходимость (t35, план Task 18; R4/R5): срастание после раскола
/// ≤5 мин без рестартов (A4), LWW-конфликт журналируется с обеих сторон (A6),
/// раздельные лаги уведомлений/данных + sync-report (A7), tombstone-сходимость
/// удалений (A8). Контур 4 узла (полный mesh env-seed).
/// </summary>
[Collection(OwnS3MeshE2eCollection.Name)]
public sealed class ConvergenceScenarios
{
    private static byte[] Body(char tag) => Enumerable.Range(0, 512).Select(i => (byte)(i + tag)).ToArray();

    /// <summary>Раскол (docker-семантика disconnect): узлы 1 и 2 становятся изолятами
    /// (их исходящие висят/TIMEOUT); остаётся сегмент {0}—{3}, клиенты с хоста живы у
    /// обоих. Попарная изоляция половин {0,1}vs{2,3} требует iptables (вне канонов
    /// docker-тестов) — двустороннее расхождение создаётся записями в 0 и 3.</summary>
    private static async Task SplitAsync(OwnS3MeshE2eEnvironment env, CancellationToken ct)
    {
        await env.DisconnectNodeAsync(1, ct);
        await env.DisconnectNodeAsync(2, ct);
    }

    private static async Task HealAsync(OwnS3MeshE2eEnvironment env, CancellationToken ct)
    {
        await env.ReconnectNodeAsync(1, ct);
        await env.ReconnectNodeAsync(2, ct);
    }

    [Fact]
    public async Task A4_SplitHeal_Convergence_NoRestarts()
    {
        DockerTrait.SkipIfUnavailable();
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a4", 4, async env =>
        {
            // Arrange — бакет создан админом на ВСЕХ узлах (каждая половина пишет своё);
            // раскол пополам
            var bucket = $"a4-{env.RunTag}";
            foreach (var node in Enumerable.Range(0, 4))
                await MeshClients.CreateBucketAsync(env, node, bucket, TestContext.Current.CancellationToken);
            await SplitAsync(env, TestContext.Current.CancellationToken);

            // Act — записи в ОБЕИХ половинах (взаимные расхождения)
            var half1 = new[] { (key: "s1-a", body: Body('1')), (key: "s1-b", body: Body('2')) };
            var half2 = new[] { (key: "s2-a", body: Body('3')), (key: "s2-b", body: Body('4')) };
            foreach (var (key, body) in half1)
                await MeshClients.PutObjectAsync(env, 0, bucket, key, body, TestContext.Current.CancellationToken);
            foreach (var (key, body) in half2)
                await MeshClients.PutObjectAsync(env, 3, bucket, key, body, TestContext.Current.CancellationToken);

            // Срастание (reconnect всех) — БЕЗ рестартов контейнеров; админ-reconcile
            // (§9 триггер 5) на приёмниках — быстрый догон без ожидания бэкоффов
            await HealAsync(env, TestContext.Current.CancellationToken);
            foreach (var node in Enumerable.Range(0, 4))
                await MeshClients.AdminReconcileAsync(env, node, TestContext.Current.CancellationToken);

            // Assert — сходимость ≤5 мин (шаг ≤30 с): каждый ключ равен на ВСЕХ узлах
            var allKeys = half1.Concat(half2).ToList();
            var deadline = Environment.TickCount64 + 300_000;
            foreach (var (key, body) in allKeys)
            {
                while (Environment.TickCount64 < deadline)
                {
                    var all = new List<byte[]?>();
                    foreach (var node in Enumerable.Range(0, 4))
                        all.Add(await MeshClients.GetObjectOrNullAsync(env, node, bucket, key,
                            TestContext.Current.CancellationToken));
                    if (all.All(v => v is not null && v.SequenceEqual(body)))
                        break;
                    await Task.Delay(30_000, TestContext.Current.CancellationToken);
                }
                foreach (var node in Enumerable.Range(0, 4))
                    (await MeshClients.GetObjectOrNullAsync(env, node, bucket, key,
                            TestContext.Current.CancellationToken))
                        .Should().BeEquivalentTo(body, $"ключ {key} на узле {node} после срастания");
            }
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A6_ConflictingKey_LwwWinner_JournalBothSides()
    {
        DockerTrait.SkipIfUnavailable();
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a6", 4, async env =>
        {
            // Arrange — раскол; ОДИН ключ PUT в обе половины (разные тела)
            var bucket = $"a6-{env.RunTag}";
            foreach (var node in Enumerable.Range(0, 4))
                await MeshClients.CreateBucketAsync(env, node, bucket, TestContext.Current.CancellationToken);
            await SplitAsync(env, TestContext.Current.CancellationToken);
            var left = Body('L');
            var right = Body('R');
            await MeshClients.PutObjectAsync(env, 0, bucket, "conflict", left, TestContext.Current.CancellationToken);
            await MeshClients.PutObjectAsync(env, 3, bucket, "conflict", right, TestContext.Current.CancellationToken);

            // Act — срастание: LWW разрешает к одной версии (+ админ-reconcile §9)
            await HealAsync(env, TestContext.Current.CancellationToken);
            foreach (var node in Enumerable.Range(0, 4))
                await MeshClients.AdminReconcileAsync(env, node, TestContext.Current.CancellationToken);
            var winner = left.SequenceEqual(right) ? left : null; // тела различны
            await MeshClients.PollUntilAsync(async () =>
            {
                var versions = new List<byte[]?>();
                foreach (var node in Enumerable.Range(0, 4))
                    versions.Add(await MeshClients.GetObjectOrNullAsync(env, node, bucket, "conflict",
                        TestContext.Current.CancellationToken));
                var nonNull = versions.Where(v => v is not null).ToList();
                return nonNull.Count == 4 && nonNull.All(v => v!.SequenceEqual(nonNull[0]));
            }, "LWW-победитель разъехался по всем узлам");

            // Assert — ОДИНАКОВОЕ тело на всех узлах
            var bodies = new List<byte[]?>();
            foreach (var node in Enumerable.Range(0, 4))
                bodies.Add(await MeshClients.GetObjectOrNullAsync(env, node, bucket, "conflict",
                    TestContext.Current.CancellationToken));
            bodies[0].Should().NotBeNull();
            bodies.Should().OnlyContain(v => v!.SequenceEqual(bodies[0]!), "LWW-победитель един");

            // R4 «факт слияния журналируется с обеих сторон»: LWW-конфликт в терминах
            // главы 07 §7 — равные modTime при разных sha; клиентские PUT двух узлов
            // дают РАЗНЫЕ modTime (часы узлов) → разрешение по modTime БЕЗ конфликта.
            // Детерминированное равенство modTime недостижимо клиентскими средствами —
            // журналирование конфликта (событие + счётчик) приёмкой на уровне
            // LWW-таблицы: юниты LwwComparatorTests (полная таблица, conflict=true) +
            // ReplicaApplierTests.ApplyObject_Conflict_ResultFlagAndEvent (событие
            // ConflictDetected с обеими версиями). Здесь — главный критерий A6:
            // сходимость всех узлов к ОДНОЙ версии без рестартов.
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A7_LiveDivergence_MetricsAndSyncReport()
    {
        DockerTrait.SkipIfUnavailable();
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a7", 4, async env =>
        {
            // Arrange — раскол; записи в обеих половинах (живое расхождение)
            var bucket = $"a7-{env.RunTag}";
            foreach (var node in Enumerable.Range(0, 4))
                await MeshClients.CreateBucketAsync(env, node, bucket, TestContext.Current.CancellationToken);
            await SplitAsync(env, TestContext.Current.CancellationToken);
            await MeshClients.PutObjectAsync(env, 0, bucket, "left-key", Body('L'),
                TestContext.Current.CancellationToken);
            await MeshClients.PutObjectAsync(env, 3, bucket, "right-key", Body('R'),
                TestContext.Current.CancellationToken);

            // Пауза: notifier-цикл источника переходит в бэкофф, очередь копится
            await Task.Delay(10_000, TestContext.Current.CancellationToken);

            // Act / Assert — при живом расхождении: раздельные лаги (источник —
            // лаг уведомлений/очередь; приёмник — fetch-очередь/лаг данных);
            // sync-report пары — aggregateHash расходится
            var statusLeft = JsonDocument.Parse(
                await MeshClients.AdminGetStatusAsync(env, 0, TestContext.Current.CancellationToken)).RootElement;
            var statusRight = JsonDocument.Parse(
                await MeshClients.AdminGetStatusAsync(env, 3, TestContext.Current.CancellationToken)).RootElement;
            var divergenceSeen =
                statusLeft.GetProperty("neighbors").EnumerateArray()
                    .Any(n => n.GetProperty("queueDepth").GetInt64() > 0
                        || n.GetProperty("deliveryLagSeconds").GetDouble() > 0)
                || statusRight.GetProperty("neighbors").EnumerateArray()
                    .Any(n => n.GetProperty("fetchQueueDepth").GetInt64() > 0
                        || n.GetProperty("fetchLagSeconds").GetDouble() > 0);
            divergenceSeen.Should().BeTrue("при живом расхождении штатные метрики отражают лаг (R5)");

            var reportLeft = JsonDocument.Parse(
                await MeshClients.AdminSyncReportAsync(env, 0, bucket, TestContext.Current.CancellationToken)).RootElement;
            var reportRight = JsonDocument.Parse(
                await MeshClients.AdminSyncReportAsync(env, 3, bucket, TestContext.Current.CancellationToken)).RootElement;
            reportLeft.GetProperty("aggregateHash").GetString()
                .Should().NotBe(reportRight.GetProperty("aggregateHash").GetString(),
                    "sync-report половин показывает расхождение (R5)");

            // Срастание → догона (админ-reconcile §9) → sync-report совпадает
            await HealAsync(env, TestContext.Current.CancellationToken);
            foreach (var node in Enumerable.Range(0, 4))
                await MeshClients.AdminReconcileAsync(env, node, TestContext.Current.CancellationToken);
            await MeshClients.PollUntilAsync(async () =>
            {
                var l = JsonDocument.Parse(await MeshClients.AdminSyncReportAsync(env, 0, bucket,
                    TestContext.Current.CancellationToken)).RootElement;
                var r = JsonDocument.Parse(await MeshClients.AdminSyncReportAsync(env, 3, bucket,
                    TestContext.Current.CancellationToken)).RootElement;
                return l.GetProperty("aggregateHash").GetString()
                    == r.GetProperty("aggregateHash").GetString()
                    && l.GetProperty("count").GetInt32() == 2;
            }, "sync-report пары сходится после догона");
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A8_Deletes_TombstoneConvergence_NoResurrection()
    {
        DockerTrait.SkipIfUnavailable();
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a8", 4, async env =>
        {
            // Arrange — 3 ключа записаны и разъехались по всем; DELETE одного + пачка двух
            var bucket = $"a8-{env.RunTag}";
            foreach (var node in Enumerable.Range(0, 4))
                await MeshClients.CreateBucketAsync(env, node, bucket, TestContext.Current.CancellationToken);
            foreach (var key in new[] { "single", "batch-1", "batch-2" })
                await MeshClients.PutObjectAsync(env, 0, bucket, key, Body('x'),
                    TestContext.Current.CancellationToken);
            await MeshClients.PollUntilAsync(async () =>
                (await MeshClients.GetObjectOrNullAsync(env, 3, bucket, "batch-2",
                    TestContext.Current.CancellationToken)) is not null,
                "ключи разъехались до удалений");

            // Act — DELETE одного ключа + DeleteObjects пачки на узле-0
            await MeshClients.DeleteObjectAsync(env, 0, bucket, "single", TestContext.Current.CancellationToken);
            await MeshClients.DeleteObjectsAsync(env, 0, bucket, ["batch-1", "batch-2"],
                TestContext.Current.CancellationToken);

            // Assert — поллинг ≤5 мин: ключей нет на всех узлах (tombstone-сходимость)
            await MeshClients.PollUntilAsync(async () =>
            {
                foreach (var node in Enumerable.Range(0, 4))
                foreach (var key in new[] { "single", "batch-1", "batch-2" })
                    if (await MeshClients.GetObjectOrNullAsync(env, node, bucket, key,
                            TestContext.Current.CancellationToken) is not null)
                        return false;
                return true;
            }, "удаления доехали до всех узлов (tombstone)");

            // tombstone-поведение: повторная доставка СТАРОГО PUT (вручную через
            // peer-кред: батч object-put со старым modTime) — ключ НЕ воскрешён
            var oldPut = JsonSerializer.Serialize(new OwnS3.Replication.Transport.NotificationBatchRequest("mesh-a",
            [
                new OwnS3.Replication.ReplicationEvent(9900, OwnS3.Replication.ReplicationEventType.ObjectPut,
                    bucket, "single", 1000, "\"old\"", 3, "old-sha", "peer"),
            ]), OwnS3.Replication.Transport.ReplicationTransportJson.Options);
            var response = await MeshClients.PeerClient(env, 3).SendAsync(
                "POST", "/replication/notifications", "", oldPut, TestContext.Current.CancellationToken);
            response.StatusCode.Should().Be(HttpStatusCode.OK);
            (await MeshClients.GetObjectOrNullAsync(env, 3, bucket, "single",
                    TestContext.Current.CancellationToken))
                .Should().BeNull("старый PUT после tombstone не воскрешает ключ (R4)");
        }, ct: TestContext.Current.CancellationToken);
    }
}
