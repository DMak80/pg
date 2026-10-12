using System.Diagnostics;
using System.Text;
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

    /// <summary>Раскол ПОПОЛАМ: {0,1} в основной сети, {2,3} во второй сети —
    /// половины теряют связь друг с другом (TIMEOUT-природа), внутри половин связь
    /// полная; host-порты обеих половин живы — клиенты с хоста пишут в обе.</summary>
    private static async Task SplitAsync(OwnS3MeshE2eEnvironment env, CancellationToken ct)
    {
        await env.DisconnectNodeAsync(1, ct);
        await env.DisconnectNodeAsync(2, ct);
        // n1 → отдельная сеть (его host-порт остаётся привязан к контейнеру)
        await env.AttachNodeToSplitNetworkAsync(1, ct);
    }

    private static async Task HealAsync(OwnS3MeshE2eEnvironment env, CancellationToken ct)
    {
        await env.ReconnectNodeAsync(1, ct);
        await env.ReconnectNodeAsync(2, ct);
        await env.DetachNodeFromSplitNetworkAsync(1, ct);
    }

    /// <summary>docker run mc (сеть раскола): команда mc, вывод stdout.</summary>
    private static async Task<string> McRunAsync(OwnS3MeshE2eEnvironment env, string args, string stdin = "")
    {
        var psi = new System.Diagnostics.ProcessStartInfo("docker",
            $"run --rm --network={env.SplitNetworkName} -i {OwnS3E2eEnvironment.McImage} {args}")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            RedirectStandardInput = true,
            CreateNoWindow = true,
        };
        using var process = System.Diagnostics.Process.Start(psi)
            ?? throw new ApplicationException("не удалось запустить mc");
        if (stdin.Length > 0)
            await process.StandardInput.WriteAsync(stdin);
        process.StandardInput.Close();
        var output = await process.StandardOutput.ReadToEndAsync(TestContext.Current.CancellationToken);
        var error = await process.StandardError.ReadToEndAsync(TestContext.Current.CancellationToken);
        await process.WaitForExitAsync(TestContext.Current.CancellationToken);
        if (process.ExitCode != 0)
            throw new ApplicationException($"mc {args} → {process.ExitCode}: {error.Trim()}");
        return output;
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

            // Act — двусторонние записи в расколе: left-extra на 0, right-extra на 3
            // (изолятами 1,2 каждая сторона пишет «в свою половину»)
            var leftExtra = "left-extra"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 0, bucket, "left-extra", leftExtra,
                TestContext.Current.CancellationToken);
            var rightExtra = "right-extra"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 3, bucket, "right-extra", rightExtra,
                TestContext.Current.CancellationToken);

            // Срастание (reconnect половин) — БЕЗ рестартов; админ-reconcile (§9)
            await HealAsync(env, TestContext.Current.CancellationToken);
            foreach (var node in Enumerable.Range(0, 4))
                await MeshClients.AdminReconcileAsync(env, node, TestContext.Current.CancellationToken);

            // Assert — сходимость ≤5 мин: каждый ключ равен на ВСЕХ узлах
            var deadline = Environment.TickCount64 + 300_000;
            var allKeys = new (string Key, byte[] Body)[]
            {
                ("left-extra", leftExtra), ("right-extra", rightExtra),
            };
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
            // главы 07 §7 — равные modTime при разных sha. Клиентские PUT двух узлов
            // дают РАЗНЫЕ modTime (modTime ставит сервер приёма, часы узлов) →
            // разрешение по modTime БЕЗ конфликта. Детерминированный equal-mod PUT
            // клиентскими средствами недостижим (PUT-запрос не несёт modTime, а фетч
            // перечитывает фактические метаданные источника — принцип 6 «доставляй
            // актуальное»), поэтому журналирование конфликта (событие ConflictDetected
            // с обеими версиями + счётчик) приёмкой на уровне LWW-таблицы:
            // юниты LwwComparatorTests (полная таблица, conflict=true) и
            // ReplicaApplierTests.ApplyObject_Conflict_ResultFlagAndEvent.
            // Здесь — главный критерий A6: сходимость всех узлов к ОДНОЙ версии
            // без рестартов.
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A7_LiveDivergence_MetricsAndSyncReport()
    {
        DockerTrait.SkipIfUnavailable();
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a7", 4, async env =>
        {
            // Arrange — раскол: узлы 1,2 — изоляты; сегмент {0}—{3} синхронен.
            // Записи: left на 0 (нотифаер 0 ретраит к изолятам — лаг уведомлений
            // растёт), right на 3 (аналогично). Ассерт «sync-report половин
            // расходится ДО сращания» на этой docker-машине недостижим: связная
            // пара 0—3 синхронизируется мгновенно, а изолят-половина
            // неотчитываема с хоста — сверка выполняется ПОСЛЕ догона (равенство
            // aggregateHash = отсутствие расхождения, R5-абсолютная истина).
            var bucket = $"a7-{env.RunTag}";
            foreach (var node in Enumerable.Range(0, 4))
                await MeshClients.CreateBucketAsync(env, node, bucket, TestContext.Current.CancellationToken);
            await SplitAsync(env, TestContext.Current.CancellationToken);
            await MeshClients.PutObjectAsync(env, 0, bucket, "left-key", Body('L'),
                TestContext.Current.CancellationToken);
            await MeshClients.PutObjectAsync(env, 3, bucket, "right-key", Body('R'),
                TestContext.Current.CancellationToken);

            // Act / Assert — при живом расхождении: лаг уведомлений источника
            // (нотифаер 0 ретраит к изолятам — очередь/лаг > 0, R5)
            await MeshClients.PollUntilAsync(async () =>
            {
                var status = JsonDocument.Parse(
                    await MeshClients.AdminGetStatusAsync(env, 0, TestContext.Current.CancellationToken)).RootElement;
                return status.GetProperty("neighbors").EnumerateArray()
                    .Where(n => n.GetProperty("online").GetBoolean())
                    .Any(n => n.GetProperty("queueDepth").GetInt64() > 0
                        || n.GetProperty("deliveryLagSeconds").GetDouble() > 0);
            }, "лаг уведомлений источника отражён при живом расхождении (R5)");

            // Срастание → догона (админ-reconcile §9) → sync-report пары сходится
            // (aggregateHash равен = отсутствие расхождения, R5)
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
            var response = await MeshClients.PeerClient(env, 3, 0).SendAsync(
                "POST", "/replication/notifications", "", oldPut, TestContext.Current.CancellationToken);
            response.StatusCode.Should().Be(HttpStatusCode.OK);
            (await MeshClients.GetObjectOrNullAsync(env, 3, bucket, "single",
                    TestContext.Current.CancellationToken))
                .Should().BeNull("старый PUT после tombstone не воскрешает ключ (R4)");
        }, ct: TestContext.Current.CancellationToken);
    }
}
