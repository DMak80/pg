using System.Diagnostics;
using System.Net;
using System.Text.Json;
using FluentAssertions;
using Xunit;

namespace OwnS3.IntegrationTests.E2e.Replication;

/// <summary>
/// A5/A9/A11 — членство и рестарты (t35, план Task 19; R2/R4/R6): backfill нового
/// узла без остановки записи (A5), hot-targets через PUT /admin/replication/targets
/// без даунтайма (A9), рестарты источника/приёмника — drain vs крах→стартовый
/// reconcile (A11).
/// </summary>
[Collection(OwnS3MeshE2eCollection.Name)]
public sealed class MembershipScenarios
{
    private static byte[] Body(int seed) => Enumerable.Range(0, 512).Select(i => (byte)(i + seed)).ToArray();

    [Fact]
    public async Task A5_BackfillNewNode_WritesContinue()
    {
        DockerTrait.SkipIfUnavailable();
        // Контур 2 узла БЕЗ targets (n1 поднимется без соседей); состав — admin-гранью
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a5", 2, async env =>
        {
            // Arrange — n0 знает n1 (env), n1 никого не знает; данные на n0 ДО backfill
            var bucket = $"a5-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, TestContext.Current.CancellationToken);
            for (var i = 0; i < 5; i++)
                await MeshClients.PutObjectAsync(env, 0, bucket, $"pre{i}", Body(i),
                    TestContext.Current.CancellationToken);

            // Act — PUT targets на n1 (сосед n0) и на n0 (сосед n1) — живой контур
            await MeshClients.AdminPutTargetsAsync(env, 1,
                [(env.NodeName(0), env.NodeInternalUrl(0))], TestContext.Current.CancellationToken);
            await MeshClients.AdminPutTargetsAsync(env, 0,
                [(env.NodeName(1), env.NodeInternalUrl(1))], TestContext.Current.CancellationToken);

            // Backfill: n1 забирает существующее (поллинг ≤5 мин); параллельно серия PUT
            // на n0 продолжается — тайминги не деградируют (каждый ≤1 с)
            var sw = Stopwatch.StartNew();
            var maxPutMs = 0L;
            for (var i = 0; i < 5; i++)
            {
                var putSw = Stopwatch.StartNew();
                await MeshClients.PutObjectAsync(env, 0, bucket, $"post{i}", Body(100 + i),
                    TestContext.Current.CancellationToken);
                putSw.Stop();
                maxPutMs = Math.Max(maxPutMs, putSw.ElapsedMilliseconds);
            }
            sw.Stop();

            // Assert — полный backfill (pre + post) на n1; запись не деградировала
            await MeshClients.PollUntilAsync(async () =>
            {
                foreach (var i in Enumerable.Range(0, 5))
                    if (await MeshClients.GetObjectOrNullAsync(env, 1, bucket, $"pre{i}",
                            TestContext.Current.CancellationToken) is null)
                        return false;
                foreach (var i in Enumerable.Range(0, 5))
                    if (await MeshClients.GetObjectOrNullAsync(env, 1, bucket, $"post{i}",
                            TestContext.Current.CancellationToken) is null)
                        return false;
                return true;
            }, "backfill pre+post ключей на n1");
            maxPutMs.Should().BeLessThanOrEqualTo(1000,
                "PUT во время backfill не деградируют (R4)");
        }, fullMeshTargets: false, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A9_HotTargets_AddAndRemove_NoDowntime()
    {
        DockerTrait.SkipIfUnavailable();
        // Контур 4 узла: 0–2 в mesh (env-seed), 3 поднят без targets
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a9", 4, async env =>
        {
            // Arrange — 0–2 знают друг друга (PUT targets), 3 пуст
            var mesh = new List<(string, string)>();
            foreach (var i in Enumerable.Range(0, 3))
                mesh.Add((env.NodeName(i), env.NodeInternalUrl(i)));
            foreach (var i in Enumerable.Range(0, 3))
            {
                var peers = mesh.Where(t => t.Item1 != env.NodeName(i)).ToList();
                await MeshClients.AdminPutTargetsAsync(env, i, peers, TestContext.Current.CancellationToken);
            }
            var bucket = $"a9-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, TestContext.Current.CancellationToken);
            await MeshClients.PutObjectAsync(env, 0, bucket, "warm", Body(1), TestContext.Current.CancellationToken);

            // Act — добавление узла 3: PUT targets на всех живых И на новом
            var withNew = mesh.Append((env.NodeName(3), env.NodeInternalUrl(3))).ToList();
            foreach (var i in Enumerable.Range(0, 3))
                await MeshClients.AdminPutTargetsAsync(env, i, withNew, TestContext.Current.CancellationToken);
            await MeshClients.AdminPutTargetsAsync(env, 3,
                mesh.Append((env.NodeName(3), env.NodeInternalUrl(3)))
                    .Where(t => t.Item1 != env.NodeName(3)).ToList(),
                TestContext.Current.CancellationToken);

            // S3-грань жива во время применения: параллельные GET 200 (≤1 с)
            using (var s3 = MeshClients.S3(env, 0))
            {
                var get = await s3.GetObjectAsync(bucket, "warm", TestContext.Current.CancellationToken);
                get.ResponseStream.Length.Should().BeGreaterThan(0);
            }

            // Новый узел получает backfill (поллинг ≤5 мин)
            await MeshClients.PollUntilAsync(async () =>
                await MeshClients.GetObjectOrNullAsync(env, 3, bucket, "warm",
                    TestContext.Current.CancellationToken) is not null,
                "backfill warm на новый узел 3");

            // Вывод узла: PUT targets без него → исполнители остановлены (status online=false),
            // запись на оставшихся работает
            var without3 = mesh.ToList();
            foreach (var i in Enumerable.Range(0, 3))
                await MeshClients.AdminPutTargetsAsync(env, i, without3, TestContext.Current.CancellationToken);
            await MeshClients.PollUntilAsync(async () =>
            {
                var status = JsonDocument.Parse(
                    await MeshClients.AdminGetStatusAsync(env, 0, TestContext.Current.CancellationToken)).RootElement;
                return !status.GetProperty("neighbors").EnumerateArray()
                    .Any(n => n.GetProperty("name").GetString() == env.NodeName(3));
            }, "узел 3 выведен из состава n0");
            await MeshClients.PutObjectAsync(env, 0, bucket, "after-remove", Body(2),
                TestContext.Current.CancellationToken);
        }, fullMeshTargets: false, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A11_Restarts_SourceDrain_CleanAndCrashTarget()
    {
        DockerTrait.SkipIfUnavailable();
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a11", 2, async env =>
        {
            // Arrange — наполненный контур: ключи на n0, разъехались на n1
            var bucket = $"a11-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, TestContext.Current.CancellationToken);
            for (var i = 0; i < 5; i++)
                await MeshClients.PutObjectAsync(env, 0, bucket, $"k{i}", Body(i),
                    TestContext.Current.CancellationToken);
            await MeshClients.PollUntilAsync(async () =>
                await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "k4",
                    TestContext.Current.CancellationToken) is not null,
                "начальная сходимость");

            // (а) Рестарт ИСТОЧНИКА: docker stop/start n0 при наполненном outbox —
            // до-рассылка, дедуп гасит повторы, сходимость
            var sourceRestarts = (await MeshClients.AdminGetStatusAsync(env, 1,
                TestContext.Current.CancellationToken)).Contains("os3m");
            sourceRestarts.Should().BeTrue("n1 знает соседа до рестарта источника");
            await env.StopNodeAsync(0, ct: TestContext.Current.CancellationToken);
            await env.StartNodeAsync(0, TestContext.Current.CancellationToken);
            // Узел после рестарта не сразу принимает соединения (wait-strategy
            // Testcontainers не переиспользуется после docker start) — поллинг healthz
            await MeshClients.PollUntilAsync(async () =>
            {
                using var http = new HttpClient
                {
                    BaseAddress = new Uri(env.NodeHostUrl(0)),
                    Timeout = TimeSpan.FromSeconds(1),
                };
                try
                {
                    return (await http.GetAsync("/healthz", TestContext.Current.CancellationToken))
                        .StatusCode == HttpStatusCode.OK;
                }
                catch
                {
                    return false;
                }
            }, "n0 поднялся после рестарта источника");
            await MeshClients.PutObjectAsync(env, 0, bucket, "after-restart", Body(9),
                TestContext.Current.CancellationToken);
            await MeshClients.PollUntilAsync(async () =>
                await MeshClients.GetObjectOrNullAsync(env, 1, bucket, "after-restart",
                    TestContext.Current.CancellationToken) is not null,
                "до-рассылка после рестарта источника");

            // (б) Чистый рестарт ПРИЁМНИКА: предусловие — fetchQueueDepth == 0 (drain
            // мгновенный); StopNodeAsync(graceSec: 30) → start → без стартового reconcile
            // (state.json clean; данные на месте, потерь нет)
            await MeshClients.PollUntilAsync(async () =>
            {
                var status = JsonDocument.Parse(
                    await MeshClients.AdminGetStatusAsync(env, 1, TestContext.Current.CancellationToken)).RootElement;
                return status.GetProperty("neighbors").EnumerateArray()
                    .All(n => n.GetProperty("fetchQueueDepth").GetInt64() == 0);
            }, "fetch-очередь n1 пуста перед чистым рестартом");
            await env.StopNodeAsync(1, graceSec: 30, ct: TestContext.Current.CancellationToken);
            await env.StartNodeAsync(1, TestContext.Current.CancellationToken);
            await MeshClients.PollUntilAsync(async () =>
            {
                using var http = new HttpClient
                {
                    BaseAddress = new Uri(env.NodeHostUrl(1)),
                    Timeout = TimeSpan.FromSeconds(1),
                };
                try
                {
                    var health = await http.GetAsync("/healthz", TestContext.Current.CancellationToken);
                    return health.StatusCode == HttpStatusCode.OK;
                }
                catch
                {
                    return false;
                }
            }, "n1 поднялся после чистого рестарта");
            foreach (var i in Enumerable.Range(0, 5))
                (await MeshClients.GetObjectOrNullAsync(env, 1, bucket, $"k{i}",
                        TestContext.Current.CancellationToken))
                    .Should().NotBeNull($"ключ k{i} на месте после чистого рестарта приёмника");

            // (в) Грязный рестарт ПРИЁМНИКА: пауза фетчей (огромный DISK_FREE_MIN через
            // env — configureNode невозможен после старта, поэтому пауза через stop
            // с накоплением уведомлений: PUT на n0 при остановленном n1 → outbox n0
            // копится) → docker kill (SIGKILL, без drain)... n1 уже остановлен —
            // аналог: остановленный n1 + серия PUT (очередь копится на n0) →
            // kill -9 невозможен (нет процесса) → ветка (в) переносится: kill ЖИВОГО
            // приёмника с наполненной fetch-очередью: пауза фетчей недоступна —
            // используем kill сразу после серии PUT (фетчи не успевают дренировать)
            var crashKeys = Enumerable.Range(10, 5).Select(i => $"crash{i}").ToList();
            // ГАРАНТИЯ непустой fetch-очереди n1 на момент kill: PUT крупного объекта
            // (50 МБ — фетч долгий), ждём появления очереди (фетч в процессе/ждёт)
            var bigBody = new byte[50 * 1024 * 1024];
            new Random(42).NextBytes(bigBody);
            await MeshClients.PutObjectAsync(env, 0, bucket, "crash-big", bigBody,
                TestContext.Current.CancellationToken);
            await MeshClients.PollUntilAsync(async () =>
            {
                var status = JsonDocument.Parse(
                    await MeshClients.AdminGetStatusAsync(env, 1, TestContext.Current.CancellationToken)).RootElement;
                return status.GetProperty("neighbors").EnumerateArray()
                    .Any(n => n.GetProperty("fetchQueueDepth").GetInt64() > 0);
            }, "fetch-очередь n1 непуста (фетч crash-big в процессе)");
            foreach (var key in crashKeys)
                await MeshClients.PutObjectAsync(env, 0, bucket, key, Body(50),
                    TestContext.Current.CancellationToken);
            await env.KillNodeAsync(1, TestContext.Current.CancellationToken); // SIGKILL без drain —
            // накопленная очередь (включая crash-big в процессе) теряется из памяти
            await env.StartNodeAsync(1, TestContext.Current.CancellationToken);
            await MeshClients.PollUntilAsync(async () =>
            {
                using var http = new HttpClient
                {
                    BaseAddress = new Uri(env.NodeHostUrl(1)),
                    Timeout = TimeSpan.FromSeconds(1),
                };
                try
                {
                    var health = await http.GetAsync("/healthz", TestContext.Current.CancellationToken);
                    return health.StatusCode == HttpStatusCode.OK;
                }
                catch
                {
                    return false;
                }
            }, "n1 поднялся после краха");
            // Дыры закрыты СТАРТОВЫМ reconcile (грязный маркер) ≤5 мин: потерянная
            // очередь (crash-big + crash-ключи) восстановлена
            await MeshClients.PollUntilAsync(async () =>
            {
                foreach (var key in crashKeys.Append("crash-big"))
                    if (await MeshClients.GetObjectOrNullAsync(env, 1, bucket, key,
                            TestContext.Current.CancellationToken) is null)
                        return false;
                return true;
            }, "дыры краха закрыты стартовым reconcile");
        }, ct: TestContext.Current.CancellationToken);
    }
}
