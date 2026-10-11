using System.Net;
using System.Diagnostics;
using FluentAssertions;
using Xunit;
using OwnS3.IntegrationTests.E2e;

namespace OwnS3.IntegrationTests.E2e.Replication;

// McImage — из namespace E2e (см. using ниже).

/// <summary>
/// A1–A3 — запись без штрафов (t35, план Task 17; R1/R3): серия 10 PUT ≤1 с
/// СУММАРНО при stop всех целей (A1, 2/3/5-узловый контур) и сетевом изоляте
/// писателя (A2); изолированный сервер остаётся полноценным S3 (A3).
/// Каждая Theory-кейс — отдельный RunScenarioAsync (свой контур, полный teardown).
/// </summary>
[Collection(OwnS3MeshE2eCollection.Name)]
public sealed class WriteIsolationScenarios
{
    private static byte[] Body(int seed) => Enumerable.Range(0, 1024).Select(i => (byte)(i + seed)).ToArray();

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    public async Task A1_TenPuts_Under1s_WithAllTargetsStopped(int nodes)
    {
        DockerTrait.SkipIfUnavailable();
        await OwnS3MeshE2eEnvironment.RunScenarioAsync($"a1-n{nodes}", nodes, async env =>
        {
            // Arrange — бакет на узле-писателе (0); ВСЕ соседи остановлены (TARGET-природа отказа)
            var bucket = $"a1-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, TestContext.Current.CancellationToken);
            for (var i = 1; i < nodes; i++)
                await env.StopNodeAsync(i, ct: TestContext.Current.CancellationToken);

            // Act — серия 10 PUT (каждый — свой HTTP с таймаутом 5 с); Stopwatch суммарно
            var sw = Stopwatch.StartNew();
            for (var i = 0; i < 10; i++)
                await MeshClients.PutObjectAsync(env, 0, bucket, $"k{i}", Body(i),
                    TestContext.Current.CancellationToken);
            sw.Stop();

            // Assert — суммарно ≤1 с, 0 блокировок, 0 отказов записи (R1)
            sw.ElapsedMilliseconds.Should().BeLessThanOrEqualTo(1000,
                $"серия 10 PUT при {nodes - 1} остановленных целях не должна деградировать (R1)");
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A2_TenPuts_Under1s_WithNeighborsDisconnected()
    {
        DockerTrait.SkipIfUnavailable();
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a2", 3, async env =>
        {
            // Arrange — контур 3; СОСЕДИ писателя отключены от сети: его исходящие
            // к целям висят (TIMEOUT-природа, не refused), входящие клиентские живы
            var bucket = $"a2-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, TestContext.Current.CancellationToken);
            try
            {
                for (var i = 1; i < 3; i++)
                    await env.DisconnectNodeAsync(i, TestContext.Current.CancellationToken);

                // Act — серия 10 PUT в писателя (его notifier утопает в таймаутах)
                var sw = Stopwatch.StartNew();
                for (var i = 0; i < 10; i++)
                    await MeshClients.PutObjectAsync(env, 0, bucket, $"k{i}", Body(i),
                        TestContext.Current.CancellationToken);
                sw.Stop();

                // Assert — суммарно ≤1 с (антиурок MinIO: блок минутами; R1)
                sw.ElapsedMilliseconds.Should().BeLessThanOrEqualTo(1000,
                    "серия 10 PUT при недоступных (timeout) целях не должна деградировать");
            }
            finally
            {
                // reconnect соседей в finally-ветке сценария (срастание для teardown)
                for (var i = 1; i < 3; i++)
                    await env.ReconnectNodeAsync(i, TestContext.Current.CancellationToken);
            }
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A3_IsolatedNode_HealthAndGetPut_AllUnder1s()
    {
        DockerTrait.SkipIfUnavailable();
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("a3", 3, async env =>
        {
            // Arrange — данные записаны ДО изоляции; узел-0 изолирован (disconnect:
            // его исходящие висят, клиенты — из его сети, см. R3 «локальных клиентов»)
            var bucket = $"a3-{env.RunTag}";
            await MeshClients.CreateBucketAsync(env, 0, bucket, TestContext.Current.CancellationToken);
            var existing = "pre-isolation"u8.ToArray();
            await MeshClients.PutObjectAsync(env, 0, bucket, "existing", existing,
                TestContext.Current.CancellationToken);

            // «Изолят» узла-0 = disconnect СОСЕДЕЙ: исходящие узла-0 висят (TIMEOUT),
            // клиентский путь узла жив (docker disconnect самого узла отнял бы и
            // host-публикацию — клиентский путь недостижим; изолят с живым клиентом —
            // через соседей). Всё ниже — ≤1 с (R3).
            for (var i = 1; i < 3; i++)
                await env.DisconnectNodeAsync(i, TestContext.Current.CancellationToken);
            try
            {
                // healthz ≤1 с (несколько проб)
                using (var http = new HttpClient
                       {
                           BaseAddress = new Uri(env.NodeHostUrl(0)),
                           Timeout = TimeSpan.FromSeconds(1),
                       })
                {
                    for (var probe = 0; probe < 3; probe++)
                    {
                        var sw = Stopwatch.StartNew();
                        var health = await http.GetAsync("/healthz", TestContext.Current.CancellationToken);
                        sw.Stop();
                        health.StatusCode.Should().Be(HttpStatusCode.OK);
                        sw.ElapsedMilliseconds.Should().BeLessThanOrEqualTo(1000, "healthz ≤1 с (R3)");
                    }
                }

                // GET ранее записанного объекта работает (≤1 с — клиентский таймаут 1 с)
                var got = await MeshClients.GetObjectOrNullAsync(env, 0, bucket, "existing",
                    TestContext.Current.CancellationToken);
                got.Should().NotBeNull().And.BeEquivalentTo(existing);

                // PUT+GET новых работают (локальная запись+чтение — R3)
                var fresh = "fresh-body"u8.ToArray();
                await MeshClients.PutObjectAsync(env, 0, bucket, "fresh", fresh,
                    TestContext.Current.CancellationToken);
                (await MeshClients.GetObjectOrNullAsync(env, 0, bucket, "fresh",
                    TestContext.Current.CancellationToken)).Should().BeEquivalentTo(fresh);
            }
            finally
            {
                for (var i = 1; i < 3; i++)
                    await env.ReconnectNodeAsync(i, TestContext.Current.CancellationToken);
            }
        }, ct: TestContext.Current.CancellationToken);
    }
}
