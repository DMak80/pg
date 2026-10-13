using System.Diagnostics;
using System.Net;
using System.Text.Json;
using FluentAssertions;
using Xunit;

namespace OwnS3.IntegrationTests.E2e.Replication;

/// <summary>
/// Активная проба репликационных линков (t37, глава 07 §2/§6): живой mesh —
/// все reachable + rttMs; остановка соседа — false без rttMs, возврат — true;
/// закрытый порт — false в пределах таймаута; без соседей — {active:false}.
/// Каждый кейс — собственный guid-контур RunScenarioAsync (изоляция/teardown/
/// телеметрия — каноны docs/e2e-isolation.md, docs/e2e-launch.md).
/// </summary>
[Collection(OwnS3MeshE2eCollection.Name)]
public sealed class ProbeScenarios
{
    [Fact]
    public async Task Probe_LiveMesh_AllReachable_TargetFilter_Matrix()
    {
        DockerTrait.SkipIfUnavailable();
        // Контур 3 узла, полный mesh (env-seed): probe всех нод — матрица в обе стороны
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("probe-live", 3, async env =>
        {
            var ct = TestContext.Current.CancellationToken;
            // Act / Assert — probe КАЖДОЙ ноды: все соседи reachable=true с rttMs
            // (покрытие пар в обе стороны — модель потребления зонда t33)
            for (var i = 0; i < env.Nodes; i++)
            {
                var (status, body) = await MeshClients.AdminProbeAsync(env, i, null, ct);
                status.Should().Be(HttpStatusCode.OK);
                var json = JsonDocument.Parse(body).RootElement;
                json.GetProperty("active").GetBoolean().Should().BeTrue();
                var targets = json.GetProperty("targets").EnumerateArray().ToList();
                targets.Should().HaveCount(env.Nodes - 1);
                foreach (var target in targets)
                {
                    target.GetProperty("reachable").GetBoolean().Should().BeTrue(
                        $"линк {env.NodeName(i)}→{target.GetProperty("name").GetString()} жив");
                    target.GetProperty("rttMs").GetInt64().Should().BeGreaterThanOrEqualTo(0);
                }
            }

            // Act — фильтр одного соседа на ноде 0
            var (singleStatus, singleBody) = await MeshClients.AdminProbeAsync(env, 0, env.NodeName(1), ct);

            // Assert — ровно один исход, ровно запрошенный сосед
            singleStatus.Should().Be(HttpStatusCode.OK);
            var single = JsonDocument.Parse(singleBody).RootElement;
            single.GetProperty("targets").EnumerateArray().Should().ContainSingle()
                .Which.GetProperty("name").GetString().Should().Be(env.NodeName(1));
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Probe_StoppedNeighbor_False_RecoversOnStart()
    {
        DockerTrait.SkipIfUnavailable();
        // Контур 2 узла, полный mesh: остановка контейнера соседа → reachable=false
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("probe-stop", 2, async env =>
        {
            var ct = TestContext.Current.CancellationToken;

            // Arrange — до остановки линк жив
            var before = JsonDocument.Parse((await MeshClients.AdminProbeAsync(env, 0, null, ct)).Body)
                .RootElement;
            before.GetProperty("targets").EnumerateArray().Single()
                .GetProperty("reachable").GetBoolean().Should().BeTrue();

            // Act — docker stop соседа
            await env.StopNodeAsync(1, ct: ct);
            var stopped = JsonDocument.Parse((await MeshClients.AdminProbeAsync(env, 0, null, ct)).Body)
                .RootElement;

            // Assert — сосед unreachable, rttMs отсутствует (обрыв — канал не отвечает)
            var outcome = stopped.GetProperty("targets").EnumerateArray().Single();
            outcome.GetProperty("reachable").GetBoolean().Should().BeFalse();
            outcome.TryGetProperty("rttMs", out _).Should().BeFalse(
                "транспортный обрыв — rttMs отсутствует (глава 07 §2)");

            // Act — возврат контейнера (поллинг healthz ≤1 с на попытку)
            await env.StartNodeAsync(1, ct);
            await MeshClients.PollUntilAsync(async () =>
            {
                using var http = new HttpClient
                {
                    BaseAddress = new Uri(env.NodeHostUrl(1)),
                    Timeout = TimeSpan.FromSeconds(1),
                };
                try
                {
                    return (await http.GetAsync("/healthz", ct)).StatusCode == HttpStatusCode.OK;
                }
                catch
                {
                    return false;
                }
            }, "n1 поднялся после остановки");

            // Assert — канал жив снова (доставка не оценивается — только линк)
            var after = JsonDocument.Parse((await MeshClients.AdminProbeAsync(env, 0, null, ct)).Body)
                .RootElement;
            after.GetProperty("targets").EnumerateArray().Single()
                .GetProperty("reachable").GetBoolean().Should().BeTrue();
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Probe_ClosedPortNeighbor_Fast_UnknownTarget_400()
    {
        DockerTrait.SkipIfUnavailable();
        // Контур 2 узла, полный mesh; состав n0 заменяется соседом с закрытым портом
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("probe-closed", 2, async env =>
        {
            var ct = TestContext.Current.CancellationToken;

            // Arrange — PUT targets на n0: сосед «closed-port» на закрытый порт живого
            // контейнера (порт 1 ничего не слушает — connection refused). Тело собираем
            // вручную: AdminPutTargetsAsync парсит индекс узла из имени соседа.
            var body = JsonSerializer.Serialize(new
            {
                targets = new[]
                {
                    new
                    {
                        name = "closed-port",
                        endpoint = $"http://{env.NodeName(1)}:1",
                        accessKey = OwnS3MeshE2eEnvironment.PeerPairAccessKeyOf(0, 1),
                        secretKey = OwnS3MeshE2eEnvironment.PeerPairSecretKeyOf(0, 1),
                    },
                },
            });
            var put = await MeshClients.AdminClient(env, 0).SendAsync(
                "PUT", "/admin/replication/targets", "", body, ct);
            put.StatusCode.Should().Be(HttpStatusCode.Accepted);

            // Act — probe n0 (дефолтный per-target таймаут 2000 мс)
            var sw = Stopwatch.StartNew();
            var (status, probeBody) = await MeshClients.AdminProbeAsync(env, 0, null, ct);
            sw.Stop();

            // Assert — reachable=false за per-target таймаут (отказ быстрый: refused;
            // бюджет — дефолт 2000 мс с запасом на DNS/ретраи контейнера)
            status.Should().Be(HttpStatusCode.OK);
            var outcome = JsonDocument.Parse(probeBody).RootElement.GetProperty("targets")
                .EnumerateArray().Single();
            outcome.GetProperty("name").GetString().Should().Be("closed-port");
            outcome.GetProperty("reachable").GetBoolean().Should().BeFalse();
            outcome.TryGetProperty("rttMs", out _).Should().BeFalse();
            sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(5),
                "probe закрывается per-target таймаутом, не висит");

            // Act — неизвестное имя фильтра
            var (unknownStatus, unknownBody) = await MeshClients.AdminProbeAsync(env, 0, "ghost", ct);

            // Assert — 400 с пояснением (глава 07 §2)
            unknownStatus.Should().Be(HttpStatusCode.BadRequest);
            unknownBody.Should().Contain("ghost");
        }, ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Probe_NoTargets_Inactive()
    {
        DockerTrait.SkipIfUnavailable();
        // Контур 2 узла БЕЗ targets (env-seed пуст): probe — «соседей нет»
        await OwnS3MeshE2eEnvironment.RunScenarioAsync("probe-inactive", 2, async env =>
        {
            // Act
            var (status, body) = await MeshClients.AdminProbeAsync(env, 0, null,
                TestContext.Current.CancellationToken);

            // Assert — осмысленный ответ, не ошибка (глава 07 §2)
            status.Should().Be(HttpStatusCode.OK);
            var json = JsonDocument.Parse(body).RootElement;
            json.GetProperty("active").GetBoolean().Should().BeFalse();
            json.GetProperty("targets").GetArrayLength().Should().Be(0);
        }, fullMeshTargets: false, ct: TestContext.Current.CancellationToken);
    }
}
