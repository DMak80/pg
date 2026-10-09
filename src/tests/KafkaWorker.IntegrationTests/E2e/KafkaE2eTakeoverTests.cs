using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using Xunit;

namespace KafkaWorker.IntegrationTests.E2e;

// Takeover-E2E KafkaWorker (spec t20 §6): kill ДЕРЖАТЕЛЯ клэйма посреди
// provisioning 3-брокерного KRaft-кластера — выживший инстанс доигрывает
// операцию (K3..K5 → done) без дублей, с целым контроль-плейном и живым
// датаплейном (инварианты I1–I7). Держатель резолвится по etcd-фактам ПЕРЕД
// kill (claims.instance → api-ключ → url → порт → сопоставление с
// Api1/Api2Port) — kill доказательно попадает в держателя. Сид кластера — в
// стиле панели (копия формата KafkaClusterFixture.SeedClusterAsync(cluster, 3)).
// Изоляция/телеметрия — docs/e2e-isolation.md + docs/e2e-launch.md; ожидания —
// только KafkaE2ePhase.WaitAsync (docker-CLI в тиках запрещён — якорь
// docker-проверяет в теле условия). Гейт: PGW_TEST_DOCKER=1.
public class KafkaE2eTakeoverTests
{
    [Fact]
    public async Task Kill_ClaimHolderMidProvision_SurvivorFinishesNoDuplicates()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        var enabled = Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") == "1";
        if (!enabled)
        {
            Assert.Skip("PGW_TEST_DOCKER=1 не задан — docker-E2E пропущен");
            return;
        }

        // Arrange A1: двухинстансный контур (сеть + etcd + ew1/ew2 + общий TLS).
        await using var fx = await KafkaE2eEnvironment.StartAsync("kfw-takeover");
        var cluster = $"tkw{fx.ClusterTag}";
        var ct = TestContext.Current.CancellationToken;

        try
        {
            // A1: готовность ОБОИХ воркеров — по 2 lease-ключа api/instances
            // (бюджет ≤ 100 с; ключ жив = процесс поднялся, keepalive тикает).
            var bothUp = await KafkaE2ePhase.WaitAsync(fx, "ktake-instances-up", async () =>
                (await fx.RangeAsync("/kafkaworker/api/", ct)).Count == 2
                && (await fx.RangeAsync("/kafkaworker/instances/", ct)).Count == 2,
                TimeSpan.FromSeconds(100), ct,
                progress: async () =>
                {
                    var api = (await fx.RangeAsync("/kafkaworker/api/", ct)).Count;
                    var instances = (await fx.RangeAsync("/kafkaworker/instances/", ct)).Count;
                    return $"api={api}, instances={instances}";
                });
            bothUp.Should().BeTrue("оба инстанса обязаны опубликовать дискавери-ключи за 100 с");

            // A2: сид кластера в стиле панели (копия формата
            // KafkaClusterFixture.SeedClusterAsync): config NOT_INITIALIZED +
            // state/resources на broker1..3 — подхватит ReconcileLoop → K1.
            await fx.PutAsync($"/kafka/clusters/{cluster}/config",
                $$"""{"brokers":3,"replication_factor":3,"min_insync_replicas":2,"default_partitions":3,"default_retention_ms":604800000,"created_unix":1756500000,"state":"NOT_INITIALIZED"}""", ct);
            for (var k = 1; k <= 3; k++)
            {
                await fx.PutAsync($"/kafka/clusters/{cluster}/brokers/broker{k}/state", "NOT_INITIALIZED", ct);
                await fx.PutAsync($"/kafka/clusters/{cluster}/brokers/broker{k}/resources",
                    """{"cpu":"1","mem":"1Gi","disk":"10Gi"}""", ct);
            }

            // A3: якорь «посередине» provisioning — ВСЕ три условия одновременно:
            // 1) контейнер broker1 существует (docker ps -a — в теле условия;
            // тик — только etcd-срез); 2) config ещё содержит "state" (K5 не
            // дошёл); 3) клэйм жив. Окно K3..K5 — внутри K4-бута брокеров.
            var midProvision = await KafkaE2ePhase.WaitAsync(fx, "ktake-mid-provision", async () =>
                (await fx.ListContainerNamesAsync($"kfw-{cluster}-broker1", all: true)).Count > 0
                && await fx.GetOrNullAsync($"/kafka/clusters/{cluster}/config", ct) is { } config
                    && config.Contains("state")
                && await fx.GetOrNullAsync($"/kafkaworker/claims/{cluster}", ct) is not null,
                TimeSpan.FromSeconds(120), ct,
                progress: async () =>
                {
                    var work = await fx.GetOrNullAsync($"/kafkaworker/work/{cluster}", ct);
                    var claim = await fx.GetOrNullAsync($"/kafkaworker/claims/{cluster}", ct) is not null;
                    var alloc = await fx.GetOrNullAsync($"/kafkaworker/portalloc/{cluster}", ct) is not null;
                    return $"work={Trunc(work ?? "нет")}, claim={claim}, portalloc={alloc}";
                });
            midProvision.Should().BeTrue("якорь обязан поймать окно K3..K5 (брокер1 создан, state жив, клэйм жив) за 120 с");

            // A4: резолв ДЕРЖАТЕЛЯ по etcd-фактам и kill (одномоментно).
            var claimJson = await fx.GetOrNullAsync($"/kafkaworker/claims/{cluster}", ct);
            claimJson.Should().NotBeNull("клэйм существует непосредственно перед kill (ассерт A4)");
            string holderInstance;
            using (var doc = JsonDocument.Parse(claimJson!))
                holderInstance = doc.RootElement.GetProperty("instance").GetString()!;
            holderInstance.Should().NotBeNullOrEmpty("claims/{C}.instance обязан быть непустым");

            var holderUrl = await fx.GetOrNullAsync($"/kafkaworker/api/{holderInstance}", ct);
            holderUrl.Should().NotBeNull($"api-ключ держателя {holderInstance} обязан существовать (общий lease с instance-ключом)");
            var holderPort = new Uri(JsonDocument.Parse(holderUrl!).RootElement.GetProperty("url").GetString()!).Port;

            var matched = new[] { fx.Api1Port, fx.Api2Port }.Count(p => p == holderPort);
            matched.Should().Be(1, $"порт держателя {holderPort} обязан совпасть ровно с одним известным api-портом "
                + $"({fx.Api1Port}/{fx.Api2Port}); claims={claimJson}, api-ключи={await DumpApiKeysAsync(fx, ct)}");

            var holderName = holderPort == fx.Api1Port ? fx.Worker1Name : fx.Worker2Name;
            var survivorName = holderPort == fx.Api1Port ? fx.Worker2Name : fx.Worker1Name;

            // Act: docker kill держателя (без рестарта — воскресать не должен,
            // доносит ТОЛЬКО выживший).
            await fx.RunDockerAsync(["kill", holderName], ct);

            // A5: Assert-takeover — выживший доигрывает provisioning (≤ 360 с;
            // K4-бут 3 брокеров — десятки секунд; прогресс-тик: config.state,
            // work-фаза, число ключей api/instances).
            var finished = await KafkaE2ePhase.WaitAsync(fx, "ktake-provision-done", async () =>
            {
                var config = await fx.GetOrNullAsync($"/kafka/clusters/{cluster}/config", ct);
                var work = await fx.GetOrNullAsync($"/kafkaworker/work/{cluster}", ct);
                return config is not null && !config.Contains("state")
                    && work is not null && work.Contains("provision") && work.Contains("done");
            }, TimeSpan.FromSeconds(360), ct,
                progress: async () =>
                {
                    var config = await fx.GetOrNullAsync($"/kafka/clusters/{cluster}/config", ct);
                    var configState = (config ?? "нет").Contains("state") ? "жива" : "нет";
                    var work = await fx.GetOrNullAsync($"/kafkaworker/work/{cluster}", ct);
                    var api = (await fx.RangeAsync("/kafkaworker/api/", ct)).Count;
                    var instances = (await fx.RangeAsync("/kafkaworker/instances/", ct)).Count;
                    return $"state={configState}, work={Trunc(work ?? "нет")}, api={api}, instances={instances}";
                });
            finished.Should().BeTrue(
                $"выживший обязан доиграть provisioning до done; work={await fx.GetOrNullAsync($"/kafkaworker/work/{cluster}", ct)}");

            // I2: операция доиграна ВЫЖИВШИМ до терминальной фазы: config без
            // state, endpoints записан, brokers RUNNING, work.instance=survivor.
            var configAfter = await fx.GetOrNullAsync($"/kafka/clusters/{cluster}/config", ct);
            configAfter.Should().NotBeNull("config кластера существует после provisioning");
            configAfter.Should().NotContain("state", "K5 CommitConfig убирает state из config (I2)");
            (await fx.GetOrNullAsync($"/kafka/clusters/{cluster}/endpoints", ct))
                .Should().NotBeNullOrEmpty("endpoints записаны K5 (I2)");
            for (var k = 1; k <= 3; k++)
                (await fx.GetOrNullAsync($"/kafka/clusters/{cluster}/brokers/broker{k}/state", ct))
                    .Should().Be("RUNNING", $"брокер{k} RUNNING после доигрывания (I2)");

            // I1: takeover клэйма — ключи мертвеца погасли, жив ровно один
            // api-ключ и один instance-ключ; клэйм держит survivor.
            var apiKeys = await fx.RangeAsync("/kafkaworker/api/", ct);
            apiKeys.Should().HaveCount(1, "после kill держателя жив ровно один api-ключ (lease погас)");
            (await fx.RangeAsync("/kafkaworker/instances/", ct))
                .Should().HaveCount(1, "instance-ключ держателя погас вместе с lease");
            var survivorId = apiKeys[0].Key.Split('/')[^1];

            var claimAfter = await fx.GetOrNullAsync($"/kafkaworker/claims/{cluster}", ct);
            claimAfter.Should().NotBeNull("клэйм живого кластера обязан существовать после takeover");
            using (var doc = JsonDocument.Parse(claimAfter!))
                doc.RootElement.GetProperty("instance").GetString().Should().Be(survivorId,
                    "клэйм кластера держит выживший инстанс — надзор мигрировал (I1)");

            var workJson = await fx.GetOrNullAsync($"/kafkaworker/work/{cluster}", ct);
            using (var doc = JsonDocument.Parse(workJson!))
            {
                doc.RootElement.GetProperty("op").GetString().Should().Be("provision");
                doc.RootElement.GetProperty("phase").GetString().Should().Be("done");
                doc.RootElement.GetProperty("instance").GetString().Should().Be(survivorId,
                    "фазы доигрывает instance-ид из клэйма — выживший (I2)");
            }

            // I3: нет дублей — контейнеров брокеров ровно 3 (вкл. stopped).
            var brokers = await fx.ListContainerNamesAsync($"kfw-{cluster}-broker", all: true);
            brokers.Should().HaveCount(3, "контейнеров kfw-{C}-broker* ровно 3 — дублей после takeover нет (I3)");

            // I4: контроль-плейн цел — portalloc жив и содержит broker1..3.
            var portalloc = await fx.GetOrNullAsync($"/kafkaworker/portalloc/{cluster}", ct);
            portalloc.Should().NotBeNull("portalloc кластера цел после takeover (I4)");
            portalloc.Should().Contain("broker1").And.Contain("broker2").And.Contain("broker3",
                "portalloc закрепляет все 3 брокера (I4)");

            // I5: датаплейн жив — admin-клиент из etcd-дискавери (паттерн
            // KafkaClusterFixture.DiscoveryAdminBuilderAsync: endpoints,
            // admin_user/admin_password, ca_pem → ssl.ca.pem, SASL_SSL + Plain)
            // видит 3 брокеров. Bootstrap — БЕЗ localhost-замены: SAN сертов
            // брокеров = advertised-хост (host.docker.internal) — подключение
            // по «localhost» даёт hostname-mismatch (SSL handshake failed на
            // стороне брокера); с хоста macOS host.docker.internal резолвится
            // в 127.0.0.1 — published порты досягаемы, SAN совпадает.
            var endpoints = await fx.GetOrNullAsync($"/kafka/clusters/{cluster}/endpoints", ct);
            var adminUser = await fx.GetOrNullAsync($"/kafka/clusters/{cluster}/admin_user", ct);
            var adminPassword = await fx.GetOrNullAsync($"/kafka/clusters/{cluster}/admin_password", ct);
            var caPem = await fx.GetOrNullAsync($"/kafka/clusters/{cluster}/ca_pem", ct);
            var adminConfig = new AdminClientConfig
            {
                BootstrapServers = endpoints,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = adminUser,
                SaslPassword = adminPassword,
            };
            adminConfig.Set("ssl.ca.pem", caPem!);
            using (var admin = new AdminClientBuilder(adminConfig).Build())
            {
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(15));
                metadata.Brokers.Should().HaveCount(3, "датаплейн жив: admin-клиент из дискавери видит 3 брокеров (I5)");
            }

            // A6: финальный демонтаж ВЫЖИВШИМ: config.state=TO_REMOVE (стиль
            // панели — перезапись config с state) → DeprovisioningProcess: ни
            // контейнеров kfw-{C}-*, ни portalloc; сеть kfw-net-{C} удалена
            // движком. Домен Kafka серты брокерам передаёт env при создании —
            // томов/helper-хвостов, держащих объекты после kill воркера, модель
            // не порождает: демонтаж выжившим укладывается в бюджет 60 с.
            var rawConfig = await fx.GetOrNullAsync($"/kafka/clusters/{cluster}/config", ct);
            await fx.PutAsync($"/kafka/clusters/{cluster}/config",
                rawConfig!.Replace("}", ",\"state\":\"TO_REMOVE\"}", StringComparison.Ordinal), ct);

            var cleaned = await KafkaE2ePhase.WaitAsync(fx, "ktake-wait-clean", async () =>
            {
                var left = await fx.ListContainerNamesAsync($"kfw-{cluster}-", all: true);
                if (left.Count > 0)
                    return false;
                if (await fx.GetOrNullAsync($"/kafkaworker/portalloc/{cluster}", ct) is not null)
                    return false;
                var networks = await fx.RunDockerAsync(
                    ["network", "ls", "--format", "{{.Name}}", "--filter", $"name=kfw-net-{cluster}"], ct);
                return networks.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    .Where(n => n.StartsWith($"kfw-net-{cluster}", StringComparison.Ordinal))
                    .ToList().Count == 0;
            }, TimeSpan.FromSeconds(60), ct,
                progress: async () =>
                {
                    var work = await fx.GetOrNullAsync($"/kafkaworker/work/{cluster}", ct);
                    var alloc = await fx.GetOrNullAsync($"/kafkaworker/portalloc/{cluster}", ct) is not null;
                    var domain = (await fx.RangeAsync($"/kafka/clusters/{cluster}/", ct)).Count;
                    return $"work={Trunc(work ?? "нет")}, portalloc={alloc}, domain-ключи={domain}";
                });
            cleaned.Should().BeTrue("выживший обязан штатно демонтировать кластер за 60 с (I6)");

            // A7: teardown + ассерт чистоты — в DisposeAsync окружения (I7);
            // survivor-name здесь только для читаемости журнала.
            Console.WriteLine($"[PHASE] ktake-done: holder={holderName} убит, survivor={survivorName} доиграл");
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — teardown остановит
            // контейнеры, но не удалит (разбор по артефактам /tmp/pgw-e2e-artifacts-*).
            fx.MarkFailed();
            throw;
        }
    }

    // ===== Локальные хелперы сценария =====

    // Обрезка длинного work-JSON до ~120 симв., чтобы прогресс-тик читался.
    private static string Trunc(string value)
        => value.Length <= 120 ? value : value[..120] + "…";

    // Дамп api-ключей для диагностики ассерта резолва держателя (Р3): полный
    // список instance→url, чтобы FAIL был доказательным.
    private static async Task<string> DumpApiKeysAsync(KafkaE2eEnvironment fx, CancellationToken ct)
    {
        var keys = await fx.RangeAsync("/kafkaworker/api/", ct);
        return string.Join("; ", keys.Select(k => $"{k.Key}={k.Value}"));
    }
}
