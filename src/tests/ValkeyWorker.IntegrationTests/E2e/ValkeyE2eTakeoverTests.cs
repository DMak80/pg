using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using ValkeyWorker.IntegrationTests.Valkey;
using Xunit;

namespace ValkeyWorker.IntegrationTests.E2e;

// Takeover-E2E ValkeyWorker (spec t20 §8): kill ДЕРЖАТЕЛЯ клэйма ДО начала
// X0 ДЕМОНТАЖА кластера (X0–X3, arch/21 §5 B) — выживший инстанс пере-захватывает
// клэйм (TTL ≤15 с) и доигрывает демонтаж до терминальной чистоты: пустой
// домен + пустая координация + нет контейнеров, без дублей (инварианты I1–I7).
// Модель env-TLS: серты нод — VALKEY_TLS_* в env контейнера + cmd-обёртка;
// TLS-тома у домена нет. У демонтажа НЕТ journal-записи done (arch/21 §5 B:
// запись после чистки воскресила бы удалённый work) — терминальные факты:
// пустой домен + пустая координация + нет контейнеров.
// I2 — ПО ПОСТРОЕНИЮ + документальный лог (решение пользователя 2026-10-09,
// spec §8 A5/§15-Р2): journal-полл «work op=deprovision instance=survivor»
// НЕПРИМЕНИМ — survivor после re-захвата проходит X0→X2 одним тиком <0.5 с,
// короче полла 500 мс (доказано прогоном e055e1e0: 120 поллов за 60 с ни разу
// не видели work.instance=survivor; по логу выжившего txn-захват клэйма →
// X0 → X1 rm → 8×X2 del → X3, все etcd-опы 0.8–3.6 мс). Цепь доказательства:
// kill ДО X0 жертвы (пре-факт A3/A4: work отсутствует ИЛИ op != deprovision
// на момент kill) + kill без рестарта + единственность survivor (I1) +
// терминальная чистота + подстрока «deprovision {C}» в логах survivor.
// Держатель резолвится по etcd-фактам ПЕРЕД kill (claims.instance → api-ключ
// → url → порт → сопоставление с Api1/Api2Port) — kill доказательно попадает
// в держателя. Изоляция/телеметрия — docs/e2e-isolation.md +
// docs/e2e-launch.md; ожидения — только ValkeyE2ePhase.WaitAsync (docker-CLI
// в тиках запрещён). Гейт: PGW_TEST_DOCKER=1.
public class ValkeyE2eTakeoverTests
{
    [Fact]
    public async Task Kill_ClaimHolderMidDeprovision_SurvivorFinishesNoDuplicates()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        var enabled = Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") == "1";
        if (!enabled)
        {
            Assert.Skip("PGW_TEST_DOCKER=1 не задан — docker-E2E пропущен");
            return;
        }

        // Arrange A1: двухинстансный контур (сеть + etcd + ew1/ew2 + общий TLS).
        await using var fx = await ValkeyE2eEnvironment.StartTwoAsync("vwk-takeover");
        var cluster = $"tvw{fx.ClusterTag}";
        using var api1 = fx.CreateApiHttpClient(fx.Api1BaseUrl);
        var ct = TestContext.Current.CancellationToken;

        try
        {
            // A1: готовность ОБОИХ воркеров — по 2 lease-ключа api/instances
            // (бюджет ≤ 100 с; ключ жив = процесс поднялся, keepalive тикает).
            var bothUp = await ValkeyE2ePhase.WaitAsync(fx, "takeover-instances-up", async () =>
                (await RangeAsync(fx, "/valkeyworker/api/", ct)).Count == 2
                && (await RangeAsync(fx, "/valkeyworker/instances/", ct)).Count == 2,
                TimeSpan.FromSeconds(100), ct,
                progress: async () =>
                {
                    var api = (await RangeAsync(fx, "/valkeyworker/api/", ct)).Count;
                    var instances = (await RangeAsync(fx, "/valkeyworker/instances/", ct)).Count;
                    return $"api={api}, instances={instances}";
                });
            bothUp.Should().BeTrue("оба инстанса обязаны опубликовать дискавери-ключи за 100 с");

            // A2: живой кластер через API ew1 (нода поднята с VALKEY_TLS_* в env
            // и cmd-обёрткой, без TLS-тома — env-модель).
            var workerUp = await ValkeyE2ePhase.WaitAsync(fx, "takeover-wait-worker", async () =>
            {
                try
                {
                    using var health = await api1.GetAsync("/healthz", ct);
                    return health.StatusCode == HttpStatusCode.OK;
                }
                catch (HttpRequestException)
                {
                    return false; // Kestrel ещё поднимается
                }
            }, TimeSpan.FromSeconds(30), ct);
            workerUp.Should().BeTrue("API ew1 (/healthz по mTLS) обязан подняться за 30 с");

            using var created = await api1.PostAsJsonAsync("/api/valkey/clusters",
                new
                {
                    name = cluster,
                    nodes = 1,
                    maxmemoryBytes = 536870912,
                    maxmemoryPolicy = "allkeys-lru",
                    resources = new { cpu = 1m, memGi = 1, diskGi = 10 },
                }, ct);
            var createdBody = await created.Content.ReadAsStringAsync(ct);
            created.StatusCode.Should().Be(HttpStatusCode.Created, createdBody);

            var provisioned = await ValkeyE2ePhase.WaitAsync(fx, "takeover-wait-provision", async () =>
            {
                var kv = await GetOrNullAsync(fx, $"/valkey/clusters/{cluster}/nodes/node1/state", ct);
                return kv == "RUNNING" && await fx.ContainerAliveAsync($"vwk-{cluster}-node1");
            }, TimeSpan.FromSeconds(100), ct,
                progress: async () =>
                    $"state={await GetOrNullAsync(fx, $"/valkey/clusters/{cluster}/nodes/node1/state", ct) ?? "нет"}");
            provisioned.Should().BeTrue("provisioning кластера обязан дойти до RUNNING за 100 с");

            // Baseline датаплейна ДО заявки демонтажа (ключи домена уйдут в X2):
            // endpoints + ca_pem + admin-кред из etcd.
            var caPem = (await GetOrNullAsync(fx, $"/valkey/clusters/{cluster}/ca_pem", ct))!;
            var endpoints = (await GetOrNullAsync(fx, $"/valkey/clusters/{cluster}/endpoints", ct))!;
            var probeHost = endpoints.Split(':')[0];
            var probePort = int.Parse(endpoints.Split(':')[1]);
            var adminPassword = (await GetOrNullAsync(fx, $"/valkey/clusters/{cluster}/admin_password", ct))!;

            // Доп-проверка env-модели (docker inspect, вне тиков): env контейнера
            // ноды несёт VALKEY_TLS_{CERT,KEY,CA}; CA == per-cluster ca_pem.
            var nodeEnv = await GetContainerEnvAsync($"vwk-{cluster}-node1");
            nodeEnv.Should().ContainKey("VALKEY_TLS_CERT", "серт ноды доставляется env контейнера (env-модель)");
            nodeEnv.Should().ContainKey("VALKEY_TLS_KEY", "ключ ноды доставляется env контейнера (env-модель)");
            nodeEnv.Should().ContainKey("VALKEY_TLS_CA", "CA ноды доставляется env контейнера (env-модель)");
            nodeEnv["VALKEY_TLS_CA"].Trim().Should().Be(caPem.Trim(),
                "CA в env ноды == per-cluster ca_pem (переносы — в значении env, Trim-сравнение)");

            // Датаплейн жив до kill (I5-часть): RESP PING admin-кредом по TLS.
            var ping = RespProbe.ExecuteTls(probeHost, probePort, "admin", adminPassword, caPem, "PING");
            ping.Ok.Should().BeTrue(ping.Error);
            ping.Value.Should().Be("PONG", "датаплейн кластера отвечает по TLS до kill (I5-часть)");

            // A3 (Act): заявка демонтажа ЛЮБОМУ из ew1/ew2 (демонтаж ведёт
            // держатель клэйма — адресат заявки не важен) → 202 = config.state
            // TO_REMOVE поставлен синхронно; исполняет процесс B тиком держателя.
            using var deleted = await api1.DeleteAsync($"/api/valkey/clusters/{cluster}", ct);
            var deletedBody = await deleted.Content.ReadAsStringAsync(ct);
            deleted.StatusCode.Should().Be(HttpStatusCode.Accepted, deletedBody);

            // A3: якорь «посередине» демонтажа — ВСЕ условия (а)–(г)
            // одновременно: kill гарантированно внутри демонтажа ДО X0 жертвы
            // («мимо» — демонтаж уже завершён — исключено условием (а):
            // config с TO_REMOVE живёт от DELETE-202 до X2). Покрытие окна
            // непрерывно: до X0 — config+клэйм+контейнер; X0..X1 — +work;
            // X1..X2 — config+клэйм+work; пре-факт (г) сужает окно до
            // «до X0»: контейнер ещё существует, work демонтажа ещё не ставился.
            var midDeprovision = await ValkeyE2ePhase.WaitAsync(fx, "takeover-mid-deprovision", async () =>
            {
                // (а) config существует и state=TO_REMOVE (живёт почти весь демонтаж).
                var config = await GetOrNullAsync(fx, $"/valkey/clusters/{cluster}/config", ct);
                if (config is null || JsonStringField(config, "state") != "TO_REMOVE")
                    return false;
                // (б) клэйм кластера существует (держатель активен).
                if (await GetOrNullAsync(fx, $"/valkeyworker/claims/{cluster}", ct) is null)
                    return false;
                // (в) демонтаж в полёте: контейнер ноды ещё существует (docker ps -a
                // — в теле условия; docker-CLI в тиках запрещён) ИЛИ work уже
                // ставился X0 (ставится в X0, живёт до X2).
                var containerExists = (await ListContainersAsync($"vwk-{cluster}-node1", all: true)).Count > 0;
                var work = await GetOrNullAsync(fx, $"/valkeyworker/work/{cluster}", ct);
                var x0Started = work is not null && JsonStringField(work, "op") == "deprovision";
                // (г) пре-факт I2: X0 жертвы НЕ начат — work отсутствует ИЛИ его
                // op != deprovision (хвост прошлого цикла op=provision/phase=done
                // демонтажом не является). Совместно с (в): контейнер существует
                // И X0 никем ещё не ставился.
                return (containerExists || x0Started) && !x0Started;
            }, TimeSpan.FromSeconds(30), ct,
                progress: async () =>
                {
                    var config = await GetOrNullAsync(fx, $"/valkey/clusters/{cluster}/config", ct);
                    var claim = await GetOrNullAsync(fx, $"/valkeyworker/claims/{cluster}", ct) is not null;
                    var work = await GetOrNullAsync(fx, $"/valkeyworker/work/{cluster}", ct);
                    var workView = work is null
                        ? "нет"
                        : $"op={JsonStringField(work, "op")}/instance={JsonStringField(work, "instance")}";
                    return $"state={JsonStringField(config, "state") ?? "нет"}, claims={claim}, work={workView}";
                });
            midDeprovision.Should().BeTrue(
                "якорь обязан поймать окно демонтажа ДО X0 жертвы (config TO_REMOVE + клэйм + контейнер + work без op=deprovision) за 30 с — "
                + "иначе окно kill стабильно недостаточно (СТОП по spec §15-Р2)");

            // A4 (Act): резолв ДЕРЖАТЕЛЯ по etcd-фактам и kill (одномоментно).
            var claimJson = await GetOrNullAsync(fx, $"/valkeyworker/claims/{cluster}", ct);
            claimJson.Should().NotBeNull("клэйм существует непосредственно перед kill (ассерт A4)");
            var holderInstance = JsonStringField(claimJson, "instance");
            holderInstance.Should().NotBeNullOrEmpty("claims/{C}.instance обязан быть непустым");

            var holderUrl = await GetOrNullAsync(fx, $"/valkeyworker/api/{holderInstance}", ct);
            holderUrl.Should().NotBeNull($"api-ключ держателя {holderInstance} обязан существовать (общий lease с instance-ключом)");
            var holderPort = new Uri(JsonStringField(holderUrl, "url")!).Port;

            var matched = new[] { fx.Api1Port, fx.Api2Port }.Count(p => p == holderPort);
            matched.Should().Be(1, $"порт держателя {holderPort} обязан совпасть ровно с одним известным api-портом "
                + $"({fx.Api1Port}/{fx.Api2Port}); claims={claimJson}, api-ключи={await DumpApiKeysAsync(fx, ct)}");

            var holderName = holderPort == fx.Api1Port ? fx.Worker1Name : fx.Worker2Name;
            var survivorName = holderPort == fx.Api1Port ? fx.Worker2Name : fx.Worker1Name;
            var survivorPort = holderPort == fx.Api1Port ? fx.Api2Port : fx.Api1Port;

            // Пре-факт I2 НА МОМЕНТ kill (спека §8 A5-1 / план Э6 A3-г): X0
            // жертвы НЕ начат — work/{C} отсутствует ИЛИ op != deprovision
            // (жертва убита до своей journal-записи). Провал = kill попал
            // ПОСЛЕ X0 жертвы → конструктивная цепь I2 недействительна — фейл
            // с дампом claims/work/config (разбор по артефактам, не ретрай).
            var workAtKill = await GetOrNullAsync(fx, $"/valkeyworker/work/{cluster}", ct);
            var claimAtKill = await GetOrNullAsync(fx, $"/valkeyworker/claims/{cluster}", ct);
            var configAtKill = await GetOrNullAsync(fx, $"/valkey/clusters/{cluster}/config", ct);
            (workAtKill is null || JsonStringField(workAtKill, "op") != "deprovision").Should().BeTrue(
                "пре-факт I2: X0 жертвы НЕ начат на момент kill — work отсутствует или op != deprovision; "
                + $"claims={Trunc(claimAtKill ?? "нет")}, work={Trunc(workAtKill ?? "нет")}, "
                + $"config={Trunc(configAtKill ?? "нет")}");

            // docker kill держателя (без рестарта — воскресать не должен,
            // доносит ТОЛЬКО выживший).
            await RunDockerAsync($"kill {holderName}");

            // Идентификатор survivor-инстанса: api-ключ с url-портом выжившего
            // (его дискавери живёт с A1 — независимо от kill).
            string? survivorId = null;
            foreach (var kv in await RangeAsync(fx, "/valkeyworker/api/", ct))
            {
                var url = JsonStringField(kv.Value, "url");
                if (url is not null && new Uri(url).Port == survivorPort)
                    survivorId = kv.Key.Split('/')[^1];
            }
            survivorId.Should().NotBeNullOrEmpty(
                $"api-ключ survivor'а (порт {survivorPort}) обязан существовать; api-ключи={await DumpApiKeysAsync(fx, ct)}");

            // A5 (Assert) — I2 ПО ПОСТРОЕНИЮ + документальный лог (спека §8 A5,
            // решение пользователя 2026-10-09): journal-полл «дождаться work
            // op=deprovision instance=survivor» НЕПРИМЕНИМ — survivor после
            // re-захвата клэйма проходит X0→X2 одним тиком <0.5 с, короче
            // полла 500 мс (доказано прогоном e055e1e0: 120 поллов за 60 с ни
            // разу не видели work.instance=survivor; по логу выжившего все
            // etcd-опы X-фаз 0.8–3.6 мс). Инвариант I2 доказывается цепью:
            // пре-факт «kill до X0 жертвы» (A3/A4) + kill без рестарта +
            // единственность survivor (I1 ниже) + терминальная чистота +
            // подстрока «deprovision {C}» в логах survivor (документальный
            // факт после терминала).

            // I1: takeover дискавери — ключи мертвеца гаснут с его lease ≤15 с
            // (ассерт С БЮДЖЕТОМ ≤20 с, НЕ мгновенный), жив ровно один api-ключ
            // и один instance-ключ — survivor. К концу фазы клэйм пере-захвачен
            // survivor'ом (lease жертвы истёк в этом же окне) — доигрывание
            // демонтажа стартует сразу за ним.
            var settled = await ValkeyE2ePhase.WaitAsync(fx, "takeover-discovery-settled", async () =>
                (await RangeAsync(fx, "/valkeyworker/api/", ct)).Count == 1
                && (await RangeAsync(fx, "/valkeyworker/instances/", ct)).Count == 1,
                TimeSpan.FromSeconds(20), ct,
                progress: async () =>
                {
                    var api = (await RangeAsync(fx, "/valkeyworker/api/", ct)).Count;
                    var instances = (await RangeAsync(fx, "/valkeyworker/instances/", ct)).Count;
                    return $"api={api}, instances={instances}";
                });
            settled.Should().BeTrue("дискавери мертвеца гаснет с его lease ≤15 с — бюджет 20 с (I1)");
            var apiKeys = await RangeAsync(fx, "/valkeyworker/api/", ct);
            apiKeys.Should().ContainSingle("после kill держателя жив ровно один api-ключ (I1)");
            apiKeys[0].Key.Split('/')[^1].Should().Be(survivorId,
                "живой api-ключ — survivor по факту резолва A4 (I1)");
            (await RangeAsync(fx, "/valkeyworker/instances/", ct))
                .Should().ContainSingle("instance-ключ держателя погас вместе с lease (I1)");

            // A5: терминал — финиш демонтажа выжившим. Journal-записи done у
            // демонтажа НЕТ (arch/21 §5 B) — терминальные факты: контейнеров
            // node1 ровно 0 (вкл. stopped) + пустой домен + пустая координация.
            // Wait-clean ≤15 с от re-захвата — целевой бюджет демонтажа
            // (survivor проходит X0→X2 одним тиком; превышение без объяснения
            // по логам — фейл фазы, разбор по телеметрии).
            var cleaned = await ValkeyE2ePhase.WaitAsync(fx, "takeover-wait-clean", async () =>
                (await ListContainersAsync($"vwk-{cluster}-node1", all: true)).Count == 0
                && (await RangeAsync(fx, $"/valkey/clusters/{cluster}/", ct)).Count == 0
                && await CoordinationGoneAsync(fx, cluster, ct),
                TimeSpan.FromSeconds(15), ct,
                progress: async () =>
                {
                    var domain = (await RangeAsync(fx, $"/valkey/clusters/{cluster}/", ct)).Count;
                    var work = await GetOrNullAsync(fx, $"/valkeyworker/work/{cluster}", ct) is not null;
                    var claim = await GetOrNullAsync(fx, $"/valkeyworker/claims/{cluster}", ct) is not null;
                    var alloc = await GetOrNullAsync(fx, $"/valkeyworker/portalloc/{cluster}", ct) is not null;
                    return $"domain-ключи={domain}, work={work}, claims={claim}, portalloc={alloc}";
                });
            cleaned.Should().BeTrue(
                "выживший обязан доиграть демонтаж до терминальной чистоты ≤15 с от re-захвата (I6; done-записи у демонтажа нет — терминал = чистота)");

            // Документальный лог-факт I2 (docker-CLI вне тиков; ОДНОКРАТНО
            // после терминала, НЕ полл): «deprovision {C}» в логах survivor —
            // жертва убита до X0 (пре-факт A3/A4) и без рестарта, имя кластера
            // guid-уникально → строку мог записать только survivor, исполнивший
            // демонтаж (logger ValkeyClusterProcesses «deprovision {C}: ok»;
            // обработчик DELETE такую строку не пишет — подтверждено grep'ом
            // логов прогона e055e1e0: у жертвы 0 вхождений).
            var survivorLog = await RunDockerAsync($"logs {survivorName}");
            survivorLog.Should().Contain($"deprovision {cluster}",
                "документальный факт I2: демонтаж кластера исполнил survivor — жертва убита до X0 и не рестартована");

            // I3: нет дублей и остатков — контейнеров префикса кластера ровно 0
            // (вкл. stopped; docker-проверки вне тиков), томов префикса нет —
            // env-модель, у домена нет docker-объектов кроме контейнеров нод.
            (await ListContainersAsync($"vwk-{cluster}", all: true)).Should().BeEmpty(
                "контейнеров префикса кластера нет — дублей/остатков после takeover нет (I3)");
            (await RunDockerAsync($"volume ls -q --filter name=vwk-{cluster}")).Should().BeEmpty(
                "томов префикса кластера нет — env-модель их не создаёт (I3)");

            // I4: контроль-плейн цел — portalloc снят демонтажом X2 (входит в
            // терминальную чистоту; продублирован явным ассертом для отчёта).
            (await GetOrNullAsync(fx, $"/valkeyworker/portalloc/{cluster}", ct))
                .Should().BeNull("portalloc кластера снят демонтажом выжившего (I4)");

            // A7: teardown + ассерт чистоты — в DisposeAsync окружения (I7);
            // survivor-name здесь только для читаемости журнала. Отдельного
            // финального демонтажа НЕТ — жертва и есть демонтаж.
            Console.WriteLine($"[PHASE] takeover-done: holder={holderName} убит, survivor={survivorName} доиграл демонтаж");
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — teardown остановит
            // контейнеры, но не удалит (разбор по артефактам pgw-e2e-artifacts-*).
            fx.MarkFailed();
            throw;
        }
    }

    // ===== Локальные хелперы сценария =====

    // Обрезка длинного JSON до ~120 симв., чтобы прогресс-тик читался.
    private static string Trunc(string value)
        => value.Length <= 120 ? value : value[..120] + "…";

    // Строковое JSON-поле или null (нет ключа/битый JSON/нет значения —
    // условие фазы корректно не срабатывает и ждёт дальше).
    private static string? JsonStringField(string? json, string field)
    {
        if (json is null)
            return null;
        try
        {
            return JsonDocument.Parse(json).RootElement.GetProperty(field).GetString();
        }
        catch (KeyNotFoundException)
        {
            return null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static async Task<string?> GetOrNullAsync(
        ValkeyE2eEnvironment fx, string key, CancellationToken ct)
        => (await fx.Gateway.GetAsync(fx.EtcdEndpoint, key, ct)).Value?.Value;

    private static async Task<IReadOnlyList<Shared.Etcd.Client.Kv>> RangeAsync(
        ValkeyE2eEnvironment fx, string prefix, CancellationToken ct)
        => (await fx.Gateway.RangeAsync(fx.EtcdEndpoint, prefix, ct)).Value;

    // Дамп api-ключей для диагностики ассерта резолва держателя (Р3): полный
    // список instance→url, чтобы FAIL был доказательным.
    private static async Task<string> DumpApiKeysAsync(ValkeyE2eEnvironment fx, CancellationToken ct)
    {
        var keys = await RangeAsync(fx, "/valkeyworker/api/", ct);
        return string.Join("; ", keys.Select(k => $"{k.Key}={k.Value}"));
    }

    // Координация кластера пуста (arch/21 §5 B, X2): claims, work,
    // work/rotation, portalloc, rotations, ca_rotations, ticket_outcomes.
    private static async Task<bool> CoordinationGoneAsync(
        ValkeyE2eEnvironment fx, string cluster, CancellationToken ct)
    {
        foreach (var key in new[]
                 {
                     $"/valkeyworker/claims/{cluster}",
                     $"/valkeyworker/work/{cluster}",
                     $"/valkeyworker/work/{cluster}/rotation",
                     $"/valkeyworker/portalloc/{cluster}",
                     $"/valkeyworker/rotations/{cluster}",
                     $"/valkeyworker/ca_rotations/{cluster}",
                     $"/valkeyworker/ticket_outcomes/{cluster}",
                 })
            if (await GetOrNullAsync(fx, key, ct) is not null)
                return false;
        return true;
    }

    // Env контейнера как словарь (json-формат docker inspect: переносы PEM
    // экранированы JSON, после парса — реальные переносы в значении).
    private static async Task<IReadOnlyDictionary<string, string>> GetContainerEnvAsync(string name)
    {
        var raw = await RunDockerAsync(
            $"inspect {name} --format \"{{{{json .Config.Env}}}}\"");
        var env = new Dictionary<string, string>();
        foreach (var item in JsonDocument.Parse(raw).RootElement.EnumerateArray())
        {
            var line = item.GetString()!;
            var eq = line.IndexOf('=');
            if (eq > 0)
                env[line[..eq]] = line[(eq + 1)..];
        }
        return env;
    }

    // docker-CLI сценария (kill/ps/inspect): вывод — в исключение.
    private static async Task<string> RunDockerAsync(string args)
    {
        var startInfo = new System.Diagnostics.ProcessStartInfo("docker", args)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = System.Diagnostics.Process.Start(startInfo)!;
        var output = await process.StandardOutput.ReadToEndAsync();
        var error = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        if (process.ExitCode != 0)
            throw new ApplicationException($"docker {args} → {process.ExitCode}: {error.Trim()}");
        return output.Trim();
    }

    // Имена контейнеров по префиксу (вкл. stopped при all=true) — I3: без дублей.
    private static async Task<List<string>> ListContainersAsync(string prefix, bool all = false)
    {
        var args = $"ps --format {{{{.Names}}}}{(all ? " -a" : "")} --filter name={prefix}";
        var output = await RunDockerAsync(args);
        return output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
    }
}
