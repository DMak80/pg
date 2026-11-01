using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Npgsql;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E ротации per-cluster секретов (spec §7.5): заявка etcdctl-формой →
// заявка исчезла, app/mover/bucket_admin изменились, dsn перезаписан, новый
// пароль подключается, старый отвергается, пишущая нагрузка переживает ротацию.
public sealed class E2eRotateScenarios(E2eRotateScenariosFx Ctx) : IClassFixture<E2eRotateScenariosFx>
{
    private E2eEnvironment Fx => Ctx.Fx;

    // Уникальное имя кластера на прогон ({slug}{тег прогона}, docs/e2e-isolation.md
    // §1): движковые контейнеры/тома pgw-<C>-* опознаются teardown'ом окружения
    // по своему тегу (OwnName) и снимаются им; константное имя запрещено.
    private string Cluster => $"rotate{Fx.ClusterTag}";


    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // Задача 2 (детерминированная диагностика): OLD rest-пароль — ФИКСИРОВАННАЯ
    // константа, сидится в etcd ДО provisioning (ensure кладёт ключ
    // put-if-absent — сид не переписывает). NEW-пароль константой быть НЕ МОЖЕТ:
    // механизм заявки ротации (панель §9.8: {"requested_unix","requested_by"})
    // явный rest-пароль НЕ принимает — NEW генерируется ротатором (R2) и
    // читается из etcd после коммита окна. Все сверки — с константой OLD и
    // прочитанным NEW; каждая проба пишет в лог «какой пароль послан, какой
    // код ожидаем» — по любому логу видно, каким паролем ходили.
    private const string OldRestPasswordConstant = "RotateOldRestE2EConstant00000001";

    [Fact]
    public async Task Rotate_TicketRotatesPerClusterSecretsOnAllShards()
    {
        // Arrange — рабочий кластер (provisioning завершён), известны старые креды
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMinutes(3)); // потолок теста 3 мин: неверное ожидание не может длиться дольше (приказ владельца)
        var ct = cts.Token;
        await SeedClusterAsync(Cluster);
        await using var app = await Fx.StartHostAsync("rotate", ct: ct);
        var provisioned = await E2ePhase.WaitAsync(Fx, "rotate-provisioned",
            async () =>
                await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn") is not null
                && await GetOrNullAsync($"/clusters/{Cluster}/shards/shard2/dsn") is not null,
            TimeSpan.FromSeconds(90), ct);
        provisioned.Should().BeTrue("the cluster came up");
        var oldPassword = await Fx.GetAppPasswordAsync(Cluster, ct);
        var oldMover = (await GetOrNullAsync($"/clusters/{Cluster}/mover_password"))!.Value;
        var oldAdmin = (await GetOrNullAsync($"/clusters/{Cluster}/bucket_admin_password"))!.Value;
        // OLD rest-пароль — константа теста (сид); факт в etcd сверяется.
        var oldRest = OldRestPasswordConstant;
        (await GetOrNullAsync($"/clusters/{Cluster}/rest_password"))!.Value
            .Should().Be(OldRestPasswordConstant,
                "the seeded OLD rest constant survived provisioning (ensure is put-if-absent)");
        Console.WriteLine(
            $"[REST-DIAG] OLD rest password is the test constant {OldRestPasswordConstant}; " +
            "NEW will be read from etcd after the window opens (the ticket format does not carry it)");
        // Т3 (усиление): постмастер-снимок ДО ротации (после provisioned) —
        // сверка неизменности start_time после reload-применения (П11.9).
        var postmasterBefore = await PostmasterStartTimesAsync(Cluster, ct);

        // Адреса shard1 из dsn (multi-host, пары host:port) + мульти-хост строка
        // (Npgsql принимает Target Session Attributes только списком хостов).
        var dsn0 = (await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn"))!.Value;
        var hosts = Regex.Match(dsn0, "host=([^ ]+)").Groups[1].Value.Split(',');
        var ports = Regex.Match(dsn0, "port=([^ ]+)").Groups[1].Value.Split(',');
        var multiHost = string.Join(",", hosts.Zip(ports, (h, p) => $"{h}:{p}"));

        // Таблица writer-пробы: суперюзер (гранты provisioning покрывают только
        // существующие таблицы) + явный INSERT-грант роли DSN-точки входа.
        await using (var su = new NpgsqlConnection(
            $"Host={multiHost};Database={Cluster};Username=postgres;Password={E2eFixture.SuPassword};" +
            "Timeout=10;SSL Mode=Require;Trust Server Certificate=true;Target Session Attributes=read-write"))
        {
            await su.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(
                "CREATE TABLE IF NOT EXISTS bucket_0.e2e_rotate_writer(i int); " +
                "GRANT INSERT ON bucket_0.e2e_rotate_writer TO \"bucket_admin\"", su);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // StartedAt контейнеров нод ДО ротации (spec §6.3/§9.6: ротация
        // rest_password — БЕЗ пересоздания контейнеров).
        var startedAtBefore = await StartedAtMapAsync(Cluster, ct);

        // Act — заявка ротации (формат панели §9.8)
        await G.PutAsync(Endpoint, $"/pgworker/rotations/{Cluster}",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"requested_by":"e2e"}""",
            null, ct);

        // Assert 1 (критерий 5а/5б): заявка исполнена и удалена; пароль сменился
        var rotated = await E2ePhase.WaitAsync(Fx, "rotate-request-executed",
        async () =>
        {
            var password = await Fx.GetAppPasswordAsync(Cluster, ct);
            return password != oldPassword
                && await GetOrNullAsync($"/pgworker/rotations/{Cluster}") is null;
        }, TimeSpan.FromSeconds(90), ct,
            progress: async () =>
            {
                var work = await GetOrNullAsync($"/pgworker/work/{Cluster}");
                var nodes = await RangeOrNullAsync($"/pgworker/nodes/{Cluster}/");
                return $"work={work?.Value ?? "absent"}; statuses={nodes}";
            });
        rotated.Should().BeTrue("the request is executed: the password changed, the request key is removed");
        var newPassword = await Fx.GetAppPasswordAsync(Cluster, ct);
        var newRest = (await GetOrNullAsync($"/clusters/{Cluster}/rest_password"))!.Value;
        Regex.IsMatch(newPassword, "^[A-Za-z0-9]{32}$").Should().BeTrue();
        Console.WriteLine(
            $"[REST-DIAG] window opened: NEW rest password (from etcd, rotator-generated) length={newRest.Length}; " +
            $"OLD is the constant {OldRestPasswordConstant}; expectations: PATCH/config OLD 401→NEW 400 " +
            "(400 = auth ok on empty-patch body) per node as R3 applies");

        // Assert 1b : mover/bucket_admin тоже сменились (32 симв [A-Za-z0-9]);
        // dsn-ключи шардов перезаписаны новым bucket_admin-паролем.
        var newMover = (await GetOrNullAsync($"/clusters/{Cluster}/mover_password"))!.Value;
        var newAdmin = (await GetOrNullAsync($"/clusters/{Cluster}/bucket_admin_password"))!.Value;
        newMover.Should().NotBe(oldMover).And.MatchRegex("^[A-Za-z0-9]{32}$");
        newAdmin.Should().NotBe(oldAdmin).And.MatchRegex("^[A-Za-z0-9]{32}$");
        var newDsn = (await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn"))!.Value;
        newDsn.Should().Contain($"password={newAdmin}").And.NotContain(oldAdmin);

        // Т3 (MR-#9): StartedAt-снимок ПОСЛЕ закрытия окна (окно может жить
        // дольше заявки — ETCD-FIRST); StartedAtMapAsync выполняется только
        // после закрытия — «без пересоздания» проверено на полном окне R2→R4.
        var windowClosed = await E2ePhase.WaitAsync(Fx, "rotate-window-closed",
        async () =>
        {
            var work = await GetOrNullAsync($"/pgworker/work/{Cluster}");
            return work is not null && !work.Value.Contains("\"rest_window_open\":true");
        }, TimeSpan.FromSeconds(120), ct,
            progress: async () =>
            {
                var work = await GetOrNullAsync($"/pgworker/work/{Cluster}");
                var nodes = await RangeOrNullAsync($"/pgworker/nodes/{Cluster}/");
                var rotations = await GetOrNullAsync($"/pgworker/rotations/{Cluster}");
                // Задача 2: детерминированная картина окна — какой rest-пароль
                // КАЖДАЯ нода принимает прямо сейчас (мутирующий PATCH /config):
                // OLD — константа (ожидание 200, пока R3 не применил NEW, далее
                // 401), NEW — из etcd (ожидание 401 до применения, 200 после).
                var restAuth = await RestAuthProbeLineAsync(newRest, ct);
                return $"work={work?.Value ?? "absent"}; rotations={rotations?.Value ?? "absent"}; statuses={nodes}; rest-auth[{restAuth}]";
            });
        windowClosed.Should().BeTrue("R4 closes the rotation window");

        // Assert 1c (spec §6.3/§9.6): ротация rest_password БЕЗ пересоздания —
        // StartedAt каждого контейнера нод НЕ изменился.
        var startedAtAfter = await StartedAtMapAsync(Cluster, ct);
        startedAtAfter.Keys.Should().BeEquivalentTo(startedAtBefore.Keys,
            "the node container set did not change");
        foreach (var (name, startedAt) in startedAtAfter)
            startedAt.Should().Be(startedAtBefore[name],
                $"the container {name} was not recreated by the rotation");

        // Т3 (усиление, П11.9): применённость rest-пароля на Patroni —
        // мутирующий эндпоинт PATCH /config (GET / публичен и доказательством
        // аутентификации НЕ является): старая пара 401, новая 200; postmaster
        // PID/start_time каждой ноды НЕИЗМЕННЫ (применение reload'ом — PG не
        // останавливался).
        var addressesAfter = await PortallocAsync(Cluster);
        foreach (var (key, addr) in addressesAfter.OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            var patchNew = await PatchPatroniConfigAsync(addr.Patroni, newRest, ct);
            Console.WriteLine(
                $"[REST-DIAG] {key}: sent=NEW(etcd, len={newRest.Length}) expect=400 got={patchNew}");
            // 400 — аутентификация NEW ПРОЙДЕНА (тело {} — пустой патч, Patroni
            // 4.1.0 отвергает его как Bad request ПОСЛЕ auth; 401 = пара не
            // принята; 200 при {} невозможен — факт пробы t05probe3a).
            patchNew.Should().Be(400,
                $"the NEW rest password (etcd, rotator-generated) is accepted by patroni on {key} " +
                "(400 = auth ok, empty-patch body is rejected after authentication)");
            var patchOld = await PatchPatroniConfigAsync(addr.Patroni, oldRest, ct);
            Console.WriteLine(
                $"[REST-DIAG] {key}: sent=OLD(const {oldRest}) expect=401 got={patchOld}");
            patchOld.Should().Be(401,
                $"the OLD rest password (test constant) is rejected by patroni on {key}");
        }
        var postmasterAfter = await PostmasterStartTimesAsync(Cluster, ct);
        postmasterAfter.Keys.Should().BeEquivalentTo(postmasterBefore.Keys,
            "all nodes are probed after the rotation");
        foreach (var (key, startTime) in postmasterAfter)
            startTime.Should().Be(postmasterBefore[key],
                $"the postmaster of {key} was not restarted (reload does not touch PG)");

        // Assert 2b (критерий «без остановки записи»): writer — DSN-точка
        // входа bucket_admin — стартует ПОСЛЕ закрытия окна (R4: pg_authid
        // выровнен с etcd) и продолжает писать. Старт в ОТКРЫТОМ окне —
        // 28P01 транзиент (pg_authid OLD до R4, arch/14 §5 I); живой клиент
        // ретраит вставки до перечитывания — writer повторяет попытки тем же
        // бюджетом (поведение клиента по arch/14).
        // Окно уже закрыто (Т3-ожидание выше) — вставки writer'а проходят.
        var inserts = 0;
        string? lastError = null;
        var wrote = await E2eFixture.WaitForAsync(async () =>
        {
            var current = (await GetOrNullAsync($"/clusters/{Cluster}/bucket_admin_password"))!.Value;
            try
            {
                await using var con = new NpgsqlConnection(
                    $"Host={multiHost};Database={Cluster};Username=bucket_admin;" +
                    $"Password={current};Timeout=5;SSL Mode=Require;Trust Server Certificate=true;" +
                    "Target Session Attributes=read-write");
                await con.OpenAsync(ct);
                await using var cmd = new NpgsqlCommand(
                    "INSERT INTO bucket_0.e2e_rotate_writer VALUES (1)", con);
                await cmd.ExecuteNonQueryAsync(ct);
                inserts++;
            }
            catch (Exception e)
            {
                lastError = e.Message; // диагностика провала writer-проб
            }

            return inserts >= 5;
        }, TimeSpan.FromSeconds(90), ct);
        wrote.Should().BeTrue($"the write load continues after the rotation (inserts={inserts}, lastError={lastError})");

        // Assert 2 (критерий 5в/5г): новый app-пароль подключается, старый отвергается
        var newWorks = false;
        foreach (var (host, port) in hosts.Zip(ports))
            newWorks |= await E2eFixture.WaitForAsync(async () =>
            {
                try
                {
                    await using var con = new NpgsqlConnection(
                        $"Host={host};Port={port};Database={Cluster};Username=app;" +
                        $"Password={newPassword};Timeout=5;SSL Mode=Require;Trust Server Certificate=true");
                    await con.OpenAsync(ct);
                    await using var cmd = new NpgsqlCommand("SELECT 1", con);
                    return await cmd.ExecuteScalarAsync(ct) is 1;
                }
                catch (NpgsqlException)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(60), ct);
        newWorks.Should().BeTrue("the new password connects as user=app");

        var oldRejected = false;
        foreach (var (host, port) in hosts.Zip(ports))
            oldRejected |= await E2eFixture.WaitForAsync(async () =>
            {
                try
                {
                    await using var con = new NpgsqlConnection(
                        $"Host={host};Port={port};Database={Cluster};Username=app;" +
                        $"Password={oldPassword};Timeout=5;SSL Mode=Require;Trust Server Certificate=true");
                    await con.OpenAsync(ct);
                    return false; // старый пароль всё ещё работает — ждём отвержения
                }
                catch (NpgsqlException)
                {
                    return true; // отвергнут — ожидаемо
                }
            }, TimeSpan.FromSeconds(60), ct);
        oldRejected.Should().BeTrue("the old password is rejected (auth fail)");
    }

    // Сид кластера в стиле панели (копия E2eAppSecretScenarios.SeedClusterAsync).
    private async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var config = $$"""
            {"buckets":2,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}
            """;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", config, null, ct);
        // Задача 2: OLD rest-пароль — константа теста, сидится ДО provisioning
        // (ensure per-cluster кредов put-if-absent — сид не переписывает).
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/rest_password",
            OldRestPasswordConstant, null, ct);
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
            // Заявки ресурсов панель-создания (arch/14 §2.1 п.4): pgtune-фаза
            // provisioning требует их обязательно (f6d4574: дефолтов нет).
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "2Gi", null, ct);
        }

        for (var i = 0; i < 2; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    // portalloc кластера (адреса нод: pg/patroni/doorman — хост-публикации).
    private sealed record NodeAddr(string Host, int Pg, int Patroni, int Doorman);

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    private async Task<Dictionary<string, NodeAddr>> PortallocAsync(string cluster)
    {
        var kv = await GetOrNullAsync($"/pgworker/portalloc/{cluster}");
        if (kv is null)
            return [];
        return JsonSerializer.Deserialize<Dictionary<string, NodeAddr>>(kv.Value, Json) ?? [];
    }

    // Т3 (усиление): postmaster_start_time нод (GET / публичен — auth не нужен).
    private async Task<Dictionary<string, string>> PostmasterStartTimesAsync(
        string cluster, CancellationToken ct)
    {
        var addresses = await PortallocAsync(cluster);
        var map = new Dictionary<string, string>();
        foreach (var (key, addr) in addresses.OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            using var response = await Fx.NewPatroniHttpClient().GetAsync(
                $"https://localhost:{addr.Patroni}/", ct);
            var body = await response.Content.ReadAsStringAsync(ct);
            map[key] = JsonSerializer
                .Deserialize<Dictionary<string, JsonElement>>(body, Json)?
                .TryGetValue("postmaster_start_time", out var t) == true
                ? t.GetString() ?? ""
                : "";
        }
        return map;
    }

    // PATCH /config (мутирующий эндпоинт — доказательство аутентификации);
    // тело {} — валидация пустого патча даёт 400 ПРИ пройденной аутентификации,
    // 401 — при непринятой паре.
    private async Task<int> PatchPatroniConfigAsync(
        int patroniPort, string password, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Patch,
            $"https://localhost:{patroniPort}/config")
        {
            Content = new StringContent("{}", Encoding.UTF8, "application/json"),
        };
        request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
            "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"patroni:{password}")));
        using var response = await Fx.NewPatroniHttpClient().SendAsync(request, ct);
        return (int)response.StatusCode;
    }

    // Задача 2: срез «какой rest-пароль принимает каждая нода сейчас» —
    // PATCH /config с константой OLD и с NEW из etcd. Формат строки называет
    // пароль (константа/etcd) и ожидание — по логу окна детерминированно
    // видно, каким паролем ходили и что ожидали.
    private async Task<string> RestAuthProbeLineAsync(string newRest, CancellationToken ct)
    {
        var addresses = await PortallocAsync(Cluster);
        var parts = new List<string>();
        foreach (var (key, addr) in addresses.OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            var oldCode = await PatchPatroniConfigAsync(addr.Patroni, OldRestPasswordConstant, ct);
            var newCode = await PatchPatroniConfigAsync(addr.Patroni, newRest, ct);
            parts.Add($"{key} sent=OLD(const) expect=400-until-R3-then-401 got={oldCode}" +
                      $" | sent=NEW(etcd) expect=401-until-R3-then-400(auth-ok) got={newCode}");
        }
        return string.Join("; ", parts);
    }

    // Компактный срез статусных ключей нод (forensic-телеметрия прогресса
    // ротации: state/applied_revision каждой ноды одной строкой).
    private async Task<string> RangeOrNullAsync(string prefix)
    {
        var range = await G.RangeAsync(Endpoint, prefix, TestContext.Current.CancellationToken);
        return range.Value is not { } list
            ? "<range error>"
            : string.Join("; ", list.Take(12).Select(kv =>
                $"{kv.Key[(kv.Key.LastIndexOf('/') + 1)..]}={kv.Value}"));
    }

    // StartedAt контейнеров нод кластера (docker inspect) — ротация без
    // пересоздания не меняет их (spec §6.3/§9.6).
    private async Task<Dictionary<string, string>> StartedAtMapAsync(string cluster, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var names = (await E2eFixture.RunDockerAsync(
            ["ps", "--format", "{{.Names}}", "--filter", $"name=pgw-{cluster}-"], ct))
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var map = new Dictionary<string, string>();
        foreach (var name in names)
        {
            map[name] = (await E2eFixture.RunDockerAsync(
                ["inspect", "-f", "{{.State.StartedAt}}", name], ct)).Trim();
        }
        sw.Stop();
        Console.WriteLine($"[TIMING] rotate startedAt-map ({map.Count} nodes): {sw.Elapsed.TotalSeconds:F1}s");
        return map;
    }
}

/// <summary>Компаньон-фикстура окружения класса (полный контур на класс:
/// сеть/etcd/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class E2eRotateScenariosFx : E2eContourFixture
{
    protected override string Slug => "rotate";
}
