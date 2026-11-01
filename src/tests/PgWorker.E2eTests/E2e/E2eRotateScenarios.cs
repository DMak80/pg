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
        var provisioned = await E2eFixture.WaitForAsync(async () =>
            await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn") is not null
            && await GetOrNullAsync($"/clusters/{Cluster}/shards/shard2/dsn") is not null,
            TimeSpan.FromSeconds(90), ct);
        provisioned.Should().BeTrue("the cluster came up");
        var oldPassword = await Fx.GetAppPasswordAsync(Cluster, ct);
        var oldMover = (await GetOrNullAsync($"/clusters/{Cluster}/mover_password"))!.Value;
        var oldAdmin = (await GetOrNullAsync($"/clusters/{Cluster}/bucket_admin_password"))!.Value;

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
        var rotated = await E2eFixture.WaitForAsync(async () =>
        {
            var password = await Fx.GetAppPasswordAsync(Cluster, ct);
            return password != oldPassword
                && await GetOrNullAsync($"/pgworker/rotations/{Cluster}") is null;
        }, TimeSpan.FromSeconds(90), ct);
        rotated.Should().BeTrue("the request is executed: the password changed, the request key is removed");
        var newPassword = await Fx.GetAppPasswordAsync(Cluster, ct);
        Regex.IsMatch(newPassword, "^[A-Za-z0-9]{32}$").Should().BeTrue();

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
        var windowClosed = await E2eFixture.WaitForAsync(async () =>
        {
            var work = await GetOrNullAsync($"/pgworker/work/{Cluster}");
            return work is not null && !work.Value.Contains("\"rest_window_open\":true");
        }, TimeSpan.FromSeconds(120), ct);
        windowClosed.Should().BeTrue("R4 closes the rotation window");

        // Assert 1c (spec §6.3/§9.6): ротация rest_password БЕЗ пересоздания —
        // StartedAt каждого контейнера нод НЕ изменился.
        var startedAtAfter = await StartedAtMapAsync(Cluster, ct);
        startedAtAfter.Keys.Should().BeEquivalentTo(startedAtBefore.Keys,
            "the node container set did not change");
        foreach (var (name, startedAt) in startedAtAfter)
            startedAt.Should().Be(startedAtBefore[name],
                $"the container {name} was not recreated by the rotation");

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

    // StartedAt контейнеров нод кластера (docker inspect) — ротация без
    // пересоздания не меняет их (spec §6.3/§9.6).
    private async Task<Dictionary<string, string>> StartedAtMapAsync(string cluster, CancellationToken ct)
    {
        var names = (await E2eFixture.RunDockerAsync(
            ["ps", "--format", "{{.Names}}", "--filter", $"name=pgw-{cluster}-"], ct))
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var map = new Dictionary<string, string>();
        foreach (var name in names)
        {
            map[name] = (await E2eFixture.RunDockerAsync(
                ["inspect", "-f", "{{.State.StartedAt}}", name], ct)).Trim();
        }
        return map;
    }
}

/// <summary>Компаньон-фикстура окружения класса (полный контур на класс:
/// сеть/etcd/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class E2eRotateScenariosFx : E2eContourFixture
{
    protected override string Slug => "rotate";
}
