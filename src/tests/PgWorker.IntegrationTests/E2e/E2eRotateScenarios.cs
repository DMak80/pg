using System.Text.RegularExpressions;
using Npgsql;
using Shared.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E ротации per-cluster секретов (t02, spec §7.5): заявка etcdctl-формой →
// заявка исчезла, app/mover/bucket_admin изменились, dsn перезаписан, новый
// пароль подключается, старый отвергается, пишущая нагрузка переживает ротацию.
public class E2eRotateScenarios
{
    // Уникальное имя кластера на прогон ({slug}{тег прогона}, docs/e2e-isolation.md
    // §1): движковые контейнеры/тома pgw-<C>-* опознаются teardown'ом окружения
    // по своему тегу (OwnName) и снимаются им; константное имя запрещено.
    private string Cluster => $"rotate{Fx.ClusterTag}";

    // Окружение Fact'а (своя сеть/etcd); создаётся в начале каждого сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    [Fact]
    public async Task Rotate_TicketRotatesPerClusterSecretsOnAllShards()
    {
        // Arrange — рабочий кластер (provisioning завершён), известны старые креды
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("rotate", ct: ct);
        Fx = fx;
        await SeedClusterAsync(Cluster);
        await using var app = await Fx.StartHostAsync("rotate", ct: ct);
        var provisioned = await E2eFixture.WaitForAsync(async () =>
            await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn") is not null
            && await GetOrNullAsync($"/clusters/{Cluster}/shards/shard2/dsn") is not null,
            TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("кластер поднялся");
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
        }, TimeSpan.FromSeconds(120), ct);
        rotated.Should().BeTrue("заявка исполнена: пароль изменён, ключ заявки удалён");
        var newPassword = await Fx.GetAppPasswordAsync(Cluster, ct);
        Regex.IsMatch(newPassword, "^[A-Za-z0-9]{32}$").Should().BeTrue();

        // Assert 1b (t02): mover/bucket_admin тоже сменились (32 симв [A-Za-z0-9]);
        // dsn-ключи шардов перезаписаны новым bucket_admin-паролем.
        var newMover = (await GetOrNullAsync($"/clusters/{Cluster}/mover_password"))!.Value;
        var newAdmin = (await GetOrNullAsync($"/clusters/{Cluster}/bucket_admin_password"))!.Value;
        newMover.Should().NotBe(oldMover).And.MatchRegex("^[A-Za-z0-9]{32}$");
        newAdmin.Should().NotBe(oldAdmin).And.MatchRegex("^[A-Za-z0-9]{32}$");
        var newDsn = (await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn"))!.Value;
        newDsn.Should().Contain($"password={newAdmin}").And.NotContain(oldAdmin);

        // Assert 2b (t02, критерий «без остановки записи»): writer — DSN-точка
        // входа bucket_admin — перечитывает креды из etcd на каждую попытку
        // (поведение клиента по arch/14) и продолжает писать после ротации.
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
        wrote.Should().BeTrue($"письменная нагрузка продолжается после ротации (inserts={inserts}, lastError={lastError})");

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
        newWorks.Should().BeTrue("новый пароль подключается user=app");

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
        oldRejected.Should().BeTrue("старый пароль отвергается (auth fail)");
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
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "8Gi", null, ct);
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
}
