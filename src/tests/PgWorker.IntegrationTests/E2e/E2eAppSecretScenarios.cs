using System.Text.Json;
using System.Text.RegularExpressions;
using Npgsql;
using PgWorker.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E per-cluster app-секрета (spec §7.1–7.3): provisioning создаёт ключи
// app_user/app_password, креды user=app работают против БД шарда (прямой
// pg-порт ноды — стенд без doorman, spec §7.2), значение пароля стабильно
// между тиками (идемпотентность).
[Collection(E2eCollection.Name)]
public class E2eAppSecretScenarios(E2eFixture fixture)
{
    private const string Cluster = "appsecret";

    private string Endpoint => fixture.EtcdEndpoint;

    private EtcdGateway G => fixture.Gateway;

    [Fact]
    public async Task AppSecret_Provisioning_KeysRoleAndStability()
    {
        // Arrange — сид кластера в стиле панели (config NOT_INITIALIZED с
        // bucket_admin-кредами фикстуры; app-ключи НЕ сеём — генерирует PgWorker)
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await SeedClusterAsync(Cluster);
        await using var app = await fixture.StartHostAsync("appsecret", ct: ct);

        // Act — ждать provisioning до dsn-ключа шарда (SQL-фаза прошла)
        var provisioned = await E2eFixture.WaitForAsync(
            async () => await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn") is not null,
            TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning должен дойти до SQL-фазы (dsn записан)");

        // Assert 1 (критерий 1): /clusters/<C>/app_user = "app"; app_password —
        // 32 символа [A-Za-z0-9]; dsn несёт user=bucket_admin (как было), но НЕ
        // содержит user=app и значения app-пароля (секрет в DSN не светится — §2.4).
        var userKv = await GetOrNullAsync($"/clusters/{Cluster}/app_user");
        userKv.Should().NotBeNull();
        userKv!.Value.Should().Be("app");
        var password = await fixture.GetAppPasswordAsync(Cluster, ct);
        Regex.IsMatch(password, "^[A-Za-z0-9]{32}$").Should().BeTrue(
            "app_password — 32 символа [A-Za-z0-9] (spec §4.1)");
        var dsnKv = await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn");
        dsnKv.Should().NotBeNull();
        dsnKv!.Value.Should().Contain("user=bucket_admin");
        dsnKv.Value.Should().NotContain("user=app");
        dsnKv.Value.Should().NotContain(password, "app-пароль не попадает в dsn-ключ (spec §2.4)");

        // Assert 2 (критерий 2): подключение user=app паролем из etcd выполняет
        // запрос к данным. ПРЯМОЙ pg-порт ноды (пофрагментный проб multi-host DSN,
        // образец E2eScenarios: стенд без doorman, EnableDoorman=false; канон
        // деплоя doorman :6432 — spec §3.1/§7.2 — отдельно не проверяется).
        var hosts = Regex.Match(dsnKv.Value, "host=([^ ]+)").Groups[1].Value.Split(',');
        var ports = Regex.Match(dsnKv.Value, "port=([^ ]+)").Groups[1].Value.Split(',');
        hosts.Should().HaveCount(2);
        ports.Should().HaveCount(2);
        var connected = false;
        foreach (var (host, port) in hosts.Zip(ports))
        {
            // Реплика в первый момент может рестартовать (bootstrap) — ретраи.
            connected = await E2eFixture.WaitForAsync(async () =>
            {
                try
                {
                    await using var con = new NpgsqlConnection(
                        $"Host={host};Port={port};Database={Cluster};Username={userKv.Value};" +
                        $"Password={password};Timeout=5;SSL Mode=Require;Trust Server Certificate=true");
                    await con.OpenAsync(ct);
                    await using var cmd = new NpgsqlCommand("SELECT 1", con);
                    return await cmd.ExecuteScalarAsync(ct) is 1;
                }
                catch (NpgsqlException)
                {
                    return false; // ещё не готова/не мастер — повторим
                }
            }, TimeSpan.FromSeconds(60), ct);
            if (connected)
                break;
        }

        connected.Should().BeTrue("креды user=app + пароль из etcd работают против БД шарда (критерий 2)");

        // Assert 3 (критерий 3): пауза ≥2 тиков scan (1 с) — app_password не меняется
        // (идемпотентность ensure: put-if-absent не перегенерирует существующее).
        await Task.Delay(TimeSpan.FromSeconds(5), ct);
        var passwordAgain = await fixture.GetAppPasswordAsync(Cluster, ct);
        passwordAgain.Should().Be(password, "повторные тики не перегенерируют app-пароль (spec §2.5)");
    }

    // ===== Хелперы =====

    // Сид кластера в стиле панели (02 §9.1; образец E2eScenarios.SeedClusterAsync):
    // config NOT_INITIALIZED с bucket_admin-кредами фикстуры, 2 шарда, routing.
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
