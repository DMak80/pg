using System.Text.RegularExpressions;
using Npgsql;
using PgWorker.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E ротации app-пароля (spec §7.5): заявка etcdctl-формой → заявка исчезла,
// app_password изменился, новый пароль подключается, старый отвергается.
[Collection(E2eCollection.Name)]
public class E2eRotateScenarios(E2eFixture fixture)
{
    private const string Cluster = "rotate";

    private string Endpoint => fixture.EtcdEndpoint;

    private EtcdGateway G => fixture.Gateway;

    [Fact]
    public async Task Rotate_TicketRotatesPasswordOnAllShards()
    {
        // Arrange — рабочий кластер (provisioning завершён), известен старый пароль
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await SeedClusterAsync(Cluster);
        await using var app = await fixture.StartHostAsync("rotate", ct: ct);
        var provisioned = await E2eFixture.WaitForAsync(async () =>
            await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn") is not null
            && await GetOrNullAsync($"/clusters/{Cluster}/shards/shard2/dsn") is not null,
            TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("кластер поднялся");
        var oldPassword = await fixture.GetAppPasswordAsync(Cluster, ct);

        // Act — заявка ротации (формат панели §9.8)
        await G.PutAsync(Endpoint, $"/pgworker/rotations/{Cluster}",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"requested_by":"e2e"}""",
            null, ct);

        // Assert 1 (критерий 5а/5б): заявка исполнена и удалена; пароль сменился
        var rotated = await E2eFixture.WaitForAsync(async () =>
        {
            var password = await fixture.GetAppPasswordAsync(Cluster, ct);
            return password != oldPassword
                && await GetOrNullAsync($"/pgworker/rotations/{Cluster}") is null;
        }, TimeSpan.FromSeconds(120), ct);
        rotated.Should().BeTrue("заявка исполнена: пароль изменён, ключ заявки удалён");
        var newPassword = await fixture.GetAppPasswordAsync(Cluster, ct);
        Regex.IsMatch(newPassword, "^[A-Za-z0-9]{32}$").Should().BeTrue();

        // Assert 2 (критерий 5в/5г): новый пароль подключается, старый отвергается
        // (проба по фрагментам multi-host dsn — образец E2eAppSecretScenarios).
        var dsn = (await GetOrNullAsync($"/clusters/{Cluster}/shards/shard1/dsn"))!.Value;
        var hosts = Regex.Match(dsn, "host=([^ ]+)").Groups[1].Value.Split(',');
        var ports = Regex.Match(dsn, "port=([^ ]+)").Groups[1].Value.Split(',');
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
