using System.Text;
using System.Text.Json;
using Npgsql;
using PgWorker.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Moves;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E-сценарии масштабирования шардов (t06 spec §8-1..5, критерии §9.2–9.4) на
// живом стенде E2eFixture: add ПУСТОГО шарда в Active-кластер (routing/schema-мир
// не тронут — главная граница §2.1), G3-блокировка remove с бакетами, авто-демонтаж
// после явных переездов, освобождение имени, takeover посреди A3.
[Collection(E2eCollection.Name)]
public class E2eScaleScenarios(E2eFixture fixture)
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    private string Endpoint => fixture.EtcdEndpoint;

    private EtcdGateway G => fixture.Gateway;

    [Fact]
    public async Task Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "sshop";

        // ---------- §8-1: add-shard в живой кластер — шард поднят и ПУСТ ----------
        // Arrange: сид sshop (NOT_INITIALIZED) → контроллер → provisioning до Active.
        await SeedClusterAsync(cluster);
        await using var p1 = await fixture.StartHostAsync("s1", ct: ct);

        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning sshop должен дойти до Active (dsn/RUNNING/без status)");

        // DDL-сид: bucket_0 у shard1 и bucket_1/bucket_3 у shard2 (INSERT-пробы
        // живости записи до/после scale-операций; гранты app выдаёт сид).
        var m1 = await MasterInfoAsync(cluster, "shard1", ct);
        var m2 = await MasterInfoAsync(cluster, "shard2", ct);
        await SeedBucketAsync(m1.Dsn, "bucket_0", ct);
        await SeedBucketAsync(m2.Dsn, "bucket_1", ct);
        await SeedBucketAsync(m2.Dsn, "bucket_3", ct);
        (await TryInsertAppAsync(cluster, "shard1", "bucket_0", ct))
            .Should().BeTrue("запись в bucket_0 (владелец shard1) работает до add-shard");

        // Снапшот routing ДО (главный ассерт границы §2.1).
        var routingBefore = await RoutingSnapshotAsync(cluster, ct);

        // Act: сид add-декларации shard3 В СТИЛЕ ПАНЕЛИ (§6.1).
        await SeedAddDeclarationAsync(cluster, "shard3", ct);

        // Assert: шард поднят и зарегистрирован; контейнеры живы.
        var added = await E2eFixture.WaitForAsync(() => ShardRegisteredAsync(cluster, "shard3"),
            TimeSpan.FromSeconds(360), ct);
        added.Should().BeTrue($"AddShardProcess должен поднять shard3 (dsn/RUNNING); work={await WorkDumpAsync(cluster, ct)}");

        var containers = await ListContainerNamesAsync($"pgw-{cluster}-shard3-");
        containers.Should().BeEquivalentTo([$"pgw-{cluster}-shard3-shard3a", $"pgw-{cluster}-shard3-shard3b"],
            "контейнеры нового шарда подняты");

        // ГЛАВНЫЙ ассерт границы §2.1: routing/status/schema-мир не изменён НИКАК.
        (await RoutingSnapshotAsync(cluster, ct)).Should().Equal(routingBefore,
            "add-shard не двигает ни один бакет (routing неизменен)");
        (await RangeAsync($"/clusters/{cluster}/buckets/status/")).Should().BeEmpty("status-ключи не появились");
        var m3 = await MasterInfoAsync(cluster, "shard3", ct);
        var schemas = await SqlScalarAsync(m3.Dsn,
            "SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'bucket_%'", ct);
        schemas.Should().Be("0", "шард стартует ПУСТЫМ — схем бакетов на нём нет (§2.1)");
        (await TryInsertAppAsync(cluster, "shard1", "bucket_0", ct))
            .Should().BeTrue("запись в существующие бакеты не прерывалась");

        // ---------- §8-2: remove шарда с бакетами заблокирован G3 ----------
        // Arrange: sync-режим приёмников (P8-префлайт move), переезд bucket_1 → shard3.
        await EnableSyncModeAsync(cluster, "shard3", ct);
        await PutMoveRequestAsync(cluster, "bucket_1",
            $$"""{"op":"move","to":"shard3","requested_unix":{{NowUnix()}}}""", ct);
        var moved = await E2eFixture.WaitForAsync(
            () => RoutingIsAsync(cluster, "bucket_1", "shard3", ct), TimeSpan.FromSeconds(120), ct);
        moved.Should().BeTrue($"move bucket_1 → shard3 должен завершиться; work={await WorkDumpAsync(cluster, ct)}");

        // Act: маркер демонтажа на shard1 (на нём ещё 3 бакета: 0, 2, 4).
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/shard1/state", "TO_REMOVE", null, ct);

        // Assert: G3 держит демонтаж — фаза blocked-G3 и причина с числом бакетов
        // (кириллица в work-JSON экранируется \uXXXX — читаем поле last_error парсером).
        var blocked = await E2eFixture.WaitForAsync(async () =>
        {
            var work = (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value;
            if (work is null || !work.Contains("blocked-G3", StringComparison.Ordinal))
                return false;
            try
            {
                using var doc = JsonDocument.Parse(work);
                return doc.RootElement.TryGetProperty("last_error", out var error)
                    && error.GetString()?.Contains("бакет", StringComparison.Ordinal) == true;
            }
            catch (JsonException)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(60), ct);
        blocked.Should().BeTrue($"guard G3 должен записать причину с числом бакетов; work={await WorkDumpAsync(cluster, ct)}");
        (await ListContainerNamesAsync($"pgw-{cluster}-shard1-")).Should().HaveCount(2,
            "контейнеры помеченного шарда живы, пока на нём бакеты");
        (await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/state"))!.Value.Should().Be("TO_REMOVE");

        // ---------- §8-3: явные переезды → демонтаж завершается сам ----------
        // Act: увозим оставшиеся бакеты shard1 (bucket_0 → shard3, 2/4 → shard2) +
        // finalize каждого (заявки t01; повторную команду демонтажа НЕ подаём).
        await PutMoveRequestAsync(cluster, "bucket_0",
            $$"""{"op":"move","to":"shard3","requested_unix":{{NowUnix()}}}""", ct);
        await PutMoveRequestAsync(cluster, "bucket_2",
            $$"""{"op":"move","to":"shard2","requested_unix":{{NowUnix()}}}""", ct);
        await PutMoveRequestAsync(cluster, "bucket_4",
            $$"""{"op":"move","to":"shard2","requested_unix":{{NowUnix()}}}""", ct);
        await EnableSyncModeAsync(cluster, "shard2", ct);

        var allMoved = await E2eFixture.WaitForAsync(async () =>
            await RoutingIsAsync(cluster, "bucket_0", "shard3", ct)
            && await RoutingIsAsync(cluster, "bucket_2", "shard2", ct)
            && await RoutingIsAsync(cluster, "bucket_4", "shard2", ct),
            TimeSpan.FromSeconds(240), ct);
        allMoved.Should().BeTrue($"все бакеты shard1 должны уехать; work={await WorkDumpAsync(cluster, ct)}, " +
                                 $"заявки={await MovesDumpAsync(cluster, ct)}");

        foreach (var bucket in new[] { "bucket_0", "bucket_2", "bucket_4" })
            await PutMoveRequestAsync(cluster, bucket,
                $$"""{"op":"finalize","old_shard":"shard1","requested_unix":{{NowUnix()}}}""", ct);

        var finalized = await E2eFixture.WaitForAsync(
            () => MovesEmptyAsync(cluster, ct), TimeSpan.FromSeconds(240), ct);
        finalized.Should().BeTrue($"finalize-заявки должны разобраться; заявки={await MovesDumpAsync(cluster, ct)}, " +
                                  $"work={await WorkDumpAsync(cluster, ct)}");

        // Assert: демонтаж дошёл сам (маркер не повторяли): контейнеры/volumes
        // удалены, ключи/порталы вычищены, кластер продолжает обслуживать запись.
        var dismantled = await E2eFixture.WaitForAsync(
            () => ShardDismantledAsync(cluster, "shard1", ct), TimeSpan.FromSeconds(180), ct);
        dismantled.Should().BeTrue($"после уезда последнего бакета демонтаж завершается сам; " +
                                   $"work={await WorkDumpAsync(cluster, ct)}, контейнеры={string.Join(",", await ListContainerNamesAsync($"pgw-{cluster}-shard1-", all: true))}");

        (await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/dsn")).Should().BeNull("ключей демонтированного шарда нет");
        (await RangeAsync($"/clusters/{cluster}/shards/shard1/")).Should().BeEmpty();
        (await RangeAsync($"/service/{cluster}-shard1/")).Should().BeEmpty();
        var portalloc = (await GetOrNullAsync($"/pgworker/portalloc/{cluster}"))!.Value;
        portalloc.Should().NotContain("shard1/").And.Contain("shard2/").And.Contain("shard3/",
            "portalloc вычищен точечно — записи остальных шардов живы");

        (await TryInsertAppAsync(cluster, "shard3", "bucket_1", ct))
            .Should().BeTrue("кластер жив: запись в бакет на shard3 успешна после демонтажа");
        (await TryInsertAppAsync(cluster, "shard2", "bucket_3", ct))
            .Should().BeTrue("кластер жив: запись в бакет на shard2 успешна после демонтажа");

        // ---------- §8-5: имя освобождается демонтажом ----------
        // Act: add-декларация с именем shard1 НАПРЯМУЮ в etcd (в обход автогенерации).
        await SeedAddDeclarationAsync(cluster, "shard1", ct);

        // Assert: AddShardProcess принял освобождённое имя (клэйм-инвариант §4.2).
        var reused = await E2eFixture.WaitForAsync(() => ShardRegisteredAsync(cluster, "shard1"),
            TimeSpan.FromSeconds(360), ct);
        reused.Should().BeTrue($"имя shard1 освобождено демонтажом и принято заново; work={await WorkDumpAsync(cluster, ct)}");
    }

    [Fact]
    public async Task Scale_TakeoverMidAdd_SecondInstanceFinishesNoDuplicates()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "stshop";

        // ---------- §8-4: takeover посреди A3 ----------
        // Arrange: живой кластер stshop (provisioned первым инстансом), затем
        // add-декларация shard3; ждём ПЕРВЫЙ контейнер нового шарда (A3 начался).
        await SeedClusterAsync(cluster);
        await using var s2 = await fixture.StartHostAsync("s2", ct: ct);

        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning stshop должен дойти до Active до старта add");

        await SeedAddDeclarationAsync(cluster, "shard3", ct);
        var a3Started = await E2eFixture.WaitForAsync(
            () => DockerHasAsync($"pgw-{cluster}-shard3-"), TimeSpan.FromSeconds(120), ct);
        a3Started.Should().BeTrue("первый инстанс должен начать A3 (появился контейнер shard3)");

        // Act: docker-kill PgWorker посреди A3 → второй инстанс доносит шард.
        s2.Kill();
        await s2.DisposeAsync();
        await using var s3 = await fixture.StartHostAsync("s3", ct: ct);

        // Assert: шард донесён (клэйм истёк ≤15 с), дублей контейнеров нет.
        var finished = await E2eFixture.WaitForAsync(() => ShardRegisteredAsync(cluster, "shard3"),
            TimeSpan.FromSeconds(360), ct);
        finished.Should().BeTrue($"второй инстанс должен донести shard3 после takeover; work={await WorkDumpAsync(cluster, ct)}");

        var containers = await ListContainerNamesAsync($"pgw-{cluster}-shard3-", all: true);
        containers.Should().HaveCount(2, "контейнеров нового шарда ровно 2 (нет дублей после takeover)");
    }

    // ===== Хелперы (приёмы E2eScenarios/E2eMoveScenarios, scoped на кластер) =====

    private static long NowUnix() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    private async Task<IReadOnlyList<Kv>> RangeAsync(string prefix)
        => (await G.RangeAsync(Endpoint, prefix, TestContext.Current.CancellationToken)).Value;

    private async Task PutMoveRequestAsync(string cluster, string bucket, string json, CancellationToken ct)
        => (await G.PutAsync(Endpoint, MoveNames.MoveKey(cluster, bucket), json, null, ct))
            .IsSuccess.Should().BeTrue($"заявка {bucket} должна записаться в etcd");

    private async Task<string?> RoutingAsync(string cluster, string bucket, CancellationToken ct)
        => (await GetOrNullAsync(MoveNames.RoutingKey(cluster, bucket)))?.Value;

    private async Task<string?> StatusAsync(string cluster, string bucket, CancellationToken ct)
        => (await GetOrNullAsync(MoveNames.StatusKey(cluster, bucket)))?.Value;

    private async Task<bool> RoutingIsAsync(string cluster, string bucket, string shard, CancellationToken ct)
        => await RoutingAsync(cluster, bucket, ct) == shard && await StatusAsync(cluster, bucket, ct) is null;

    // Снапшот routing кластера (упорядоченные пары ключ=значение) — сравнение до/после.
    private async Task<List<string>> RoutingSnapshotAsync(string cluster, CancellationToken ct)
        => (await RangeAsync($"/clusters/{cluster}/buckets/routing/"))
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => $"{kv.Key}={kv.Value}")
            .ToList();

    private async Task<string> WorkDumpAsync(string cluster, CancellationToken ct)
        => (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "нет";

    private async Task<string> MovesDumpAsync(string cluster, CancellationToken ct)
    {
        var kvs = await RangeAsync(MoveNames.MovesPrefix(cluster));
        return kvs.Count == 0 ? "нет" : string.Join("; ", kvs.Select(k => $"{k.Key.Split('/')[^1]}={k.Value}"));
    }

    private async Task<bool> MovesEmptyAsync(string cluster, CancellationToken ct)
        => (await RangeAsync(MoveNames.MovesPrefix(cluster))).Count == 0;

    private async Task<bool> ProvisionedAsync(string cluster)
    {
        var config = await GetOrNullAsync($"/clusters/{cluster}/config");
        if (config is null || JsonSerializer
                .Deserialize<Dictionary<string, JsonElement>>(config.Value)!.ContainsKey("state"))
            return false;
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            if (await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/dsn") is null)
                return false;
            foreach (var node in new[] { $"{shard}a", $"{shard}b" })
                if ((await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{node}/state"))?.Value != "RUNNING")
                    return false;
        }

        return (await RangeAsync($"/clusters/{cluster}/buckets/status/")).Count == 0;
    }

    // Шард поднят и зарегистрирован: dsn записан, все ноды RUNNING (§4.3 финал add).
    private async Task<bool> ShardRegisteredAsync(string cluster, string shard)
    {
        if (await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/dsn") is null)
            return false;
        foreach (var node in new[] { $"{shard}a", $"{shard}b" })
            if ((await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/nodes/{node}/state"))?.Value != "RUNNING")
                return false;

        return true;
    }

    // Шард демонтирован: контейнеров/volumes нет, ключи шарда и scope пусты (§4.3 финал remove).
    private async Task<bool> ShardDismantledAsync(string cluster, string shard, CancellationToken ct)
    {
        if ((await ListContainerNamesAsync($"pgw-{cluster}-{shard}-", all: true)).Count > 0)
            return false;
        var volumes = await fixture.RunDockerAsync(
            ["volume", "ls", "-q", "--filter", $"name=pgw-{cluster}-{shard}-"], ct);
        if (volumes.Length > 0)
            return false;
        if ((await RangeAsync($"/clusters/{cluster}/shards/{shard}/")).Count > 0)
            return false;

        return (await RangeAsync($"/service/{cluster}-{shard}/")).Count == 0;
    }

    // Сид кластера в стиле панели (02 §9.1) — копия E2eScenarios.SeedClusterAsync.
    private async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config",
            $$"""{"buckets":6,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}""",
            null, ct);
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "8Gi", null, ct);
        }

        for (var i = 0; i < 6; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/routing/bucket_{i}", $"shard{i % 2 + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    // Add-декларация в стиле панели (§4.1/§6.1): replicas + nodes + request_*,
    // БЕЗ dsn (его запишет PgWorker) и БЕЗ routing/status (шард пустой).
    private async Task SeedAddDeclarationAsync(string cluster, string shard, CancellationToken ct)
    {
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_cpu", "2", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "8Gi", null, ct);
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_disk", "10Gi", null, ct);
    }

    // Таблица с identity-sequence в бакете + гранты app/mover (приём E2eMoveScenarios).
    private static async Task SeedBucketAsync(string adminDsn, string bucket, CancellationToken ct)
    {
        var ddl = $"""
            CREATE TABLE {bucket}.items(id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, note text NOT NULL);
            INSERT INTO {bucket}.items(note) SELECT 'seed' FROM generate_series(1, 10);
            GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {bucket} TO app;
            GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA {bucket} TO app;
            GRANT SELECT ON ALL TABLES IN SCHEMA {bucket} TO bucket_mover;
            """;
        await using var con = new NpgsqlConnection($"{adminDsn};Timeout=10;SSL Mode=Require;Trust Server Certificate=true");
        await con.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(ddl, con);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private sealed record NodeAddr(string Host, int Pg, int Patroni, int Doorman);

    private async Task<Dictionary<string, NodeAddr>> PortallocAsync(string cluster)
    {
        var kv = await GetOrNullAsync($"/pgworker/portalloc/{cluster}");
        if (kv is null)
            return [];
        return JsonSerializer.Deserialize<Dictionary<string, NodeAddr>>(kv.Value, Json) ?? [];
    }

    private sealed record MasterInfo(string Node, int Port, int PatroniPort, string Dsn);

    // Мастер шарда: master-ключ host:doorman → порты из portalloc (приём E2eMoveScenarios).
    private async Task<MasterInfo> MasterInfoAsync(string cluster, string shard, CancellationToken ct)
    {
        for (var i = 0; i < 60; i++)
        {
            var key = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/master");
            if (key is { Value.Length: > 0 })
            {
                var doorman = key.Value.Split(':')[^1];
                var match = (await PortallocAsync(cluster))
                    .FirstOrDefault(p => p.Key.StartsWith($"{shard}/") && p.Value.Doorman.ToString() == doorman);
                if (match.Key is { Length: > 0 })
                {
                    var node = match.Key.Split('/')[1];
                    return new MasterInfo(node, match.Value.Pg, match.Value.Patroni,
                        $"Host=localhost;Port={match.Value.Pg};Database={cluster};Username=postgres;Password={E2eFixture.SuPassword}");
                }
            }

            await Task.Delay(1000, ct);
        }

        throw new ApplicationException($"мастер {cluster}/{shard} не найден за 60 с");
    }

    // Включение synchronous_mode у Patroni мастера шарда (P8-префлайт move).
    private async Task EnableSyncModeAsync(string cluster, string shard, CancellationToken ct)
    {
        var master = await MasterInfoAsync(cluster, shard, ct);
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        using var response = await http.PatchAsync(
            $"http://localhost:{master.PatroniPort}/config",
            new StringContent("""{"synchronous_mode":true}""", Encoding.UTF8, "application/json"), ct);
        response.IsSuccessStatusCode.Should().BeTrue(
            $"Patroni {shard} должен принять PATCH /config (получили HTTP {(int)response.StatusCode})");

        var synced = await E2eFixture.WaitForAsync(async () =>
        {
            var names = await SqlScalarAsync(master.Dsn,
                "SELECT setting FROM pg_settings WHERE name = 'synchronous_standby_names'", ct);
            var count = await SqlScalarAsync(master.Dsn,
                "SELECT count(*) FROM pg_stat_replication WHERE sync_state IN ('sync','quorum')", ct);
            return !string.IsNullOrWhiteSpace(names) && long.Parse(count) >= 1;
        }, TimeSpan.FromSeconds(60), ct);
        synced.Should().BeTrue($"у мастера {shard} должен появиться sync-standby после PATCH");
    }

    private static async Task<string> SqlScalarAsync(string dsn, string sql, CancellationToken ct)
    {
        await using var con = new NpgsqlConnection($"{dsn};Timeout=10;SSL Mode=Require;Trust Server Certificate=true");
        await con.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, con);
        return (await cmd.ExecuteScalarAsync(ct))?.ToString() ?? "";
    }

    // Запись под app в бакет (мастер шарда): true = прошла, false = ошибка.
    // Пароль — из etcd-ключа /clusters/<C>/app_password (путь приложения, spec §4.3).
    private async Task<bool> TryInsertAppAsync(string cluster, string shard, string bucket, CancellationToken ct)
    {
        try
        {
            var master = await MasterInfoAsync(cluster, shard, ct);
            await using var con = new NpgsqlConnection(
                $"Host=localhost;Port={master.Port};Database={cluster};Username=app;" +
                $"Password={await fixture.GetAppPasswordAsync(cluster, ct)};Timeout=10;SSL Mode=Require;Trust Server Certificate=true");
            await con.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand($"INSERT INTO {bucket}.items(note) VALUES ('scale-probe')", con);
            await cmd.ExecuteNonQueryAsync(ct);
            return true;
        }
        catch (PostgresException)
        {
            return false;
        }
    }

    private async Task<bool> DockerHasAsync(string prefix)
        => (await ListContainerNamesAsync(prefix)).Count > 0;

    private async Task<List<string>> ListContainerNamesAsync(string prefix, bool all = false)
    {
        var ct = TestContext.Current.CancellationToken;
        var args = new List<string> { "ps", "--format", "{{.Names}}" };
        if (all)
            args.Add("-a");
        args.AddRange(["--filter", $"name={prefix}"]);
        var output = await fixture.RunDockerAsync([.. args], ct);
        return output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
    }
}
