using System.Text;
using System.Text.Json;
using Npgsql;
using PgWorker.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Moves;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E-сценарий переезда бакета (t01 задача 19, spec §10 e2e / §11 AC3–AC6+AC8).
// Один [Fact], 5 этапов вызывают друг друга по цепочке:
//   E1 → E2 → E3 → E4 → E5
// Каждый этап — приватный метод, output.WriteLine показывает прогресс.
[Collection(E2eCollection.Name)]
public class E2eMoveScenarios(E2eFixture fixture, ITestOutputHelper output)
{
    private const string Cluster = "mshop";

    private string Endpoint => fixture.EtcdEndpoint;
    private EtcdGateway G => fixture.Gateway;

    private static HostInstance? Host { get; set; }
    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    [Fact]
    public async Task Move_Lifecycle_Chain()
    {
        DockerTrait.SkipIfUnavailable();
        await E1_Provisioning();
        output.WriteLine("=== E1 done, calling E2 ===");

        await E2_MoveUnderLoad();
        output.WriteLine("=== E2 done, calling E3 ===");

        await E3_AutoFinalize();
        output.WriteLine("=== E3 done, calling E4 ===");

        await E4_AbortTakeover();
        output.WriteLine("=== E4 done, calling E5 ===");

        await E5_Deprovisioning();
        output.WriteLine("=== E5 done — lifecycle complete ===");
    }

    // ===== E1: Provisioning =====

    private async Task E1_Provisioning()
    {
        var ct = TestContext.Current.CancellationToken;
        output.WriteLine("E1: seeding cluster + starting host...");

        await SeedClusterAsync();
        Host = await fixture.StartHostAsync("m1", ct: ct);

        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning mshop должен дойти до DONE");

        var shard1 = await MasterInfoAsync("shard1", ct);
        var shard2 = await MasterInfoAsync("shard2", ct);
        await SeedBucketAsync(shard1.Dsn, "bucket_0", 50, ct);
        await SeedBucketAsync(shard2.Dsn, "bucket_1", 10, ct);
        await EnableSyncModeAsync("shard1", ct);
        await EnableSyncModeAsync("shard2", ct);

        (await RoutingAsync("bucket_0", ct)).Should().Be("shard1", "bucket_0 на shard1");
        (await RoutingAsync("bucket_1", ct)).Should().Be("shard2", "bucket_1 на shard2");
        output.WriteLine("E1: provisioning done");
    }

    // ===== E2: Move under load =====

    private async Task E2_MoveUnderLoad()
    {
        var ct = TestContext.Current.CancellationToken;
        output.WriteLine("E2: move bucket_0 → shard2 under load...");

        var shard1 = await MasterInfoAsync("shard1", ct);
        var shard2 = await MasterInfoAsync("shard2", ct);

        await using var ghost = new NpgsqlConnection(await AppDsnAsync(shard1.Port, ct));
        await ghost.OpenAsync(ct);
        await using (var warm = new NpgsqlCommand("SELECT 1", ghost))
            await warm.ExecuteNonQueryAsync(ct);

        using var load = new LoadWorker(this);
        load.Start();

        await PutMoveRequestAsync("bucket_0",
            $$"""{"op":"move","to":"shard2","requested_unix":{{NowUnix()}}}""", ct);

        var moved = await E2eFixture.WaitForAsync(
            () => RoutingIsAsync("bucket_0", "shard2", ct), TimeSpan.FromSeconds(120), ct);
        moved.Should().BeTrue($"move должен завершиться; routing={await RoutingAsync("bucket_0", ct)}, " +
                              $"status={await StatusAsync("bucket_0", ct) ?? "нет"}, work={await WorkDumpAsync(ct)}");

        // Детерминизация (t09, гонка E2-kill ↔ M6-REPLACE): routing+статус
        // сменяются РАНЬШЕ, чем воркер допишет REPLACE на finalize — между ними
        // секунды post-flip SQL (pub_rb/sub_rb). Kill до REPLACE оставлял заявку
        // op=move, m2 отклонял её «бакет уже на shard2», и finalize не начинался
        // (E3: artifacts не вычищены). Ждём op=finalize — свидетельство M6.
        var replacedOnFinalize = await E2eFixture.WaitForAsync(async () =>
        {
            var req = await GetOrNullAsync(MoveNames.MoveKey(Cluster, "bucket_0"));
            return req?.Value.Contains("\"op\":\"finalize\"", StringComparison.Ordinal) == true;
        }, TimeSpan.FromSeconds(15), ct);
        replacedOnFinalize.Should().BeTrue($"перед kill заявка должна стать finalize (M6 REPLACE); " +
                                           $"заявка={await MovesDumpAsync(ct)}, work={await WorkDumpAsync(ct)}");

        // НEMЕДЛЕННО убиваем контроллер: auto-finalize не должен удалить схему на shard1
        // до завершения проверок FROZEN/counts/sequence/ghost.
        Host!.Kill();
        await Host.DisposeAsync();
        Host = null;

        output.WriteLine("E2: routing flipped, host killed, checking FROZEN window...");

        var sawDenied = await E2eFixture.WaitForAsync(
            () => Task.FromResult(load.Snapshot().Any(e => !e.Ok)), TimeSpan.FromSeconds(20), ct);
        sawDenied.Should().BeTrue("нагрузка обязана упереться в заморозку P1 (42501)");
        var firstDenied = load.Snapshot().First(e => !e.Ok);
        var okAfter = await E2eFixture.WaitForAsync(
            () => Task.FromResult(load.Snapshot().Any(e => e.Ok && e.TsMs >= firstDenied.TsMs)),
            TimeSpan.FromSeconds(20), ct);
        okAfter.Should().BeTrue("после flip нагрузка обязана продолжить запись у нового владельца");
        var firstOkAfter = load.Snapshot().First(e => e.Ok && e.TsMs >= firstDenied.TsMs);
        TimeSpan.FromMilliseconds(firstOkAfter.TsMs - firstDenied.TsMs)
            .Should().BeLessOrEqualTo(TimeSpan.FromSeconds(15), "окно FROZEN ≤ 15с (AC3)");
        firstOkAfter.Port.Should().Be(shard2.Port, "первая запись после flip — через мастер shard2");
        output.WriteLine("E2: FROZEN OK, checking counts/sequence/ghost...");

        load.Pause();

        var aligned = await E2eFixture.WaitForAsync(async () =>
        {
            var c1 = await SqlScalarAsync(shard1.Dsn, "SELECT count(*) FROM bucket_0.items", ct);
            var c2 = await SqlScalarAsync(shard2.Dsn, "SELECT count(*) FROM bucket_0.items", ct);
            return c1 == c2;
        }, TimeSpan.FromSeconds(15), ct);
        aligned.Should().BeTrue("обратная репликация sub_rb должна догнать");

        var count1 = await SqlScalarAsync(shard1.Dsn, "SELECT count(*) FROM bucket_0.items", ct);
        var count2 = await SqlScalarAsync(shard2.Dsn, "SELECT count(*) FROM bucket_0.items", ct);
        count1.Should().Be(count2, "counts источника/приёмника совпадают (P8)");
        long.Parse(count1).Should().BeGreaterThanOrEqualTo(50, "нагрузочные строки не потеряны");

        var issued = long.Parse(await SqlScalarAsync(shard1.Dsn,
            "SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM bucket_0.items_id_seq", ct));
        var nextOnNew = long.Parse(await SqlScalarAsync(shard2.Dsn,
            "SELECT CASE WHEN is_called THEN last_value + 1 ELSE last_value END FROM bucket_0.items_id_seq", ct));
        nextOnNew.Should().BeGreaterThan(issued, "sequence-инвариант (P6)");

        PostgresException? ghostError = null;
        try
        {
            await using var cmd = new NpgsqlCommand("INSERT INTO bucket_0.items(note) VALUES ('ghost')", ghost);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException e) { ghostError = e; }
        ghostError.Should().NotBeNull("призрак-P1: write в старой сессии обязан упасть");
        ghostError!.SqlState.Should().Be("42501", "заморозка срезает write (permission denied)");
        await load.StopAsync();
        output.WriteLine("E2: counts/sequence/ghost OK");
    }

    // ===== E3: Auto-finalize =====

    private async Task E3_AutoFinalize()
    {
        var ct = TestContext.Current.CancellationToken;
        output.WriteLine("E3: starting host for auto-finalize...");

        (await RoutingAsync("bucket_0", ct)).Should().Be("shard2", "E2 переехал bucket_0 на shard2");

        Host = await fixture.StartHostAsync("m2", ct: ct);

        var autoFin = await E2eFixture.WaitForAsync(
            () => ArtifactsCleanAsync("shard1", "bucket_0", schemaMustBeAbsent: true, ct),
            TimeSpan.FromSeconds(120), ct);
        autoFin.Should().BeTrue($"auto-finalize должен вычистить shard1; " +
                                $"artifacts={await ArtifactsDumpAsync("shard1", "bucket_0", ct)}, work={await WorkDumpAsync(ct)}");
        (await ArtifactsCleanAsync("shard2", "bucket_0", schemaMustBeAbsent: false, ct))
            .Should().BeTrue("у владельца (shard2) схема bucket_0 жива, артефактов нет");
        (await MovesDumpAsync(ct)).Should().Be("нет", "auto-finalize завершён — заявка удалена");
        output.WriteLine("E3: auto-finalize done (shard1 cleaned)");
    }

    // ===== E4: Abort + takeover =====

    private async Task E4_AbortTakeover()
    {
        var ct = TestContext.Current.CancellationToken;
        output.WriteLine("E4: abort bucket_1 mid-SYNCING + kill + takeover...");

        await PutMoveRequestAsync("bucket_1",
            $$"""{"op":"move","to":"shard1","requested_unix":{{NowUnix()}}}""", ct);
        var syncing = await E2eFixture.WaitForAsync(
            async () => (await StatusAsync("bucket_1", ct))?.Contains("SYNCING") == true,
            TimeSpan.FromSeconds(60), ct);
        syncing.Should().BeTrue("переезд bucket_1 должен войти в SYNCING до отмены");

        await PutMoveRequestAsync("bucket_1",
            $$"""{"op":"abort","force":true,"requested_unix":{{NowUnix()}}}""", ct);
        Host!.Kill();
        await Host.DisposeAsync();
        Host = await fixture.StartHostAsync("m3", ct: ct);

        var aborted = await E2eFixture.WaitForAsync(
            async () => await StatusAsync("bucket_1", ct) is null, TimeSpan.FromSeconds(180), ct);
        aborted.Should().BeTrue($"abort должен довестись новым контроллером (takeover); " +
                                $"status={await StatusAsync("bucket_1", ct) ?? "нет"}, work={await WorkDumpAsync(ct)}");

        var shard2 = await MasterInfoAsync("shard2", ct);
        (await ArtifactsCleanAsync("shard1", "bucket_1", schemaMustBeAbsent: true, ct))
            .Should().BeTrue($"после abort на shard1 артефактов bucket_1 нет; " +
                             $"artifacts={await ArtifactsDumpAsync("shard1", "bucket_1", ct)}");
        (await ArtifactsCleanAsync("shard2", "bucket_1", schemaMustBeAbsent: false, ct))
            .Should().BeTrue($"у владельца (shard2) схема bucket_1 жива, артефактов нет; " +
                             $"artifacts={await ArtifactsDumpAsync("shard2", "bucket_1", ct)}");
        (await TryInsertAppAsync(shard2.Port, "bucket_1", ct))
            .Should().BeTrue("после abort бакет ACTIVE: запись владельцем работает (AC6-abort)");
        output.WriteLine("E4: abort+takeover done");
    }

    // ===== E5: Deprovisioning =====

    private async Task E5_Deprovisioning()
    {
        var ct = TestContext.Current.CancellationToken;
        output.WriteLine("E5: deprovisioning with pending move request...");

        await PutMoveRequestAsync("bucket_2",
            $$"""{"op":"move","to":"shard1","requested_unix":{{NowUnix()}}}""", ct);
        await SetToRemoveAsync();

        var deprovisioned = await E2eFixture.WaitForAsync(
            () => DeprovisionedAsync(), TimeSpan.FromSeconds(180), ct);
        deprovisioned.Should().BeTrue("deprovisioning должен убрать кластер целиком");

        (await RangeAsync(MoveNames.MovesPrefix(Cluster)))
            .Should().BeEmpty("заявки /pgworker/moves/mshop/ не переживают кластер (D2, AC8)");

        if (Host is not null)
            await Host.DisposeAsync();
        output.WriteLine("E5: deprovisioning done");
    }

    // ===== Хелперы etcd =====

    private static long NowUnix() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    private async Task<IReadOnlyList<Kv>> RangeAsync(string prefix)
        => (await G.RangeAsync(Endpoint, prefix, TestContext.Current.CancellationToken)).Value;

    private async Task PutMoveRequestAsync(string bucket, string json, CancellationToken ct)
        => (await G.PutAsync(Endpoint, MoveNames.MoveKey(Cluster, bucket), json, null, ct))
            .IsSuccess.Should().BeTrue($"заявка {bucket} должна записаться в etcd");

    private async Task<string?> RoutingAsync(string bucket, CancellationToken ct)
        => (await GetOrNullAsync(MoveNames.RoutingKey(Cluster, bucket)))?.Value;

    private async Task<string?> StatusAsync(string bucket, CancellationToken ct)
        => (await GetOrNullAsync(MoveNames.StatusKey(Cluster, bucket)))?.Value;

    private async Task<bool> RoutingIsAsync(string bucket, string shard, CancellationToken ct)
        => await RoutingAsync(bucket, ct) == shard && await StatusAsync(bucket, ct) is null;

    private async Task<string> WorkDumpAsync(CancellationToken ct)
        => (await GetOrNullAsync($"/pgworker/work/{Cluster}"))?.Value ?? "нет";

    private async Task<string> MovesDumpAsync(CancellationToken ct)
    {
        var kvs = await RangeAsync(MoveNames.MovesPrefix(Cluster));
        return kvs.Count == 0 ? "нет" : string.Join("; ", kvs.Select(k => $"{k.Key.Split('/')[^1]}={k.Value}"));
    }

    // ===== Сид кластера/бакетов =====

    private async Task SeedClusterAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        var config = $$"""
            {"buckets":6,"dbname":"{{Cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}
            """;
        await G.PutAsync(Endpoint, $"/clusters/{Cluster}/config", config, null, ct);
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            await G.PutAsync(Endpoint, $"/clusters/{Cluster}/shards/{shard}/replicas", "2", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{Cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{Cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/service/{Cluster}-{shard}/request_cpu", "2", null, ct);
            await G.PutAsync(Endpoint, $"/service/{Cluster}-{shard}/request_mem", "8Gi", null, ct);
        }

        for (var i = 0; i < 6; i++)
        {
            await G.PutAsync(Endpoint, $"/clusters/{Cluster}/buckets/routing/bucket_{i}", $"shard{i % 2 + 1}", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{Cluster}/buckets/status/bucket_{i}",
                """{"state":"NOT_INITIALIZED"}""", null, ct);
        }
    }

    private static async Task SeedBucketAsync(string adminDsn, string bucket, int rows, CancellationToken ct)
    {
        var ddl = $"""
            CREATE TABLE {bucket}.items(id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, note text NOT NULL);
            INSERT INTO {bucket}.items(note) SELECT 'seed' FROM generate_series(1, {rows});
            GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {bucket} TO app;
            GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA {bucket} TO app;
            GRANT SELECT ON ALL TABLES IN SCHEMA {bucket} TO bucket_mover;
            """;
        // Сид идёт сразу после provision DONE: fresh-проброс порта Docker Desktop
        // может дать разовый RST на SSL-рукопожатии (TCP принимает docker-proxy,
        // PG-контейнер ещё не готов) — детерминизация t09: ждём готовность, а не
        // полагаемся на мгновенный коннект (паттерн O2-probe в E2eScenarios).
        // SQL-ошибки (PostgresException) НЕ ретраим — это валидный провал.
        (await E2eFixture.WaitForAsync(async () =>
        {
            try
            {
                await using var con = new NpgsqlConnection(
                    $"{adminDsn};Timeout=10;SSL Mode=Require;Trust Server Certificate=true");
                await con.OpenAsync(ct);
                await using var cmd = new NpgsqlCommand(ddl, con);
                await cmd.ExecuteNonQueryAsync(ct);
                return true;
            }
            catch (NpgsqlException e) when (e is not PostgresException)
            {
                return false; // PG ещё не готов/разовый сброс транспорта — повторим
            }
        }, TimeSpan.FromSeconds(60), ct))
            .Should().BeTrue($"сид {bucket} должен примениться к мастеру ({adminDsn})");
    }

    private async Task SetToRemoveAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        var current = await GetOrNullAsync($"/clusters/{Cluster}/config");
        current.Should().NotBeNull("конфиг кластера обязан существовать до TO_REMOVE");
        var doc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(current!.Value!)!;
        doc["state"] = JsonSerializer.SerializeToElement("TO_REMOVE");
        await G.PutAsync(Endpoint, $"/clusters/{Cluster}/config", JsonSerializer.Serialize(doc), null, ct);
    }

    private async Task<bool> ProvisionedAsync()
    {
        var config = await GetOrNullAsync($"/clusters/{Cluster}/config");
        if (config is null || JsonSerializer
                .Deserialize<Dictionary<string, JsonElement>>(config.Value)!.ContainsKey("state"))
            return false;
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            if (await GetOrNullAsync($"/clusters/{Cluster}/shards/{shard}/dsn") is null)
                return false;
            foreach (var node in new[] { $"{shard}a", $"{shard}b" })
                if ((await GetOrNullAsync($"/clusters/{Cluster}/shards/{shard}/nodes/{node}/state"))?.Value != "RUNNING")
                    return false;
        }

        return (await RangeAsync($"/clusters/{Cluster}/buckets/status/")).Count == 0;
    }

    private async Task<bool> DeprovisionedAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        var containers = await fixture.RunDockerAsync(
            ["ps", "-aq", "--filter", $"name=pgw-{Cluster}-"], ct);
        if (containers.Length > 0) return false;
        var volumes = await fixture.RunDockerAsync(
            ["volume", "ls", "-q", "--filter", $"name=pgw-{Cluster}-"], ct);
        if (volumes.Length > 0) return false;
        if ((await RangeAsync($"/clusters/{Cluster}/")).Count > 0) return false;
        if ((await RangeAsync($"/service/{Cluster}-")).Count > 0) return false;

        return await GetOrNullAsync($"/pgworker/claims/{Cluster}") is null;
    }

    // ===== Адресация мастеров и SQL-хелперы =====

    private sealed record NodeAddr(string Host, int Pg, int Patroni, int Doorman);

    private async Task<Dictionary<string, NodeAddr>> PortallocAsync()
    {
        var kv = await GetOrNullAsync($"/pgworker/portalloc/{Cluster}");
        if (kv is null) return [];
        return JsonSerializer.Deserialize<Dictionary<string, NodeAddr>>(kv.Value, Json) ?? [];
    }

    private sealed record MasterInfo(string Node, int Port, int PatroniPort, string Dsn);

    // Пробы Patroni из теста (семантика /primary, arch/14 §5 C).
    private static readonly HttpClient PatroniHttp = new() { Timeout = TimeSpan.FromSeconds(3) };

    private async Task<MasterInfo> MasterInfoAsync(string shard, CancellationToken ct)
    {
        // Резолв мастера по контракту §5 C: проба /primary по patroni-портам
        // portalloc. Матч master-ключа по doorman-порту при EnableDoorman=false
        // недискриминантен (ключ host:0 у всех нод, arch/14 §2.4 п.5 — t09):
        // FirstOrDefault по такому матчу отдавал произвольную ноду шарда —
        // seed уходил на реплику (25006 read-only).
        for (var i = 0; i < 60; i++)
        {
            var key = await GetOrNullAsync($"/clusters/{Cluster}/shards/{shard}/master");
            if (key is { Value.Length: > 0 })
            {
                var addresses = await PortallocAsync();
                foreach (var (nodeKey, addr) in addresses
                             .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                             .OrderBy(p => p.Key, StringComparer.Ordinal))
                {
                    try
                    {
                        using var response = await PatroniHttp.GetAsync(
                            $"http://localhost:{addr.Patroni}/primary", ct);
                        if (!response.IsSuccessStatusCode)
                            continue;
                        var node = nodeKey.Split('/')[1];
                        return new MasterInfo(node, addr.Pg, addr.Patroni,
                            $"Host=localhost;Port={addr.Pg};Database={Cluster};Username=postgres;Password={E2eFixture.SuPassword}");
                    }
                    catch (Exception)
                    {
                        // сетевой сбой пробы (рестарт/ещё не готова) — не primary
                    }
                }
            }

            await Task.Delay(1000, ct);
        }
        throw new ApplicationException($"мастер {Cluster}/{shard} не найден за 60 с");
    }

    private async Task EnableSyncModeAsync(string shard, CancellationToken ct)
    {
        var master = await MasterInfoAsync(shard, ct);
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

    // DSN приложения: пароль — из etcd-ключа /clusters/<C>/app_password
    // (тот же путь, что и приложение — spec §4.3), не из env фикстуры.
    private async Task<string> AppDsnAsync(int port, CancellationToken ct)
        => $"Host=localhost;Port={port};Database={Cluster};Username=app;" +
           $"Password={await fixture.GetAppPasswordAsync(Cluster, ct)};SSL Mode=Require;Trust Server Certificate=true";

    private static async Task<string> SqlScalarAsync(string dsn, string sql, CancellationToken ct)
    {
        await using var con = new NpgsqlConnection($"{dsn};Timeout=10;SSL Mode=Require;Trust Server Certificate=true");
        await con.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, con);
        return (await cmd.ExecuteScalarAsync(ct))?.ToString() ?? "";
    }

    private async Task<bool> TryInsertAppAsync(int masterPort, string bucket, CancellationToken ct)
    {
        try
        {
            await using var con = new NpgsqlConnection($"{await AppDsnAsync(masterPort, ct)};Timeout=10;SSL Mode=Require;Trust Server Certificate=true");
            await con.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(
                $"INSERT INTO {bucket}.items(note) VALUES ('probe')", con);
            await cmd.ExecuteNonQueryAsync(ct);
            return true;
        }
        catch (PostgresException) { return false; }
    }

    private sealed record Artifacts(bool SchemaExists, long Pubs, long Subs, long Slots);

    private async Task<Artifacts> ArtifactsAsync(string shard, string bucket, CancellationToken ct)
    {
        var master = await MasterInfoAsync(shard, ct);
        await using var con = new NpgsqlConnection($"{master.Dsn};Timeout=10;SSL Mode=Require;Trust Server Certificate=true");
        await con.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand($"""
            SELECT
              to_regnamespace('{bucket}') IS NOT NULL,
              (SELECT count(*) FROM pg_publication
                 WHERE pubname IN ('{MoveNames.Pub(bucket)}', '{MoveNames.PubRb(bucket)}')),
              (SELECT count(*) FROM pg_subscription
                 WHERE subname IN ('{MoveNames.Sub(bucket)}', '{MoveNames.SubRb(bucket)}')),
              (SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '%{bucket}%')
            """, con);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new Artifacts(reader.GetBoolean(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3));
    }

    private async Task<bool> ArtifactsCleanAsync(
        string shard, string bucket, bool schemaMustBeAbsent, CancellationToken ct)
    {
        var a = await ArtifactsAsync(shard, bucket, ct);
        return a.Pubs == 0 && a.Subs == 0 && a.Slots == 0 && a.SchemaExists != schemaMustBeAbsent;
    }

    private async Task<string> ArtifactsDumpAsync(string shard, string bucket, CancellationToken ct)
    {
        var a = await ArtifactsAsync(shard, bucket, ct);
        return $"schema={a.SchemaExists} pubs={a.Pubs} subs={a.Subs} slots={a.Slots}";
    }

    // ===== Нагрузочный генератор =====

    private sealed class LoadWorker(E2eMoveScenarios owner) : IDisposable
    {
        public sealed record Event(long TsMs, bool Ok, int Port);

        private readonly CancellationTokenSource _cts = new();
        private readonly object _gate = new();
        private readonly List<Event> _events = [];
        private volatile bool _paused;
        private Task _loop = Task.CompletedTask;

        public void Start() => _loop = Task.Run(() => RunAsync(_cts.Token));
        public void Pause() => _paused = true;
        public void Resume() => _paused = false;

        public IReadOnlyList<Event> Snapshot() { lock (_gate) return _events.ToList(); }

        public async Task StopAsync()
        {
            _cts.Cancel();
            try { await _loop; } catch (OperationCanceledException) { }
        }

        private void Record(Event e) { lock (_gate) _events.Add(e); }
        private static long Ts() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        private async Task RunAsync(CancellationToken ct)
        {
            var prevShard = "";
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    if (_paused) { await Task.Delay(100, ct); continue; }

                    try
                    {
                        var shard = await owner.RoutingAsync("bucket_0", ct);
                        if (string.IsNullOrEmpty(shard)) { await Task.Delay(200, ct); continue; }

                        if (shard != prevShard) { await Task.Delay(700, ct); prevShard = shard; }

                        var port = (await owner.MasterInfoAsync(shard, ct)).Port;
                        await using var con = new NpgsqlConnection($"{await owner.AppDsnAsync(port, ct)};Timeout=5;SSL Mode=Require;Trust Server Certificate=true");
                        await con.OpenAsync(ct);
                        await using var cmd = new NpgsqlCommand(
                            "INSERT INTO bucket_0.items(note) VALUES ('load')", con);
                        await cmd.ExecuteNonQueryAsync(ct);
                        Record(new Event(Ts(), true, port));
                    }
                    catch (PostgresException e) when (e.SqlState == "42501")
                        { Record(new Event(Ts(), false, 0)); }
                    catch (Exception) { }

                    await Task.Delay(300, ct);
                }
            }
            catch (OperationCanceledException) { }
        }

        public void Dispose() { _cts.Cancel(); _cts.Dispose(); }
    }
}
