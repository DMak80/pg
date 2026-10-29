using System.Text;
using System.Text.Json;
using Npgsql;
using Shared.Etcd.Client;
using PgWorker.Moves;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E-сценарии масштабирования шардов (t06 spec §8-1..5, критерии §9.2–9.4) на
// живом стенде (изолированное окружение E2eEnvironment — своя сеть/etcd на Fact):
// add ПУСТОГО шарда в Active-кластер (routing/schema-мир не тронут — главная
// граница §2.1), G3-блокировка remove с бакетами, авто-демонтаж после явных
// переездов, освобождение имени, takeover посреди A3.
public sealed class E2eScaleScenarios(E2eScaleScenariosFx Ctx) : IClassFixture<E2eScaleScenariosFx>
{
    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    // Окружение класса (класс-фикстура): факты последовательны и делят его;
    // имена кластеров фактов различны литеральными префиксами (sshop/stshop).
    private E2eEnvironment Fx => Ctx.Fx;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    [Fact]
    public async Task Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused()
    {
        var ct = TestContext.Current.CancellationToken;

        // Уникальное имя кластера на прогон: движковые контейнеры/тома (pgw-<C>-*)
        // опознаются teardown'ом окружения по своему тегу (OwnName) и снимаются им.
        var cluster = $"sshop{Fx.ClusterTag}";

        try
        {
            // ---------- §8-1: add-shard в живой кластер — шард поднят и ПУСТ ----------
            // Arrange: сид (NOT_INITIALIZED) → контроллер → provisioning до Active.
            await SeedClusterAsync(cluster);
            await using var p1 = await Fx.StartHostAsync("s1", ct: ct);

            var provisioned = await E2eFixture.WaitForAsync(
                E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster, () => ProvisionedAsync(cluster)), TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("provisioning of the cluster must reach Active (dsn/RUNNING/no status)");

            // DDL-сид: bucket_0 у shard1 и bucket_1/bucket_3 у shard2 (INSERT-пробы
            // живости записи до/после scale-операций; гранты app выдаёт сид).
            var m1 = await MasterInfoAsync(cluster, "shard1", ct);
            var m2 = await MasterInfoAsync(cluster, "shard2", ct);
            await SeedBucketAsync(m1.Dsn, "bucket_0", ct);
            await SeedBucketAsync(m2.Dsn, "bucket_1", ct);
            await SeedBucketAsync(m2.Dsn, "bucket_3", ct);
            (await TryInsertAppAsync(cluster, "shard1", "bucket_0", ct))
                .Should().BeTrue("writing to bucket_0 (the shard1 owner) works before add-shard");

            // Снапшот routing ДО (главный ассерт границы §2.1).
            var routingBefore = await RoutingSnapshotAsync(cluster, ct);

            // Act: сид add-декларации shard3 В СТИЛЕ ПАНЕЛИ (§6.1).
            await SeedAddDeclarationAsync(cluster, "shard3", ct);

            // Assert: шард поднят и зарегистрирован; контейнеры живы.
            var added = await E2eFixture.WaitForAsync(() => ShardRegisteredAsync(cluster, "shard3"),
                TimeSpan.FromSeconds(360), ct);
            added.Should().BeTrue($"AddShardProcess must raise shard3 (dsn/RUNNING); work={await WorkDumpAsync(cluster, ct)}");

            var containers = await ListContainerNamesAsync($"pgw-{cluster}-shard3-");
            containers.Should().BeEquivalentTo([$"pgw-{cluster}-shard3-shard3a", $"pgw-{cluster}-shard3-shard3b"],
                "the containers of the new shard are raised");

            // ГЛАВНЫЙ ассерт границы §2.1: routing/status/schema-мир не изменён НИКАК.
            (await RoutingSnapshotAsync(cluster, ct)).Should().Equal(routingBefore,
                "add-shard does not move any bucket (routing unchanged)");
            (await RangeAsync($"/clusters/{cluster}/buckets/status/")).Should().BeEmpty("no status keys appeared");
            var m3 = await MasterInfoAsync(cluster, "shard3", ct);
            var schemas = await SqlScalarAsync(m3.Dsn,
                "SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'bucket_%'", ct);
            schemas.Should().Be("0", "the shard starts EMPTY — it has no bucket schemas (§2.1)");
            (await TryInsertAppAsync(cluster, "shard1", "bucket_0", ct))
                .Should().BeTrue("writing to the existing buckets was not interrupted");

            // ---------- §8-2: remove шарда с бакетами заблокирован G3 ----------
            // Arrange: sync-режим приёмников (P8-префлайт move), переезд bucket_1 → shard3.
            await EnableSyncModeAsync(cluster, "shard3", ct);
            await PutMoveRequestAsync(cluster, "bucket_1",
                $$"""{"op":"move","to":"shard3","requested_unix":{{NowUnix()}}}""", ct);
            var moved = await E2eFixture.WaitForAsync(
                () => RoutingIsAsync(cluster, "bucket_1", "shard3", ct), TimeSpan.FromSeconds(120), ct);
            moved.Should().BeTrue($"move bucket_1 → shard3 must finish; work={await WorkDumpAsync(cluster, ct)}");

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
                        && error.GetString()?.Contains("bucket", StringComparison.Ordinal) == true;
                }
                catch (JsonException)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(60), ct);
            blocked.Should().BeTrue($"the G3 guard must record the reason with the bucket count; work={await WorkDumpAsync(cluster, ct)}");
            (await ListContainerNamesAsync($"pgw-{cluster}-shard1-")).Should().HaveCount(2,
                "the containers of the marked shard stay alive while it has buckets");
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
            allMoved.Should().BeTrue($"all buckets of shard1 must move away; work={await WorkDumpAsync(cluster, ct)}, " +
                                     $"requests={await MovesDumpAsync(cluster, ct)}");

            foreach (var bucket in new[] { "bucket_0", "bucket_2", "bucket_4" })
                await PutMoveRequestAsync(cluster, bucket,
                    $$"""{"op":"finalize","old_shard":"shard1","requested_unix":{{NowUnix()}}}""", ct);

            var finalized = await E2eFixture.WaitForAsync(
                () => MovesEmptyAsync(cluster, ct), TimeSpan.FromSeconds(240), ct);
            finalized.Should().BeTrue($"the finalize requests must be processed; requests={await MovesDumpAsync(cluster, ct)}, " +
                                      $"work={await WorkDumpAsync(cluster, ct)}");

            // Assert: демонтаж дошёл сам (маркер не повторяли): контейнеры/volumes
            // удалены, ключи/порталы вычищены, кластер продолжает обслуживать запись.
            var dismantled = await E2eFixture.WaitForAsync(
                () => ShardDismantledAsync(cluster, "shard1", ct), TimeSpan.FromSeconds(180), ct);
            dismantled.Should().BeTrue($"after the last bucket leaves, the dismantling completes on its own; " +
                                       $"work={await WorkDumpAsync(cluster, ct)}, containers={string.Join(",", await ListContainerNamesAsync($"pgw-{cluster}-shard1-", all: true))}");

            (await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/dsn")).Should().BeNull("no keys of the dismantled shard");
            (await RangeAsync($"/clusters/{cluster}/shards/shard1/")).Should().BeEmpty();
            (await RangeAsync($"/service/{cluster}-shard1/")).Should().BeEmpty();
            var portalloc = (await GetOrNullAsync($"/pgworker/portalloc/{cluster}"))!.Value;
            portalloc.Should().NotContain("shard1/").And.Contain("shard2/").And.Contain("shard3/",
                "portalloc is cleaned up pointwise — the records of the other shards are alive");

            (await TryInsertAppAsync(cluster, "shard3", "bucket_1", ct))
                .Should().BeTrue("the cluster is alive: writing to a bucket on shard3 succeeds after the dismantling");
            (await TryInsertAppAsync(cluster, "shard2", "bucket_3", ct))
                .Should().BeTrue("the cluster is alive: writing to a bucket on shard2 succeeds after the dismantling");

            // ---------- §8-5: имя освобождается демонтажом ----------
            // Act: add-декларация с именем shard1 НАПРЯМУЮ в etcd (в обход автогенерации).
            await SeedAddDeclarationAsync(cluster, "shard1", ct);

            // Assert: AddShardProcess принял освобождённое имя (клэйм-инвариант §4.2).
            var reused = await E2eFixture.WaitForAsync(() => ShardRegisteredAsync(cluster, "shard1"),
                TimeSpan.FromSeconds(360), ct);
            reused.Should().BeTrue($"the shard1 name is freed by the dismantling and accepted anew; work={await WorkDumpAsync(cluster, ct)}");
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — окружение ОСТАНОВИТЬ, не
            // удалить (телеметрия в артефактах + живые объекты для разбора).
            Fx.MarkFailed();
            throw;
        }
    }

    [Fact]
    public async Task Scale_TakeoverMidAdd_SecondInstanceFinishesNoDuplicates()
    {
        var ct = TestContext.Current.CancellationToken;

        // Уникальное имя кластера на прогон (см. scale-add).
        var cluster = $"stshop{Fx.ClusterTag}";

        try
        {
            // ---------- §8-4: takeover посреди A3 ----------
            // Arrange: живой кластер (provisioned первым инстансом), затем
            // add-декларация shard3; ждём ПЕРВЫЙ контейнер нового шарда (A3 начался).
            await SeedClusterAsync(cluster);
            await using var s2 = await Fx.StartHostAsync("s2", ct: ct);

            var provisioned = await E2eFixture.WaitForAsync(
                E2eEnvironment.ProvisionStuckFailFast(Fx, GetOrNullAsync, cluster, () => ProvisionedAsync(cluster)), TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("provisioning of the cluster must reach Active before add starts");

            await SeedAddDeclarationAsync(cluster, "shard3", ct);
            var a3Started = await E2eFixture.WaitForAsync(
                () => DockerHasAsync($"pgw-{cluster}-shard3-"), TimeSpan.FromSeconds(120), ct);
            a3Started.Should().BeTrue("the first instance must start A3 (the shard3 container appeared)");

            // Act: docker-kill PgWorker посреди A3 → второй инстанс доносит шард.
            s2.Kill();
            await s2.DisposeAsync();
            await using var s3 = await Fx.StartHostAsync("s3", ct: ct);

            // Assert: шард донесён (клэйм истёк ≤15 с), дублей контейнеров нет.
            var finished = await E2eFixture.WaitForAsync(() => ShardRegisteredAsync(cluster, "shard3"),
                TimeSpan.FromSeconds(360), ct);
            finished.Should().BeTrue($"the second instance must carry shard3 after the takeover; work={await WorkDumpAsync(cluster, ct)}");

            var containers = await ListContainerNamesAsync($"pgw-{cluster}-shard3-", all: true);
            containers.Should().HaveCount(2, "exactly 2 containers of the new shard (no duplicates after the takeover)");
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — окружение ОСТАНОВИТЬ, не
            // удалить (телеметрия в артефактах + живые объекты для разбора).
            Fx.MarkFailed();
            throw;
        }
    }

    // ===== Хелперы (приёмы E2eScenarios/E2eMoveScenarios, scoped на кластер) =====

    private static long NowUnix() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    private async Task<IReadOnlyList<Kv>> RangeAsync(string prefix)
        => (await G.RangeAsync(Endpoint, prefix, TestContext.Current.CancellationToken)).Value;

    private async Task PutMoveRequestAsync(string cluster, string bucket, string json, CancellationToken ct)
        => (await G.PutAsync(Endpoint, MoveNames.MoveKey(cluster, bucket), json, null, ct))
            .IsSuccess.Should().BeTrue($"the request {bucket} must be written to etcd");

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
        => (await GetOrNullAsync($"/pgworker/work/{cluster}"))?.Value ?? "absent";

    private async Task<string> MovesDumpAsync(string cluster, CancellationToken ct)
    {
        var kvs = await RangeAsync(MoveNames.MovesPrefix(cluster));
        return kvs.Count == 0 ? "absent" : string.Join("; ", kvs.Select(k => $"{k.Key.Split('/')[^1]}={k.Value}"));
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
        var volumes = await Fx.RunDockerAsync(
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
            await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "2Gi", null, ct);
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
        await G.PutAsync(Endpoint, $"/service/{cluster}-{shard}/request_mem", "2Gi", null, ct);
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
        // Детерминизация (t09, тот же дефект-класс, что E1-seed в Move): fresh-
        // проброс порта Docker Desktop даёт разовый RST на SSL-рукопожатии
        // (TCP принят docker-proxy, PG-контейнер ещё не готов) — ждём готовность
        // ретраем, SQL-ошибки (PostgresException) не ретраим — валидный провал.
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
                return false; // PG ещё не готов / разовый сброс транспорта — повторим
            }
        }, TimeSpan.FromSeconds(60), ct))
            .Should().BeTrue($"the seed {bucket} must be applied to the master ({adminDsn})");
    }

    private sealed record NodeAddr(string Host, int Pg, int Patroni, int Doorman);

    private async Task<Dictionary<string, NodeAddr>> PortallocAsync(string cluster)
    {
        var kv = await GetOrNullAsync($"/pgworker/portalloc/{cluster}");
        if (kv is null)
            return [];
        return JsonSerializer.Deserialize<Dictionary<string, NodeAddr>>(kv.Value, Json) ?? [];
    }

    // Host — SQL-адрес мастера с хоста (localhost); Patroni — REST-порт
    // публикации (PATCH /config синхрон-режима).
    private sealed record MasterInfo(string Node, string Host, int Patroni, string Dsn);

    // t22: Patroni REST нод — https (цепочка к per-contour CA, без hostname)
    private HttpClient PatroniHttp => Fx.NewPatroniHttpClient();

    // Мастер шарда: резолв по контракту §5 C — проба /primary по patroni-портам
    // portalloc. Матч master-ключа по doorman-порту при EnableDoorman=false
    // недискриминантен (ключ host:0 у всех нод, arch/14 §2.4 п.5 — t09):
    // FirstOrDefault по такому матчу отдавал произвольную ноду шарда — seed
    // уходил на реплику (25006 read-only). Приём E2eMoveScenarios.
    private async Task<MasterInfo> MasterInfoAsync(string cluster, string shard, CancellationToken ct)
    {
        for (var i = 0; i < 60; i++)
        {
            var key = await GetOrNullAsync($"/clusters/{cluster}/shards/{shard}/master");
            if (i > 0 && i % 15 == 0)
            {
                // Диагностика ожидания мастер-ключа: что вообще есть в etcd
                // (кластерные ключи + DCS-лок Patroni) без перезапуска прогона.
                var dump = (await G.RangeAsync(Endpoint, $"/clusters/{cluster}/", ct)).Value;
                var dcs = (await G.RangeAsync(Endpoint, "/service/", ct)).Value;
                var work = (await G.RangeAsync(Endpoint, "/pgworker/work/", ct)).Value;
                var line =
                    $"[diag] master-{shard}: attempt {i}: /clusters/{cluster}/* = {dump.Count}"
                    + $" [{string.Join(", ", dump.Select(kv => kv.Key).Take(12))}];"
                    + $" /service/* = {dcs.Count} [{string.Join(", ", dcs.Select(kv => kv.Key).Take(8))}];"
                    + $" work = [{string.Join("; ", work.Select(kv => kv.Value[..Math.Min(120, kv.Value.Length)]))}]";
                // Файловый канал: Console.Error раннер проглатывает (xUnit v3).
                await File.AppendAllTextAsync(
                    Path.Combine(Fx.ArtifactsDir, "diag.log"), line + "\n", ct);
            }

            if (key is { Value.Length: > 0 })
            {
                var addresses = await PortallocAsync(cluster);
                foreach (var (nodeKey, addr) in addresses
                             .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                             .OrderBy(p => p.Key, StringComparer.Ordinal))
                {
                    var node = nodeKey.Split('/')[1];
                    try
                    {
                        var host = $"pgw-{cluster}-{shard}-{node}";
                        // Изолированный клиент на пробу (диагностика общего
                        // PatroniHttp: ObjectDisposedException в §8-3).
                        // Публикация REST ноды на хост: localhost:{динамический порт}.
                        using var http = Fx.NewPatroniHttpClient();
                        using var response = await http.GetAsync(
                            $"https://localhost:{addr.Patroni}/primary", ct);
                        await File.AppendAllTextAsync(
                            Path.Combine(Fx.ArtifactsDir, "diag.log"),
                            $"[diag] probe {node}: {(int)response.StatusCode}\n", ct);
                        if (!response.IsSuccessStatusCode)
                            continue;
                        return new MasterInfo(node, "localhost", addr.Patroni,
                            $"Host=localhost;Port={addr.Pg};Database={cluster};Username=postgres;Password={E2eFixture.SuPassword}");
                    }
                    catch (Exception e)
                    {
                        await File.AppendAllTextAsync(
                            Path.Combine(Fx.ArtifactsDir, "diag.log"),
                            $"[diag] probe {node}: EXC {e}\n", ct);
                        // сетевой сбой пробы (рестарт/ещё не готова) — не primary
                    }
                }
            }

            await Task.Delay(1000, ct);
        }

        throw new ApplicationException($"master {cluster}/{shard} not found within 60 s");
    }

    // Включение synchronous_mode у Patroni мастера шарда (P8-префлайт move).
    // PATCH /config ретрится (транспорт/таймаут/5xx/409 Patroni под нагрузкой
    // серии — transient, мастер перечитывается перед попыткой; 4xx — не
    // transient, падаем сразу). Гейт sync-standby читает АКТУАЛЬНОГО мастера
    // на каждом такте: лидерсмен между PATCH и чтением уводит
    // synchronous_standby_names на бывшего мастера (реплика несёт пустое
    // значение), чтение с фиксированной ноды давало ложный фейл. При неудаче
    // гейта — дамп DCS-config, Patroni /cluster и pg_stat_replication в
    // diag.log (телеметрия e2e-launch: «что произошло» — из лога прогона).
    private async Task EnableSyncModeAsync(string cluster, string shard, CancellationToken ct)
    {
        var restPassword = (await G.GetAsync(Endpoint, $"/clusters/{cluster}/rest_password", ct))
            .Value!.Value;
        var patched = false;
        string patchError = "not attempted";
        for (var attempt = 1; attempt <= 3 && !patched; attempt++)
        {
            // t22: мутация Patroni — https с доверием CA контура + basic-auth
            // per-cluster (unsafe-эндпоинт без Authorization закрыт 401).
            var master = await MasterInfoAsync(cluster, shard, ct);
            try
            {
                using var http = Fx.NewPatroniHttpClient();
                using var response = await http.SendAsync(
                    E2eEnvironment.PatroniPatch($"https://localhost:{master.Patroni}/config",
                        restPassword, """{"synchronous_mode":true}"""),
                    ct);
                if (response.IsSuccessStatusCode)
                {
                    patched = true;
                    break;
                }

                patchError = $"HTTP {(int)response.StatusCode}";
                if ((int)response.StatusCode < 500 && (int)response.StatusCode != 409)
                    break; // 4xx — не transient: ретрай бессмыслен
            }
            catch (Exception e) when (e is HttpRequestException or TaskCanceledException)
            {
                patchError = e.Message; // транспорт/таймаут клиента (3 с) — transient
            }

            await Task.Delay(2000, ct);
        }

        patched.Should().BeTrue(
            $"Patroni {shard} must accept the PATCH /config (last error: {patchError})");

        var synced = await E2eFixture.WaitForAsync(async () =>
        {
            var current = await PrimaryOrNullAsync(cluster, shard, ct);
            if (current is null)
                return false; // лидерсмен/рестарт Patroni — такт повторит
            try
            {
                var names = await SqlScalarAsync(current.Dsn,
                    "SELECT setting FROM pg_settings WHERE name = 'synchronous_standby_names'", ct);
                var count = await SqlScalarAsync(current.Dsn,
                    "SELECT count(*) FROM pg_stat_replication WHERE sync_state IN ('sync','quorum')", ct);
                return !string.IsNullOrWhiteSpace(names) && long.Parse(count) >= 1;
            }
            catch (NpgsqlException)
            {
                return false; // перезапуск/перенос мастера — такт повторит
            }
        }, TimeSpan.FromSeconds(60), ct);
        if (!synced)
            await DumpSyncGateDiagnosticsAsync(cluster, shard, restPassword, ct);
        synced.Should().BeTrue($"the master {shard} must have a sync-standby after the PATCH");
    }

    // АКТУАЛЬНЫЙ мастер шарда на пробу (легковесно, без ожидания master-ключа):
    // /primary на patroni-портах нод из portalloc; null — ни одна не отвечает
    // как primary (транзит лидерсмена — такт гейта повторит).
    private async Task<MasterInfo?> PrimaryOrNullAsync(string cluster, string shard, CancellationToken ct)
    {
        var addresses = await PortallocAsync(cluster);
        foreach (var (nodeKey, addr) in addresses
                     .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                     .OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            var node = nodeKey.Split('/')[1];
            try
            {
                // Изолированный клиент на пробу (диагностика общего
                // PatroniHttp: ObjectDisposedException в §8-3).
                // Публикация REST ноды на хост: localhost:{динамический порт}.
                using var http = Fx.NewPatroniHttpClient();
                using var response = await http.GetAsync(
                    $"https://localhost:{addr.Patroni}/primary", ct);
                if (!response.IsSuccessStatusCode)
                    continue;
                return new MasterInfo(node, "localhost", addr.Patroni,
                    $"Host=localhost;Port={addr.Pg};Database={cluster};Username=postgres;Password={E2eFixture.SuPassword}");
            }
            catch (Exception)
            {
                // сетевой сбой пробы (рестарт/ещё не готова) — не primary
            }
        }

        return null;
    }

    // Телеметрия фейла гейта sync-standby (без перезапуска прогона): DCS-config
    // Patroni (значение synchronous_mode в DCS), /cluster каждой ноды (роли,
    // sync_standby) и pg_stat_replication с обоих узлов — в diag.log.
    private async Task DumpSyncGateDiagnosticsAsync(
        string cluster, string shard, string restPassword, CancellationToken ct)
    {
        var lines = new StringBuilder();
        try
        {
            var dcs = await GetOrNullAsync($"/service/{cluster}-{shard}/config");
            lines.AppendLine($"[diag] sync-gate {cluster}/{shard}: DCS config = {dcs?.Value ?? "absent"}");
            var addresses = await PortallocAsync(cluster);
            foreach (var (nodeKey, addr) in addresses
                         .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                         .OrderBy(p => p.Key, StringComparer.Ordinal))
            {
                var node = nodeKey.Split('/')[1];
                try
                {
                    using var http = Fx.NewPatroniHttpClient();
                    using var request = new HttpRequestMessage(
                        HttpMethod.Get, $"https://localhost:{addr.Patroni}/cluster");
                    request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
                        "Basic", Convert.ToBase64String(
                            Encoding.UTF8.GetBytes($"patroni:{restPassword}")));
                    using var response = await http.SendAsync(request, ct);
                    var body = (await response.Content.ReadAsStringAsync(ct)).Trim();
                    lines.AppendLine($"[diag] sync-gate {cluster}/{shard}: /cluster {node} → "
                                     + $"HTTP {(int)response.StatusCode}: {body}");
                }
                catch (Exception e)
                {
                    lines.AppendLine($"[diag] sync-gate {cluster}/{shard}: /cluster {node}: EXC {e.Message}");
                }

                try
                {
                    var dsn = $"Host=localhost;Port={addr.Pg};Database={cluster};" +
                              $"Username=postgres;Password={E2eFixture.SuPassword}";
                    var names = await SqlScalarAsync(dsn,
                        "SELECT setting FROM pg_settings WHERE name = 'synchronous_standby_names'", ct);
                    var repl = await SqlScalarAsync(dsn,
                        "SELECT coalesce(string_agg(application_name || ':' || state || ':' || sync_state, '; '), 'none') "
                        + "FROM pg_stat_replication", ct);
                    lines.AppendLine($"[diag] sync-gate {cluster}/{shard}: SQL {node}: "
                                     + $"sync_names='{names}'; repl=[{repl}]");
                }
                catch (Exception e)
                {
                    lines.AppendLine($"[diag] sync-gate {cluster}/{shard}: SQL {node}: EXC {e.Message}");
                }
            }
        }
        catch (Exception e)
        {
            lines.AppendLine($"[diag] sync-gate {cluster}/{shard}: dump failed: {e}");
        }

        // Файловый канал: Console.Error раннер проглатывает (xUnit v3).
        await File.AppendAllTextAsync(Path.Combine(Fx.ArtifactsDir, "diag.log"), lines.ToString(), ct);
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
            // Порт мастера — из portalloc-записи (внутри Dsn-строки MasterInfo);
            // SQL-подключение с хоста — localhost:{динамический pg-порт}.
            await using var con = new NpgsqlConnection(
                $"{master.Dsn};Username=app;" +
                $"Password={await Fx.GetAppPasswordAsync(cluster, ct)};Timeout=10;SSL Mode=Require;Trust Server Certificate=true");
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
        var output = await Fx.RunDockerAsync([.. args], ct);
        return output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
    }
}

/// <summary>Компаньон-фикстура окружения класса (полный контур на класс:
/// сеть/etcd/CA/окно портов; docs/e2e-isolation.md §1).</summary>
public sealed class E2eScaleScenariosFx : E2eContourFixture
{
    protected override string Slug => "scale-add";
}
