using System.Text;
using System.Text.Json;
using Npgsql;
using PgWorker.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Moves;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E-сценарий переезда бакета (t01 задача 19, spec §10 e2e / §11 AC3–AC6+AC8)
// на живом стенде E2eFixture (etcd + pgworker-node PG16 → FailoverSlots=false,
// R1/Д11): move под нагрузкой с измерением окна FROZEN по timeline
// Ok|FrozenDenied (42501) → sequence-инвариант + counts → призрак-P1 →
// rollback (владелец разморожен) → finalize копии отката (контракт повторного
// move: приёмник без схемы) → повторный move → finalize источника (артефактов
// нет) → abort посреди SYNCING с kill контроллера и takeover → deprovisioning
// кластера с висящей заявкой (чистка /pgworker/moves/<C>/, D2).
[Collection(E2eCollection.Name)]
public class E2eMoveScenarios(E2eFixture fixture)
{
    private const string Cluster = "mshop";

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    private string Endpoint => fixture.EtcdEndpoint;

    private EtcdGateway G => fixture.Gateway;

    [Fact]
    public async Task Move_Lifecycle_UnderLoad_Ghost_Rollback_Refinalize_AbortTakeover_Deprovisioning()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // ---------- Arrange: стенд + сид кластера + запуск контроллера ----------
        await SeedClusterAsync(Cluster);
        await using var p1 = await fixture.StartHostAsync("m1", ct: ct);

        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(Cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning mshop должен дойти до DONE (для move-заявок)");

        // DDL-сид: bucket_0 у владельца shard1 (50 строк — сквозной lifecycle),
        // bucket_1 у владельца shard2 (10 строк — abort-ветка). Гранты app/mover
        // выдаёт сид (provisioning грантовал на пустые схемы).
        var shard1 = await MasterInfoAsync("shard1", ct);
        var shard2 = await MasterInfoAsync("shard2", ct);
        await SeedBucketAsync(shard1.Dsn, "bucket_0", 50, ct);
        await SeedBucketAsync(shard2.Dsn, "bucket_1", 10, ct);

        // P8-префлайт move требует sync-standby у приёмника (Permanent по spec
        // §6.1): включаем synchronous_mode в Patroni обоих шардов (стенд может
        // поднять кластеры без него; PATCH /config — рантайм, only mshop).
        await EnableSyncModeAsync("shard1", ct);
        await EnableSyncModeAsync("shard2", ct);

        // Призрак-P1: сессия app на старом владельце, открытая ДО переезда.
        await using var ghost = new NpgsqlConnection(AppDsn(shard1.Port));
        await ghost.OpenAsync(ct);
        await using (var warm = new NpgsqlCommand("SELECT 1", ghost))
            await warm.ExecuteNonQueryAsync(ct);

        // Нагрузка: INSERT от app в bucket_0.items по текущему routing (ретраи,
        // timeline Ok|FrozenDenied — окно FROZEN измеряется по нему, ревью №7).
        using var load = new LoadWorker(this);
        load.Start();

        // ---------- Act 1: заявка move bucket_0 → shard2 ----------
        await PutMoveRequestAsync("bucket_0",
            $$"""{"op":"move","to":"shard2","requested_unix":{{NowUnix()}}}""", ct);

        // ---------- Assert 1: AC3-move — routing переехал, статус-ключа нет ----------
        var moved = await E2eFixture.WaitForAsync(
            () => RoutingIsAsync("bucket_0", "shard2", ct), TimeSpan.FromSeconds(120), ct);
        moved.Should().BeTrue($"move должен завершиться за 120 с; routing={await RoutingAsync("bucket_0", ct)}, " +
                              $"status={await StatusAsync("bucket_0", ct) ?? "нет"}, заявки={await MovesDumpAsync(ct)}, " +
                              $"репликация={await ReplicationDumpAsync(ct)}, work={await WorkDumpAsync(ct)}");

        // Окно FROZEN (AC3, ревью №7): от первого FrozenDenied (42501) до первого
        // Ok после него ≤ 15 с (FreezeWaitSec=1 + буфер на sequences/counts/flip).
        // События ждём с бюджетом: тест-полл и нагрузка (пауза смены владельца
        // 700 мс — TTL кэша роутера) идут асинхронно друг с другом.
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
            .Should().BeLessOrEqualTo(TimeSpan.FromSeconds(15),
                "окно FROZEN ≤ FreezeWaitSec + несколько секунд (AC3)");
        firstOkAfter.Port.Should().Be(shard2.Port,
            "первая запись после flip идёт через мастера shard2 (нагрузка переключилась на нового владельца)");

        // Сверка данных (AC5): counts источника/приёмника равны — на приостановленной
        // нагрузке (замороженный источник фиксирован, приёмник догнал до flip).
        load.Pause();

        // Обратная подписка (sub_rb, приёмник→источник) асинхронна: последний
        // пост-flip бит мог ещё не доехать до источника — перед сверкой ждём
        // выравнивания (без ожидания приёмник на бит впереди — гонка, P8 цел).
        var aligned = await E2eFixture.WaitForAsync(async () =>
        {
            var c1 = await SqlScalarAsync(shard1.Dsn, "SELECT count(*) FROM bucket_0.items", ct);
            var c2 = await SqlScalarAsync(shard2.Dsn, "SELECT count(*) FROM bucket_0.items", ct);
            return c1 == c2;
        }, TimeSpan.FromSeconds(15), ct);
        aligned.Should().BeTrue("обратная репликация sub_rb должна догнать (counts выровнялись)");

        var count1 = await SqlScalarAsync(shard1.Dsn, "SELECT count(*) FROM bucket_0.items", ct);
        var count2 = await SqlScalarAsync(shard2.Dsn, "SELECT count(*) FROM bucket_0.items", ct);
        count1.Should().Be(count2, "после flip counts всех таблиц источника/приёмника совпадают (P8)");
        long.Parse(count1).Should().BeGreaterThanOrEqualTo(50, "нагрузочные строки не потеряны");

        // Sequence-инвариант (AC4): следующее значение на новом владельце строго
        // больше последнего выданного на старом.
        var issued = long.Parse(await SqlScalarAsync(shard1.Dsn,
            "SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM bucket_0.items_id_seq", ct));
        var nextOnNew = long.Parse(await SqlScalarAsync(shard2.Dsn,
            "SELECT CASE WHEN is_called THEN last_value + 1 ELSE last_value END FROM bucket_0.items_id_seq", ct));
        nextOnNew.Should().BeGreaterThan(issued,
            "sequence-инвариант: следующий выдаваемый id на shard2 строго > последнего выданного на shard1 (P6)");

        // Призрак-P1 (AC3-призрак): запись в сессии, открытой до flip, — 42501.
        PostgresException? ghostError = null;
        try
        {
            await using var cmd = new NpgsqlCommand(
                "INSERT INTO bucket_0.items(note) VALUES ('ghost')", ghost);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException e)
        {
            ghostError = e;
        }
        ghostError.Should().NotBeNull("призрак-P1: write в старой сессии обязан упасть (источник заморожен)");
        ghostError!.SqlState.Should().Be("42501", "заморозка срезает write (permission denied), а не что-то иное");
        load.Resume();

        // ---------- Act 2: rollback → владелец вернулся ----------
        await PutMoveRequestAsync("bucket_0",
            $$"""{"op":"rollback","requested_unix":{{NowUnix()}}}""", ct);

        var rolledBack = await E2eFixture.WaitForAsync(
            () => RoutingIsAsync("bucket_0", "shard1", ct), TimeSpan.FromSeconds(120), ct);
        rolledBack.Should().BeTrue($"rollback должен вернуть владельца за 120 с; заявки={await MovesDumpAsync(ct)}, " +
                                   $"work={await WorkDumpAsync(ct)}");

        // Заморозка снята: запись под app на вернувшемся владельце успешна.
        (await TryInsertAppAsync(shard1.Port, "bucket_0", ct))
            .Should().BeTrue("после rollback владелец разморожен — запись работает (P1)");

        // Finalize копии отката: контракт повторного move требует чистого приёмника
        // (схема на shard2 без подписки и с данными → Permanent в M0), поэтому до
        // повторного переезда остатки бывшего приёмника убирает finalize (AC6).
        await PutMoveRequestAsync("bucket_0",
            $$"""{"op":"finalize","old_shard":"shard2","requested_unix":{{NowUnix()}}}""", ct);
        var finOld = await E2eFixture.WaitForAsync(
            () => ArtifactsCleanAsync("shard2", "bucket_0", schemaMustBeAbsent: true, ct),
            TimeSpan.FromSeconds(120), ct);
        finOld.Should().BeTrue($"finalize копии отката должен убрать схему/артефакты bucket_0 на shard2; " +
                               $"artifacts={await ArtifactsDumpAsync("shard2", "bucket_0", ct)}, work={await WorkDumpAsync(ct)}");

        // ---------- Act 3: повторный move bucket_0 → shard2 ----------
        await PutMoveRequestAsync("bucket_0",
            $$"""{"op":"move","to":"shard2","requested_unix":{{NowUnix()}}}""", ct);

        var movedAgain = await E2eFixture.WaitForAsync(
            () => RoutingIsAsync("bucket_0", "shard2", ct), TimeSpan.FromSeconds(120), ct);
        movedAgain.Should().BeTrue($"повторный move (чистый приёмник) должен пройти; " +
                                   $"routing={await RoutingAsync("bucket_0", ct)}, заявки={await MovesDumpAsync(ct)}, " +
                                   $"work={await WorkDumpAsync(ct)}");

        // Finalize источника (AC3-finalize): на shard1 нет ни схемы, ни pub/sub,
        // ни слотов (AC6-артефакты); на новом владельце схема жива и чист от артефактов.
        await PutMoveRequestAsync("bucket_0",
            $$"""{"op":"finalize","old_shard":"shard1","requested_unix":{{NowUnix()}}}""", ct);
        var finSrc = await E2eFixture.WaitForAsync(
            () => ArtifactsCleanAsync("shard1", "bucket_0", schemaMustBeAbsent: true, ct), 
            TimeSpan.FromSeconds(120), ct);
        finSrc.Should().BeTrue($"finalize источника должен оставить shard1 чистым; " +
                               $"artifacts={await ArtifactsDumpAsync("shard1", "bucket_0", ct)}, work={await WorkDumpAsync(ct)}");
        (await ArtifactsCleanAsync("shard2", "bucket_0", schemaMustBeAbsent: false, ct))
            .Should().BeTrue("у владельца схема bucket_0 жива, артефактов (pub/sub/слоты) нет");

        // ---------- Act 4: abort посреди SYNCING + kill контроллера + takeover ----------
        // bucket_1 (владелец shard2) уезжает на shard1; в SYNCING заявку ПЕРЕЗАПИСЫВАЕМ
        // на abort force (ключ один — контракт «заявка на бакет», spec §4.1, ревью №6).
        await PutMoveRequestAsync("bucket_1",
            $$"""{"op":"move","to":"shard1","requested_unix":{{NowUnix()}}}""", ct);
        var syncing = await E2eFixture.WaitForAsync(
            async () => (await StatusAsync("bucket_1", ct))?.Contains("SYNCING") == true,
            TimeSpan.FromSeconds(60), ct);
        syncing.Should().BeTrue("переезд bucket_1 должен войти в SYNCING до отмены");

        await PutMoveRequestAsync("bucket_1",
            $$"""{"op":"abort","force":true,"requested_unix":{{NowUnix()}}}""", ct);
        p1.Kill(); // смерть контроллера посреди уборки — доводит новый инстанс
        await p1.DisposeAsync();
        await using var p2 = await fixture.StartHostAsync("m2", ct: ct);

        var aborted = await E2eFixture.WaitForAsync(
            async () => await StatusAsync("bucket_1", ct) is null, TimeSpan.FromSeconds(180), ct);
        aborted.Should().BeTrue($"abort должен довестись новым контроллером (takeover); " +
                                $"status={await StatusAsync("bucket_1", ct) ?? "нет"}, work={await WorkDumpAsync(ct)}");

        // AC6-abort: артефактов bucket_1 нет нигде (кроме схемы владельца), запись жива.
        (await ArtifactsCleanAsync("shard1", "bucket_1", schemaMustBeAbsent: true, ct))
            .Should().BeTrue($"после abort на бывшем приёмнике (shard1) схемы и артефактов bucket_1 нет; " +
                             $"artifacts={await ArtifactsDumpAsync("shard1", "bucket_1", ct)}");
        (await ArtifactsCleanAsync("shard2", "bucket_1", schemaMustBeAbsent: false, ct))
            .Should().BeTrue($"у владельца (shard2) схема bucket_1 жива, артефактов нет; " +
                             $"artifacts={await ArtifactsDumpAsync("shard2", "bucket_1", ct)}");
        (await TryInsertAppAsync(shard2.Port, "bucket_1", ct))
            .Should().BeTrue("после abort бакет ACTIVE: запись владельцем работает (AC6-abort)");

        // ---------- Act 5: deprovisioning с висящей заявкой (AC8) ----------
        await PutMoveRequestAsync("bucket_2",
            $$"""{"op":"move","to":"shard1","requested_unix":{{NowUnix()}}}""", ct);
        await SetToRemoveAsync(Cluster);

        var deprovisioned = await E2eFixture.WaitForAsync(
            () => DeprovisionedAsync(Cluster), TimeSpan.FromSeconds(180), ct);
        deprovisioned.Should().BeTrue("deprovisioning должен убрать кластер целиком (контейнеры/volume/ключи)");

        (await RangeAsync(MoveNames.MovesPrefix(Cluster)))
            .Should().BeEmpty("заявки /pgworker/moves/mshop/ не переживают кластер (D2, AC8)");

        // ---------- Cleanup: нагрузка останавливается, хосты dispose (await using) ----------
        await load.StopAsync();
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

    // Дамп work-журнала кластера (диагностика сообщений об ошибках).
    private async Task<string> WorkDumpAsync(CancellationToken ct)
        => (await GetOrNullAsync($"/pgworker/work/{Cluster}"))?.Value ?? "нет";

    // Дамп живых заявок кластера (диагностика: transient — заявка жива).
    private async Task<string> MovesDumpAsync(CancellationToken ct)
    {
        var kvs = await RangeAsync(MoveNames.MovesPrefix(Cluster));
        return kvs.Count == 0 ? "нет" : string.Join("; ", kvs.Select(k => $"{k.Key.Split('/')[^1]}={k.Value}"));
    }

    // Дамп слотов/подписок обоих шардов (диагностика catchup-фаз cutover).
    private async Task<string> ReplicationDumpAsync(CancellationToken ct)
    {
        var parts = new List<string>();
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            try
            {
                var master = await MasterInfoAsync(shard, ct);
                var slots = await SqlScalarAsync(master.Dsn,
                    "SELECT coalesce(string_agg(slot_name || CASE WHEN active THEN ':on' ELSE ':off' END || ':' || coalesce(confirmed_flush_lsn::text, '-'), ', '), 'нет') " +
                    "FROM pg_replication_slots WHERE slot_name NOT LIKE 'shard%'", ct);
                var subs = await SqlScalarAsync(master.Dsn,
                    "SELECT coalesce(string_agg(subname, ', '), 'нет') FROM pg_subscription", ct);
                parts.Add($"{shard}[slots={slots}; subs={subs}]");
            }
            catch (Exception e)
            {
                parts.Add($"{shard}[{e.Message}]");
            }
        }

        return string.Join(" ", parts);
    }

    // ===== Сид кластера/бакетов (копия приёмов E2eScenarios) =====

    private async Task SeedClusterAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var config = $$"""
            {"buckets":6,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}
            """;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", config, null, ct);
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

    // Таблица с identity-sequence (P6) + гранты app/mover на ВЛАДЕЛЬЦЕ (сид;
    // provisioning грантовал по пустые схемы — на новые таблицы не действует).
    private static async Task SeedBucketAsync(string adminDsn, string bucket, int rows, CancellationToken ct)
    {
        var ddl = $"""
            CREATE TABLE {bucket}.items(id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, note text NOT NULL);
            INSERT INTO {bucket}.items(note) SELECT 'seed' FROM generate_series(1, {rows});
            GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {bucket} TO app;
            GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA {bucket} TO app;
            GRANT SELECT ON ALL TABLES IN SCHEMA {bucket} TO bucket_mover;
            """;
        await using var con = new NpgsqlConnection($"{adminDsn};Timeout=10");
        await con.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(ddl, con);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task SetToRemoveAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var current = await GetOrNullAsync($"/clusters/{cluster}/config");
        current.Should().NotBeNull("конфиг кластера обязан существовать до TO_REMOVE");
        var doc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(current!.Value!)!;
        doc["state"] = JsonSerializer.SerializeToElement("TO_REMOVE");
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", JsonSerializer.Serialize(doc), null, ct);
    }

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

    private async Task<bool> DeprovisionedAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        var containers = await fixture.RunDockerAsync(
            ["ps", "-aq", "--filter", $"name=pgw-{cluster}-"], ct);
        if (containers.Length > 0)
            return false;
        var volumes = await fixture.RunDockerAsync(
            ["volume", "ls", "-q", "--filter", $"name=pgw-{cluster}-"], ct);
        if (volumes.Length > 0)
            return false;
        if ((await RangeAsync($"/clusters/{cluster}/")).Count > 0)
            return false;
        if ((await RangeAsync($"/service/{cluster}-")).Count > 0)
            return false;

        return await GetOrNullAsync($"/pgworker/claims/{cluster}") is null;
    }

    // ===== Адресация мастеров и SQL-хелперы =====

    private sealed record NodeAddr(string Host, int Pg, int Patroni, int Doorman);

    private async Task<Dictionary<string, NodeAddr>> PortallocAsync(string cluster)
    {
        var kv = await GetOrNullAsync($"/pgworker/portalloc/{cluster}");
        if (kv is null)
            return [];
        return JsonSerializer.Deserialize<Dictionary<string, NodeAddr>>(kv.Value, Json) ?? [];
    }

    private sealed record MasterInfo(string Node, int Port, int PatroniPort, string Dsn);

    // Мастер шарда: master-ключ host:doorman → порты из portalloc; admin-DSN
    // (postgres) для DDL/проверок, PG-порт для app-подключений нагрузки,
    // Patroni-порт — для PATCH /config.
    private async Task<MasterInfo> MasterInfoAsync(string shard, CancellationToken ct)
    {
        for (var i = 0; i < 60; i++)
        {
            var key = await GetOrNullAsync($"/clusters/{Cluster}/shards/{shard}/master");
            if (key is { Value.Length: > 0 })
            {
                var doorman = key.Value.Split(':')[^1];
                var match = (await PortallocAsync(Cluster))
                    .FirstOrDefault(p => p.Key.StartsWith($"{shard}/") && p.Value.Doorman.ToString() == doorman);
                if (match.Key is { Length: > 0 })
                {
                    var node = match.Key.Split('/')[1];
                    return new MasterInfo(node, match.Value.Pg, match.Value.Patroni,
                        $"Host=localhost;Port={match.Value.Pg};Database={Cluster};Username=postgres;Password={E2eFixture.SuPassword}");
                }
            }

            await Task.Delay(1000, ct);
        }

        throw new ApplicationException($"мастер {Cluster}/{shard} не найден за 60 с");
    }

    // Включение synchronous_mode у Patroni мастера шарда (runtime-PATCH /config;
    // bootstrap.dcs применяется только при создании DCS-кластера) и ожидание,
    // пока мастер получит sync-standby — P8-префлайт move это требует.
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
        synced.Should().BeTrue($"у мастера {shard} должен появиться sync-standby после PATCH; " +
                               $"names='{await SqlScalarAsync(master.Dsn, "SELECT setting FROM pg_settings WHERE name = 'synchronous_standby_names'", ct)}'");
    }

    private static string AppDsn(int port)
        => $"Host=localhost;Port={port};Database={Cluster};Username=app;Password={E2eFixture.AppPassword}";

    private static async Task<string> SqlScalarAsync(string dsn, string sql, CancellationToken ct)
    {
        await using var con = new NpgsqlConnection($"{dsn};Timeout=10");
        await con.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, con);
        return (await cmd.ExecuteScalarAsync(ct))?.ToString() ?? "";
    }

    // Запись под app в бакет: true = прошла (заморозок нет), false = 42501/ошибка.
    private static async Task<bool> TryInsertAppAsync(int masterPort, string bucket, CancellationToken ct)
    {
        try
        {
            await using var con = new NpgsqlConnection($"{AppDsn(masterPort)};Timeout=10");
            await con.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(
                $"INSERT INTO {bucket}.items(note) VALUES ('probe')", con);
            await cmd.ExecuteNonQueryAsync(ct);
            return true;
        }
        catch (PostgresException)
        {
            return false;
        }
    }

    private sealed record Artifacts(bool SchemaExists, long Pubs, long Subs, long Slots);

    // Инвентарь артефактов бакета на шарде: схема + pub/sub по конвенциям + слоты.
    private async Task<Artifacts> ArtifactsAsync(string shard, string bucket, CancellationToken ct)
    {
        var master = await MasterInfoAsync(shard, ct);
        await using var con = new NpgsqlConnection($"{master.Dsn};Timeout=10");
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

    // «Чисто» = pub/sub/слотов нет; схема отсутствует (источник/приёмник после
    // finalize/abort) либо присутствует (владелец — данные не трогаем).
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

    // ===== Нагрузочный генератор (timeline Ok|FrozenDenied) =====

    // Фоновая запись в bucket_0.items от app ПО ТЕКУЩЕМУ ВЛАДЕЛЬЦУ (routing из
    // etcd — семантика роутера). Смена владельца → пауза 700 мс (TTL кэша
    // роутера; за это время контроллер после flip успевает поставить pub_rb).
    // События: Ok(ts, порт мастера) | FrozenDenied (SqlState 42501) — по ним
    // измеряется окно FROZEN.
    private sealed class LoadWorker(E2eMoveScenarios owner) : IDisposable
    {
        public sealed record Event(long TsMs, bool Ok, int Port);

        private readonly CancellationTokenSource _cts = new();
        private readonly object _gate = new();
        private readonly List<Event> _events = [];
        private volatile bool _paused;
        private Task _loop = Task.CompletedTask;

        public void Start()
            => _loop = Task.Run(() => RunAsync(_cts.Token));

        public void Pause() => _paused = true;

        public void Resume() => _paused = false;

        public IReadOnlyList<Event> Snapshot()
        {
            lock (_gate)
                return _events.ToList();
        }

        public async Task StopAsync()
        {
            _cts.Cancel();
            try
            {
                await _loop;
            }
            catch (OperationCanceledException)
            {
                // штатная остановка
            }
        }

        private void Record(Event e)
        {
            lock (_gate)
                _events.Add(e);
        }

        private static long Ts() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        private async Task RunAsync(CancellationToken ct)
        {
            var prevShard = "";
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    if (_paused)
                    {
                        await Task.Delay(100, ct);
                        continue;
                    }

                    try
                    {
                        var shard = await owner.RoutingAsync("bucket_0", ct);
                        if (string.IsNullOrEmpty(shard))
                        {
                            await Task.Delay(200, ct);
                            continue;
                        }

                        if (shard != prevShard)
                        {
                            // имитация TTL кэша роутера при смене владельца
                            await Task.Delay(700, ct);
                            prevShard = shard;
                        }

                        var port = (await owner.MasterInfoAsync(shard, ct)).Port;
                        await using var con = new NpgsqlConnection($"{AppDsn(port)};Timeout=5");
                        await con.OpenAsync(ct);
                        await using var cmd = new NpgsqlCommand(
                            "INSERT INTO bucket_0.items(note) VALUES ('load')", con);
                        await cmd.ExecuteNonQueryAsync(ct);
                        Record(new Event(Ts(), true, port));
                    }
                    catch (PostgresException e) when (e.SqlState == "42501")
                    {
                        Record(new Event(Ts(), false, 0));
                    }
                    catch (Exception)
                    {
                        // мастер недоступен/сменяется — ретрай без события
                    }

                    await Task.Delay(300, ct);
                }
            }
            catch (OperationCanceledException)
            {
                // остановка
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            _cts.Dispose();
        }
    }
}
