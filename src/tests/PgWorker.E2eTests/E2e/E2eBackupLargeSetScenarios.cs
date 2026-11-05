using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using PgWorker.Provisioning.Sql;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// Профиль «большой набор» (spec t01 §4 фаза 6, §7): полный бэкап ≥3 ГиБ
// ОДНИМ объектом и БЕЗ второй копии на диске ноды-источника. Замер ЗАНЯТОСТИ
// data-volume (du -sb PGDATA — содержимое тома; df в E2E видит общий диск VM,
// куда пишется и сам S3-объект) ДО и ВО ВРЕМЯ полного: прирост обязан быть
// меньше 10% размера набора (staging-копия съела бы ~100%). Изолированное
// окружение E2eContourFixture (своя сеть/etcd/ownS3/CA/окно портов); имя
// кластера {slug}{Fx.ClusterTag} — уникально на прогон, движковые
// контейнеры/тома pgw-*-<C>-* опознаются teardown'ом (arch/testing/02-e2e-isolation.md
// §1). Каждый Fact — свой кластер (окружение класса общее): кластеры Facts не
// пересекаются.
public sealed class E2eBackupLargeSetScenarios(E2eBackupLargeSetScenariosFx Ctx)
    : IClassFixture<E2eBackupLargeSetScenariosFx>
{
    private E2eEnvironment Fx => Ctx.Fx;

    private const string Bucket = "pgworker-backups";

    // t36: пул E2E (однонодовый — ownS3-нода окружения).
    private const string Pool = "bkpool";

    // Засев: 3400 строк по 1 МиБ (TOAST STORAGE EXTERNAL — без сжатия)
    // = 3,32 ГиБ физического объёма; порог профиля — 3 ГиБ.
    private const int SeedRows = 3400;
    private const int SeedBatchRows = 400;
    private const long MinSetBytes = 3L * 1024 * 1024 * 1024;

    // «Свободное место упало меньше, чем на 10% размера набора» (spec §7).
    private const int NoSecondCopyPercent = 10;

    // Потолок одного окна ожидания: приказ владельца — неверное ожидание не
    // может длиться дольше 3 мин. Тяжёлый профиль разбит на секции по 3 мин
    // (образец — E2eBackupVerifyScenarios, мульти-CTS).
    private static readonly TimeSpan SectionCeiling = TimeSpan.FromMinutes(3);

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // AAA: полный 3,32 ГиБ не создаёт второй копии на диске ноды-источника:
    // свободное место её data-volume падает меньше чем на 10% размера набора,
    // в S3 — ровно один объект full/<id>.tar.
    [Fact]
    public Task FullBackup_NoSecondCopyOnDisk() => RunScenarioAsync(async _ =>
    {
        // Arrange — контур: воркер бэкапов + кластер + засев ≥3 ГиБ.
        var ctAll = TestContext.Current.CancellationToken;
        var cluster = $"bklarge{Fx.ClusterTag}";
        await using var app = await StartBackupHostAsync("bklarge", ctAll);
        var setup = await ArrangeLargeAsync(cluster, ctAll);

        // Act — полный: поллим ЗАНЯТОСТЬ data-volume нод шарда, пока не появится
        // COMPLETED (максимум по каждому контейнеру; ниже берётся контейнер
        // ноды-источника из статуса).
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ctAll);
        cts.CancelAfter(SectionCeiling);
        var ct = cts.Token;
        await Fx.SeedS3PoolAsync(Pool, ct);
        await PutPolicyAsync(cluster, ct);
        var (completed, maxUsed, maxTmp) =
            await WaitCompletedSamplingUsedAsync(cluster, setup.Containers, ct);

        // Assert 1 — полный завершён, набор ≥3 ГиБ, объект один.
        completed.Should().NotBeNull("the large full must reach COMPLETED within the window");
        var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(completed!.Value)!;
        status["state"].GetString().Should().Be("COMPLETED");
        var sizeBytes = status["size_bytes"].GetInt64();
        sizeBytes.Should().BeGreaterThanOrEqualTo(MinSetBytes, "the seeded set is ≥3 GiB (profile)");
        status["sha256"].GetString().Should().NotBeNullOrEmpty("the etcd status carries the object sha256");
        var id = completed.Key.Split('/').Last();
        var listing = await McLsAsync($"{cluster}/shard1/full/");
        listing.Split('\n', StringSplitOptions.RemoveEmptyEntries).Should().ContainSingle(
            "the full backup is a single tar object");

        // Assert 2 — ЗАНЯТОСТЬ data-volume ноды-ИСТОЧНИКА за время полного выросла
        // меньше чем на 10% размера набора (второй копии на диске нет).
        var node = status["node"].GetString()!;
        var source = setup.Containers.FirstOrDefault(
                c => c.EndsWith($"-{node}", StringComparison.Ordinal))
            ?? throw new ApplicationException(
                $"source node container not found: node={node}, containers=[{string.Join(",", setup.Containers)}]");
        var growth = maxUsed[source] - setup.BaselineUsed[source];
        Console.WriteLine($"[PHASE] large-set-disk: node={node} baseline={setup.BaselineUsed[source]} "
                          + $"max={maxUsed[source]} growth={growth} size={sizeBytes} object={id}.tar");
        growth.Should().BeLessThan(sizeBytes / NoSecondCopyPercent,
            "the full must not create a second copy of the set on the node's data volume");
        // Вторая копия в /tmp (writable-слой контейнера) тоже недопустима:
        // допускаем только служебные мегабайты (CA-файлы/логи).
        var tmpGrowth = maxTmp[source];
        Console.WriteLine($"[PHASE] large-set-tmp: node={node} max=/tmp {tmpGrowth}");
        tmpGrowth.Should().BeLessThan(256L * 1024 * 1024,
            "the streaming full must not stage a second copy in the container's /tmp");
    }, TestContext.Current.CancellationToken);

    // AAA: набор 3,32 ГиБ лежит в S3 РОВНО одним объектом full/<id>.tar;
    // on_create verify OK; etcd COMPLETED несёт sha256 и size_bytes.
    [Fact]
    public Task FullBackup_SingleObjectInList() => RunScenarioAsync(async _ =>
    {
        // Arrange — контур: воркер бэкапов + кластер + засев ≥3 ГиБ.
        var ctAll = TestContext.Current.CancellationToken;
        var cluster = $"bksingle{Fx.ClusterTag}";
        await using var app = await StartBackupHostAsync("bksingle", ctAll);
        var setup = await ArrangeLargeAsync(cluster, ctAll);

        // Act — полный до COMPLETED.
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ctAll);
        cts.CancelAfter(SectionCeiling);
        var ct = cts.Token;
        await Fx.SeedS3PoolAsync(Pool, ct);
        await PutPolicyAsync(cluster, ct);
        var completed = await WaitCompletedAsync(cluster, ct);
        completed.Should().NotBeNull("the large full must reach COMPLETED within the window");
        var id = completed!.Key.Split('/').Last();

        // Assert 1 — листинг full/ возвращает РОВНО один объект набора.
        var listing = await McLsAsync($"{cluster}/shard1/full/");
        var objects = listing.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        objects.Should().ContainSingle("the full backup is a single tar object");
        objects[0].Should().Contain($"{id}.tar", "mc ls печатает строку объекта с датой/размером");

        // Assert 2 — verify (on_create) дочитывает объект целиком: OK + checked_unix.
        var verified = await E2ePhase.WaitAsync(Fx, "large-set-verify", async () =>
                (await FullKeysAsync(cluster, "shard1"))
                    .Any(f => f.Value.Contains(""""verify":{"state":"OK"""")),
            TimeSpan.FromSeconds(150), ct);
        verified.Should().BeTrue("on_create verify must reach OK on the 3 GiB object");
        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(done.Value)!;
        status["verify"].GetProperty("state").GetString().Should().Be("OK");
        status["verify"].GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);

        // Assert 3 — поля канона COMPLETED: sha256 (hex, 64 знака) и size_bytes ≥3 ГиБ.
        status["sha256"].GetString().Should().MatchRegex("^[0-9a-f]{64}$",
            "sha256 of the tar object is written to etcd");
        status["size_bytes"].GetInt64().Should().BeGreaterThanOrEqualTo(MinSetBytes,
            "the set size in etcd is the ≥3 GiB seeded volume")
            .And.BeGreaterThanOrEqualTo(setup.RelationBytes);
    }, TestContext.Current.CancellationToken);

    // Пометка упавшего сценария (stop-режим teardown'а, arch/testing/01-e2e-launch.md §3):
    // контейнеры/тома/артефакты остаются для разбора, зачистка — по
    // README-cleanup.txt после анализа.
    private async Task RunScenarioAsync(Func<E2eEnvironment, Task> body, CancellationToken ct)
    {
        try
        {
            await body(Fx);
        }
        catch
        {
            Fx.MarkFailed();
            throw;
        }
    }

    /// <summary>Результат подготовки «большого набора»: DSN админа, узел-мастер,
    /// контейнеры шарда, базовая занятость data-volume и физический размер
    /// засеянной таблицы.</summary>
    private sealed record LargeSetup(
        string AdminDsn,
        string MasterNode,
        IReadOnlyList<string> Containers,
        Dictionary<string, long> BaselineUsed,
        long RelationBytes);

    // Подготовка профиля в трёх 3-минутных секциях: provisioning → засев →
    // синхронизация реплик + checkpoint + базовая занятость data-volume.
    private async Task<LargeSetup> ArrangeLargeAsync(string cluster, CancellationToken ctAll)
    {
        // Секция 1 — кластер + provisioning (воркер поднимает 4 ноды).
        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(ctAll))
        {
            cts.CancelAfter(SectionCeiling);
            var ct = cts.Token;
            await SeedClusterAsync(cluster, ct);
            var provisioned = await E2ePhase.WaitAsync(Fx, "large-set-provisioning",
                E2eEnvironment.ProvisionStuckFailFast(
                    Fx, GetOrNullAsync, cluster, () => ProvisionedAsync(cluster)),
                TimeSpan.FromSeconds(180), ct);
            provisioned.Should().BeTrue("the cluster must be provisioned before the seed");
        }

        // Секция 2 — засев ≥3 ГиБ через админский DSN мастера.
        string adminDsn;
        string masterNode;
        IReadOnlyList<string> containers;
        long relationBytes;
        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(ctAll))
        {
            cts.CancelAfter(SectionCeiling);
            var ct = cts.Token;
            var (host, port, node) = await MasterPgAsync(cluster, "shard1", ct);
            adminDsn = AdminDsn(host, port, cluster);
            masterNode = node;
            await SeedLargeSetAsync(adminDsn, ct);
            relationBytes = long.Parse(
                await ScalarAsync(adminDsn, "SELECT pg_total_relation_size('large_set')", ct),
                CultureInfo.InvariantCulture);
            relationBytes.Should().BeGreaterThanOrEqualTo(MinSetBytes,
                "the seeded relation is ≥3 GiB (профиль «большой набор»)");
            containers = await Fx.ShardNodeContainerNamesAsync(cluster, "shard1", ct);
        }

        // Секция 3 — реплики догнали засев, checkpoint и базовая точка занятости
        // data-volume.
        Dictionary<string, long> baseline;
        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(ctAll))
        {
            cts.CancelAfter(SectionCeiling);
            var ct = cts.Token;
            var synced = await E2ePhase.WaitAsync(Fx, "large-set-replica-sync",
                () => ReplicasSyncedAsync(cluster, masterNode, adminDsn, ct),
                TimeSpan.FromSeconds(90), ct);
            synced.Should().BeTrue(
                "the replica must replay the seeded set before the baseline measurement");
            // Засев осел на диск: checkpoint сбрасывает ещё не записанные страницы,
            // иначе их запись во время полного дала бы ложный прирост занятости.
            await ExecAsync(adminDsn, "CHECKPOINT", ct);
            baseline = new Dictionary<string, long>();
            foreach (var container in containers)
                baseline[container] = await DataVolumeUsedBytesAsync(container, ct);
            Console.WriteLine("[PHASE] large-set-baseline: "
                + string.Join(",", baseline.Select(kv => $"{kv.Key}={kv.Value}")));
        }

        return new LargeSetup(adminDsn, masterNode, containers, baseline, relationBytes);
    }

    // Засев: 3400 строк по 1 МиБ батчами по 400 (сжатие TOAST отключено —
    // repeat('x', 1 МиБ) занимает физический 1 МиБ, набор честно ≥3 ГиБ).
    private static async Task SeedLargeSetAsync(string adminDsn, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        await ExecAsync(adminDsn, "CREATE TABLE large_set(id int, payload text)", ct);
        await ExecAsync(adminDsn,
            "ALTER TABLE large_set ALTER COLUMN payload SET STORAGE EXTERNAL", ct);
        for (var from = 1; from <= SeedRows; from += SeedBatchRows)
        {
            var to = Math.Min(from + SeedBatchRows - 1, SeedRows);
            await ExecAsync(adminDsn,
                $"INSERT INTO large_set SELECT g, repeat('x', 1048576) FROM generate_series({from}, {to}) g",
                ct);
        }

        Console.WriteLine($"[PHASE] large-set-seed: {SeedRows} MiB rows in {sw.Elapsed.TotalSeconds:F1}s");
    }

    // Ожидание COMPLETED с поллингом ЗАНЯТОСТИ data-volume нод шарда (бюджет
    // 170 с — внутри 3-минутной секции). Максимум фиксируется по каждому
    // контейнеру; нода-источник определяется по статусу после завершения.
    private async Task<(Kv? Completed, Dictionary<string, long> MaxUsed,
        Dictionary<string, long> MaxTmp)>
        WaitCompletedSamplingUsedAsync(
            string cluster, IReadOnlyList<string> containers, CancellationToken ct)
    {
        var maxUsed = containers.ToDictionary(c => c, _ => 0L);
        // Вторая копия вне data-volume (writable-слой/`/tmp`) не видна du по
        // PGDATA: снимаем и /tmp — потоковый полный не пишет на диск ничего.
        var maxTmp = containers.ToDictionary(c => c, _ => 0L);
        var sw = Stopwatch.StartNew();
        var budget = TimeSpan.FromSeconds(170);
        var nextTick = TimeSpan.FromSeconds(5);
        Kv? completed = null;
        while (true)
        {
            foreach (var container in containers)
            {
                maxUsed[container] = Math.Max(maxUsed[container], await DataVolumeUsedBytesAsync(container, ct));
                maxTmp[container] = Math.Max(maxTmp[container], await TmpUsedBytesAsync(container, ct));
            }
            try
            {
                completed = (await FullKeysAsync(cluster, "shard1"))
                    .FirstOrDefault(f => f.Value.Contains("COMPLETED"));
            }
            catch (Exception)
            {
                // транзиент чтения etcd — следующая попытка
            }

            if (completed is not null || sw.Elapsed >= budget)
                break;
            if (sw.Elapsed >= nextTick)
            {
                Console.WriteLine($"[PHASE-TICK] large-set-full t={sw.Elapsed.TotalSeconds:F1}s: "
                    + string.Join(",", containers.Select(c => $"{c}={maxUsed[c]}")));
                nextTick += TimeSpan.FromSeconds(5);
            }

            await Task.Delay(1000, ct);
        }

        Console.WriteLine($"[PHASE] large-set-full: ok={completed is not null}, "
            + $"elapsed={sw.Elapsed.TotalSeconds:F1}s, budget={budget.TotalSeconds:F0}s");
        if (completed is null)
        {
            await Fx.CollectDiagnosticsAsync("failed-phase-large-set-full");
            Fx.MarkFailed();
        }

        return (completed, maxUsed, maxTmp);
    }

    // Ожидание COMPLETED без замера диска (второй сценарий профиля).
    private async Task<Kv?> WaitCompletedAsync(string cluster, CancellationToken ct)
    {
        var completed = await E2ePhase.WaitAsync(Fx, "large-set-full", async () =>
                (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(170), ct);
        if (!completed)
            return null;
        return (await FullKeysAsync(cluster, "shard1")).FirstOrDefault(f => f.Value.Contains("COMPLETED"));
    }

    // Занятость /tmp контейнера (служебные файлы; вторая копия набора сюда
    // попасть не должна — потоковый полный на диск не пишет).
    private async Task<long> TmpUsedBytesAsync(string container, CancellationToken ct)
    {
        var output = await Fx.RunDockerAsync(["exec", container, "du", "-sb", "/tmp"], ct);
        var line = output.Split('\n', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
        if (line is null)
            return 0;
        var fields = line.Split([' ', '\t'], StringSplitOptions.RemoveEmptyEntries);
        return fields.Length >= 2 && long.TryParse(fields[0], CultureInfo.InvariantCulture, out var used)
            ? used
            : 0;
    }

    // Занятость data-volume ноды — байты фактического содержимого PGDATA
    // (du -sb). df свободного места в E2E непригоден: у Docker Desktop все
    // тома — один диск VM, и запись самого S3-объекта/WAL-выгрузок «съедает»
    // свободное место независимо от копии внутри data-volume; du меряет
    // ровно то, что требует критерий профиля — вторую копию в data-volume.
    private async Task<long> DataVolumeUsedBytesAsync(string container, CancellationToken ct)
    {
        var output = await Fx.RunDockerAsync(
            ["exec", container, "du", "-sb", "/home/postgres/pgdata"], ct);
        var line = output.Split('\n', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault()
            ?? throw new ApplicationException($"du output is empty for {container}");
        var fields = line.Split([' ', '\t'], StringSplitOptions.RemoveEmptyEntries);
        fields.Length.Should().BeGreaterThanOrEqualTo(2, $"du output for {container}: {output}");
        return long.Parse(fields[0], CultureInfo.InvariantCulture);
    }

    // Реплики шарда replay'нули текущую позицию мастера (replay_lsn из SQL
    // реплики — фактическое подтверждение, не представление Patroni).
    private async Task<bool> ReplicasSyncedAsync(
        string cluster, string masterNode, string adminDsn, CancellationToken ct)
    {
        try
        {
            var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
            if (kv.Value is not { } raw)
                return false;
            var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(raw.Value)!;
            var replicas = entries
                .Where(p => p.Key.StartsWith("shard1/", StringComparison.Ordinal)
                            && p.Key.Split('/')[1] != masterNode)
                .ToList();
            if (replicas.Count == 0)
                return false;
            var masterPos = ParseLsn(await ScalarAsync(adminDsn, "SELECT pg_current_wal_lsn()", ct));
            foreach (var (_, addr) in replicas)
            {
                var replay = ParseLsn(await ScalarAsync(
                    AdminDsn("localhost", addr.GetProperty("pg").GetInt32(), cluster),
                    "SELECT pg_last_wal_replay_lsn()", ct));
                if (replay < masterPos)
                    return false;
            }

            return true;
        }
        catch (Exception)
        {
            // нода рестартует/ещё не принимает соединения — следующая попытка
            return false;
        }
    }

    // LSN 'X/Y' → ulong (старшие 32 бита — X, младшие — Y); пусто/мусор → 0.
    private static ulong ParseLsn(string lsn)
    {
        var parts = lsn.Trim().Split('/');
        if (parts.Length != 2
            || !uint.TryParse(parts[0], NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var hi)
            || !uint.TryParse(parts[1], NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var lo))
            return 0;
        return ((ulong)hi << 32) | lo;
    }

    // Published pg-порт мастера шарда из portalloc (резолв фактического primary
    // по пробам Patroni /primary; host-клиент — localhost).
    private HttpClient PatroniHttp => Fx.NewPatroniHttpClient();

    private async Task<(string Host, int Port, string Node)> MasterPgAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc is written at provisioning");
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
        string? primary = null;
        var resolved = await E2eFixture.WaitForAsync(async () =>
        {
            foreach (var (key, addr) in entries
                         .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                         .OrderBy(p => p.Key, StringComparer.Ordinal))
            {
                try
                {
                    using var response = await PatroniHttp.GetAsync(
                        $"https://localhost:{addr.GetProperty("patroni").GetInt32()}/primary", ct);
                    if (response.IsSuccessStatusCode)
                    {
                        primary = key.Split('/')[1];
                        return true;
                    }
                }
                catch (Exception)
                {
                    // проба (рестарт/ещё не готова) — следующая нода
                }
            }

            return false;
        }, TimeSpan.FromSeconds(90), ct);
        resolved.Should().BeTrue("the shard primary must be resolved by Patroni probes");
        return ("localhost", entries[$"{shard}/{primary}"].GetProperty("pg").GetInt32(), primary!);
    }

    // Воркер с включённой подсистемой бэкапов: пул — policy.s3 (etcd), bucket и
    // env-профиль кредов — env (t36); операции — в приёмниках нод.
    private Task<HostInstance> StartBackupHostAsync(
        string name, CancellationToken ct,
        IReadOnlyDictionary<string, string>? extraEnv = null)
    {
        var env = new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
        };
        foreach (var (key, value) in extraEnv ?? new Dictionary<string, string>())
            env[key] = value;
        return Fx.StartHostAsync(name, extraEnv: env, ct: ct);
    }

    // Policy кластера: полный по возрасту (переснятие в окне теста не нужно —
    // листинг обязан остаться из одного объекта) + verify on_create.
    private Task PutPolicyAsync(string cluster, CancellationToken ct)
        => G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            $$"""{"full_max_age_sec":3600,"verify":{"on_create":true,"interval_sec":0},{{E2eEnvironment.PoolS3(Pool)}}}""",
            null, ct);

    private async Task<IReadOnlyList<Kv>> FullKeysAsync(string cluster, string shard)
        => (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/{shard}/full/",
            TestContext.Current.CancellationToken)).Value;

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    // Условие готовности provisioning: config без state + dsn + нода RUNNING.
    private async Task<bool> ProvisionedAsync(string cluster)
    {
        var config = await GetOrNullAsync($"/clusters/{cluster}/config");
        if (config is null)
            return false;
        if (JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(config.Value)!.ContainsKey("state"))
            return false;
        var dsn = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/dsn");
        var node = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/nodes/shard1a/state");
        return dsn is not null && node is { Value: "RUNNING" };
    }

    private static string AdminDsn(string host, int port, string cluster)
        => DatabaseProvisioner.BuildAdminDsn(host, port, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));

    private static async Task ExecAsync(string dsn, string sql, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(dsn);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ScalarAsync(string dsn, string sql, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(dsn);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        return Convert.ToString(await cmd.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) ?? "";
    }

    // mc ls --recursive s3-пути кластера (mc в сети ownS3-ноды окружения — по алиасу).
    private async Task<string> McLsAsync(string path)
        => await Fx.RunDockerAsync(
        [
            "run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
            "-c", $"mc alias set t {Fx.S3EndpointForContainers} {E2eEnvironment.S3RootUser} {E2eEnvironment.S3RootPassword} >/dev/null && mc ls --recursive t/{Bucket}/{path}",
        ], TestContext.Current.CancellationToken);

    // Сид кластера БЕЗ policy: полный стартует только после засева (policy
    // ставится позже — иначе бэкап снял бы пустую базу).
    private async Task SeedClusterAsync(string cluster, CancellationToken ct)
    {
        var config = $$"""
            {"buckets":2,"dbname":"{{cluster}}","created_unix":1755800000,"state":"NOT_INITIALIZED","bucket_admin_password":"{{E2eFixture.BucketAdminPassword}}"}
            """;
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config", config, null, ct);
        foreach (var shard in new[] { "shard1", "shard2" })
        {
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/replicas", "2", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}a/state", "NOT_INITIALIZED", null, ct);
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/{shard}b/state", "NOT_INITIALIZED", null, ct);
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
}

/// <summary>Компаньон-фикстура окружения класса (полный контур на класс:
/// сеть/etcd/ownS3/CA/окно портов; arch/testing/02-e2e-isolation.md §1).</summary>
public sealed class E2eBackupLargeSetScenariosFx : E2eContourFixture
{
    protected override string Slug => "bk-large";
    protected override bool WithS3Node => true;
}
