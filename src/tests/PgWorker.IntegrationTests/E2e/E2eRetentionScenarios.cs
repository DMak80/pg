using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E ретенции (t06, spec §4 Ф6): изолированное окружение E2eEnvironment (своя
// сеть/etcd/MinIO, динамические порты, полный teardown в DisposeAsync) +
// provisioned-кластер с включёнными бэкапами → реальный COMPLETED полный →
// синтетика (etcd-ключ старого COMPLETED ~40 дней назад с wal_start ниже
// реального + его объекты full/<old-id>/ + WAL-объекты ниже cutoff) →
// policy days=1/weeks=0/months=0, маленький Retention:IntervalSec →
// ассерты: старые префиксы/ключи удалены, реальный полный жив, WAL ниже
// cutoff удалён, всё живое ≥ cutoff, ключ /pgworker/backups/storage пишется.
public class E2eRetentionScenarios
{
    private const string Bucket = "pgworker-backups";

    // Окружение Fact'а (своя сеть/etcd/MinIO); создаётся в начале сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private PgWorker.Etcd.Client.EtcdGateway G => Fx.Gateway;

    // AAA: GFS-чистка на живом кластере — guard реального COMPLETED, удаление
    // старого через DELETING, WAL-trim ниже cutoff, ключ storage
    [Fact]
    public async Task Retention_TrimsToGfs()
    {
        // Arrange 1 — окружение + кластер bkret + policy 1/0/0 + воркер
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-ret", withMinio: true, ct: ct);
        Fx = fx;
        const string cluster = "bkret";
        await SeedClusterAsync(cluster);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"retention":{"days":1,"weeks":0,"months":0},"full_max_age_sec":600,"verify":{"on_create":false}}""",
            null, ct);
        await using var app = await StartRetentionHostAsync("bkret", ct);

        // Arrange 2 — provisioning DONE
        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning должен дойти до DONE до бэкапов");

        // Arrange 3 — генерация WAL ДО первого полного: позиция wal_start
        // обязана оставить минимум 5 сегментов ниже cutoff для посева
        var (pgHost, pgPort) = await MasterPgAsync(cluster, "shard1", ct);
        var adminDsn = DatabaseProvisioner.BuildAdminDsn(pgHost, pgPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
        await SwitchWalsAsync(adminDsn, 16, ct);

        // Arrange 4 — реальный COMPLETED полный; при слишком низком wal_start
        // (гонка старта джоба с генерацией) — переснимаем: чистим ключи,
        // планировщик снимает новый (позиция к тому времени выше)
        var real = await WaitForHighWalStartFullAsync(cluster, adminDsn, ct);

        // Arrange 5 — синтетика: старый COMPLETED (40 дней назад, wal_start
        // = realWalStart−2) + его объекты full/<old-id>/ + WAL-объекты
        // realWalStart−4 и realWalStart−3 (mc-посев)
        var realStart = PgWorker.Backups.WalFileName.TryParse(real.WalStart)!.Value;
        var realPos = (long)realStart.Log * 256 + realStart.Seg;
        var at = (long p) => new PgWorker.Backups.WalFileName(1, (uint)(p / 256), (uint)(p % 256));
        var oldWalStart = at(realPos - 2).Name;
        var seededWal = new[] { at(realPos - 4).Name, at(realPos - 3).Name };
        var oldId = $"{DateTimeOffset.UtcNow.AddDays(-40):yyyyMMddHHmmss}Z";
        var oldStarted = DateTimeOffset.UtcNow.AddDays(-40).ToUnixTimeSeconds();
        await G.PutAsync(Endpoint,
            PgWorker.Backups.BackupNames.FullKey(cluster, "shard1", oldId),
            PgWorker.Backups.Job.BackupStatusJson.Serialize(new PgWorker.Etcd.Parsing.FullBackupState(
                oldId, FullBackupStatus.Completed, "shard1a", BackupSourceRole.Replica,
                oldStarted, oldStarted + 300, oldWalStart, 1024, null, null)), null, ct);
        await McCpAsync($"{cluster}/shard1/full/{oldId}/base.tar", "synthetic-old-full");
        foreach (var seg in seededWal)
            await McCpAsync($"{cluster}/shard1/wal/{seg}", "synthetic-wal");

        // Act — ретенционный проход (расписание ≤ 60 c): DELETING-доводка
        // старого полного (один кандидат — один проход) + WAL-чистка ниже cutoff
        var trimmed = await E2eFixture.WaitForAsync(
            async () => await GetOrNullAsync(PgWorker.Backups.BackupNames.FullKey(cluster, "shard1", oldId)) is null,
            TimeSpan.FromSeconds(240), ct);
        if (!trimmed)
        {
            // диагностика: журнал воркера + ключи полных + ключ storage
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var fulls = await FullKeysAsync(cluster, "shard1");
            throw new ApplicationException(
                $"ретенция не удалила старый полный: journal=[{workKv?.Value[..Math.Min(400, workKv.Value.Length)]}] " +
                $"fulls=[{string.Join(";", fulls.Select(f => f.Key))}]");
        }
        trimmed.Should().BeTrue("старый COMPLETED (единственный кандидат GFS) обязан уйти");

        // Assert 1 — ключи: старый исчез (WaitFor выше), реальный жив
        (await GetOrNullAsync(PgWorker.Backups.BackupNames.FullKey(cluster, "shard1", real.Id)))
            .Should().NotBeNull("guard: самый свежий COMPLETED не удаляется");

        // Assert 2 — S3: префикс старого пуст, реального жив, посевные WAL
        // ниже cutoff удалены, всё живое в wal/ ≥ cutoff
        var hostEndpoint = Fx.S3Endpoint.Replace(
            "host.docker.internal:", "localhost:", StringComparison.Ordinal);
        await using var backupS3 = new PgWorker.Backups.BackupS3(new PgWorker.Backups.BackupsRuntimeOptions
        {
            Enabled = true,
            S3Endpoint = hostEndpoint,
            S3Bucket = Bucket,
            S3AccessKey = "minioadmin",
            S3SecretKey = "minioadmin",
            S3PathStyle = true,
        });
        var oldPrefix = await backupS3.ListPrefixAsync($"{cluster}/shard1/full/{oldId}/", ct: ct);
        oldPrefix.Value.Should().BeEmpty("объекты старого полного удалены (DELETING-доводка)");
        var realPrefix = await backupS3.ListPrefixAsync($"{cluster}/shard1/full/{real.Id}/", ct: ct);
        realPrefix.Value.Should().NotBeEmpty("объекты реального полного не тронуты");
        var walListed = await backupS3.ListPrefixAsync($"{cluster}/shard1/wal/", ct: ct);
        walListed.Value.Should().NotBeNull();
        walListed.Value.Select(o => o.Key).Should().NotContain(
            seededWal.Select(s => $"{cluster}/shard1/wal/{s}"),
            "посевные сегменты строго ниже cutoff удалены");
        walListed.Value
            .Select(o => o.Key.Split('/')[^1])
            .Where(n => PgWorker.Backups.WalFileName.TryParse(n) is not null)
            .Select(n => (long)PgWorker.Backups.WalFileName.TryParse(n)!.Value.Log * 256
                         + PgWorker.Backups.WalFileName.TryParse(n)!.Value.Seg)
            .Should().OnlyContain(p => p >= realPos, "всё живое в wal/ — не ниже cutoff");

        // Assert 3 — ключ storage: существует, used > 0, state OK (квота не задана)
        var storage = await GetOrNullAsync("/pgworker/backups/storage");
        storage.Should().NotBeNull("ретенционный проход пишет монитор занятости");
        var storageDoc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(storage!.Value)!;
        storageDoc["used_bytes"].GetInt64().Should().BeGreaterThan(0);
        storageDoc["state"].GetString().Should().Be("OK");
        storageDoc.Should().NotContainKey("quota_bytes", "квота в E2E не задана");
    }

    // ===== Хелперы (копии образца E2eBackupScenarios — файлы сценариев
    // независимы, паттерн репо) =====

    // Реальный COMPLETED с позицией wal_start ≥ 5 (посев строго ниже cutoff
    // должен существовать). Позиция мала → чистим ключи полных: планировщик
    // видит «COMPLETED нет» и снимает новый полный (RetryBaseSec=2).
    private async Task<(string Id, string WalStart)> WaitForHighWalStartFullAsync(
        string cluster, string adminDsn, CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            var completed = await E2eFixture.WaitForAsync(
                async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
                TimeSpan.FromSeconds(300), ct);
            completed.Should().BeTrue("реальный полный обязан дойти до COMPLETED");
            var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
            var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(done.Value)!;
            var walSeg = status["wal_start_segment"].GetString();
            walSeg.Should().NotBeNullOrEmpty("wal_start_segment заполняется с UPLOADING");
            var parsed = PgWorker.Backups.WalFileName.TryParse(walSeg!);
            if (parsed is { } w && (long)w.Log * 256 + w.Seg >= 5)
                return (done.Key.Split('/').Last(), walSeg!);

            // Позиция мала (гонка старта джоба с генерацией WAL) — дожимаем
            // переключения и переснимаем полный с чистого листа
            Assert.True(attempt < 3, "полный так и не встал выше позиции 5 по wal_start");
            await SwitchWalsAsync(adminDsn, 16, ct);
            await G.DeleteAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/full/",
                prefix: true, ct);
        }
    }

    // Воркер с включённой подсистемой: S3 на локальный MinIO, образ джобов
    // pgworker-backup:e2e, ускоренный бэкофф, ретенция раз в 60 c (минимум
    // валидации IntervalSec ≥ 60; первый проход — сразу, далее по расписанию).
    private Task<HostInstance> StartRetentionHostAsync(string name, CancellationToken ct)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Endpoint"] = Fx.S3Endpoint,
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Backups__S3__SecretKey"] = "minioadmin",
            ["PgWorker__Backups__Job__Image"] = E2eEnvironment.JobImage,
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
            ["PgWorker__Backups__Policy__FullMaxAgeSec"] = "600",
            ["PgWorker__Backups__Policy__VerifyOnCreate"] = "false",
            ["PgWorker__Backups__Retention__IntervalSec"] = "60",
        }, ct: ct);

    private async Task<IReadOnlyList<Kv>> FullKeysAsync(string cluster, string shard)
        => (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/{shard}/full/",
            TestContext.Current.CancellationToken)).Value;

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    // mc-посев объекта с фиксированным телом (mc в сети MinIO окружения).
    private async Task McCpAsync(string key, string content)
        => await Fx.RunDockerAsync(
        [
            "run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
            "-c", $"mc alias set t http://e2e-minio:9000 minioadmin minioadmin >/dev/null"
                  + $" && echo {content} >/tmp/f && mc cp /tmp/f t/{Bucket}/{key}",
        ], TestContext.Current.CancellationToken);

    // Сид кластера (копия E2eRotateScenarios.SeedClusterAsync).
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

    // Published pg-порт мастера шарда из portalloc (копия MasterPgAsync —
    // резолв фактического primary по пробам Patroni /primary).
    private static readonly HttpClient PatroniHttp = new() { Timeout = TimeSpan.FromSeconds(3) };

    private async Task<(string Host, int Port)> MasterPgAsync(string cluster, string shard, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc пишется при provisioning");
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
                        $"http://localhost:{addr.GetProperty("patroni").GetInt32()}/primary", ct);
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
        }, TimeSpan.FromSeconds(120), ct);
        resolved.Should().BeTrue("primary шарда обязан определиться пробами Patroni");
        var entry = entries[$"{shard}/{primary}"];
        return (entry.GetProperty("host").GetString()!, entry.GetProperty("pg").GetInt32());
    }

    // pg_switch_wal × n (superuser-only): форсированное закрытие пустых
    // сегментов 16 МБ — wal_start будущего полного уходит выше позиции посева.
    private static async Task SwitchWalsAsync(string adminDsn, int count, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        for (var i = 0; i < count; i++)
        {
            await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
            await switchWal.ExecuteScalarAsync(ct);
        }
    }

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
}
