using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E полных бэкапов (t02, spec §7.1–7.4): изолированное окружение E2eEnvironment
// (своя docker-сеть, свой etcd, СВОЙ MinIO — withMinio:true; порт динамический)
// + образ pgworker-backup:e2e + живой кластер воркером с Backups:Enabled=true.
// COMPLETED с полными полями и объектами в S3; FAILED по недоступному S3 с
// переснятием новым id; deprovisioning чистит джобы и префикс; ротация
// backup_password включает backup_exec. Каждый Fact — своё окружение: методы
// оставляют после себя Active-кластеры, и без per-method изоляции воркер
// следующего Fact'а подхватывал бы чужие джобы (инцидент Release). Имя кластера сценария —
// {slug}{Fx.ClusterTag} (уникально на прогон, docs/e2e-isolation.md §1):
// движковые контейнеры/тома pgw-*-<C>-* опознаются teardown'ом окружения.
public class E2eBackupScenarios
{
    private const string Bucket = "pgworker-backups";

    // Окружение Fact'а (своя сеть/etcd/MinIO); создаётся в начале каждого сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // AAA: полный суточный цикл — PLANNED→RUNNING→UPLOADING→COMPLETED,
    // поля канона, объекты full/<id>/ + wal/ в S3, чистка контейнера/volume
    [Fact]
    public async Task Backup_FullDaily_Completes()
    {
        // Arrange — кластер bkshop<тег прогона> + policy (verify on_create) + воркер
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-full", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"bkshop{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"full_max_age_sec":3600,"verify":{"on_create":true}}""", null, ct);
        await using var app = await StartBackupHostAsync("bkfull", ct);

        // Act/Assert 1 — COMPLETED на shard1 за ≤ 300 c
        var completed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(300), ct);
        completed.Should().BeTrue("полный бэкап должен дойти до COMPLETED");

        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(done.Value)!;
        status["state"].GetString().Should().Be("COMPLETED");
        var walSeg = status["wal_start_segment"].GetString();
        walSeg.Should().NotBeNullOrEmpty("wal_start_segment заполняется с UPLOADING");
        status["node"].GetString().Should().NotBeNullOrEmpty();
        status["role"].GetString().Should().BeOneOf("replica", "master");
        status["started_unix"].GetInt64().Should().BeGreaterThan(0);
        status["finished_unix"].GetInt64().Should().BeGreaterThanOrEqualTo(status["started_unix"].GetInt64());
        status["size_bytes"].GetInt64().Should().BeGreaterThan(0);
        status["verify"].GetProperty("state").GetString().Should().Be("PENDING");

        // Assert 2 — объекты в S3: full/<id>/ с manifest и pg_wal/ + wal/<seg>
        var id = done.Key.Split('/').Last();
        var listing = await McLsAsync($"{cluster}/shard1/");
        listing.Should().Contain($"full/{id}/backup_manifest");
        listing.Should().Contain($"full/{id}/PG_VERSION");
        listing.Should().Contain($"full/{id}/pg_wal/");
        listing.Should().Contain($"wal/{walSeg}", "закрытый сегмент набора -X stream дублируется в wal/-префикс");

        // Assert 3 — чистка: контейнера и staging volume нет. COMPLETED пишется
        // в etcd ДО CleanupJobAsync (BackupProcess.PutAsync → CleanupJobAsync) —
        // между итогом и удалением джоба/volume есть окно, поэтому чистка ждётся
        // ограниченно (паттерн Backup_Deprovision_CleansPrefix), а не мгновенно.
        var cleaned = await E2eFixture.WaitForAsync(async () =>
        {
            var jobContainers = await Fx.RunDockerAsync(
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-full-{cluster}-"], ct);
            if (jobContainers.Length > 0)
                return false;
            var stagingVolumes = await Fx.RunDockerAsync(
            ["volume", "ls", "-q", "--filter", $"name=pgw-backup-{cluster}-"], ct);
            return stagingVolumes.Length == 0;
        }, TimeSpan.FromSeconds(60), ct);
        cleaned.Should().BeTrue("ephemeral-джоб и staging volume удаляются после итога");
    }

    // AAA: недоступный S3 → FAILED с error; переснятие НОВЫМ id после бэкоффа
    [Fact]
    public async Task Backup_FailsOnBadS3_RetriesWithNewId()
    {
        // Arrange — S3-endpoint на заведомо закрытом порт 1 (tcpmux; НЕ тестовый
        // хардкод: фиксированный протокольный «всегда закрыт»)
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-bads3", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"bkbads3{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var app = await StartBackupHostAsync(
            "bkbads3", ct, s3EndpointOverride: "http://host.docker.internal:1");

        // Act/Assert 1 — первая попытка FAILED с error ≤ 300 c
        var failed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("FAILED")),
            TimeSpan.FromSeconds(300), ct);
        failed.Should().BeTrue("попытка с недоступным S3 должна упасть в FAILED");
        var failedKv = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("FAILED"));
        var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(failedKv.Value)!;
        status["error"].GetString().Should().NotBeNullOrEmpty("причина фиксируется в статусе");

        // Assert 2 — переснятие: вторая попытка с ДРУГИМ id (Retry BaseSec=2)
        var retried = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Count >= 2,
            TimeSpan.FromSeconds(120), ct);
        retried.Should().BeTrue("бэкофф 2 c должен запустить переснятие новым id");
    }

    // AAA: deprovisioning не переживают джобы и префикс /pgworker/backups/<C>/
    [Fact]
    public async Task Backup_Deprovision_CleansPrefix()
    {
        // Arrange — кластер bkclean; ждём появления первой попытки (джоб жив)
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-clean", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"bkclean{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var app = await StartBackupHostAsync("bkclean", ct);
        var started = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Count > 0,
            TimeSpan.FromSeconds(300), ct);
        started.Should().BeTrue("подсистема должна начать первую попытку");

        // Act — state=TO_REMOVE (панель-семантика §4.2)
        var config = await G.GetAsync(Endpoint, $"/clusters/{cluster}/config", ct);
        config.Value.Should().NotBeNull();
        var doc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(config.Value!.Value)!;
        doc["state"] = JsonSerializer.SerializeToElement("TO_REMOVE");
        await G.PutAsync(Endpoint, $"/clusters/{cluster}/config",
            JsonSerializer.Serialize(doc), null, ct);

        // Assert — префикс бэкапов пуст; джобов и staging volume нет
        var cleaned = await E2eFixture.WaitForAsync(async () =>
        {
            if ((await FullKeysAsync(cluster, "shard1")).Count > 0
                || (await FullKeysAsync(cluster, "shard2")).Count > 0)
                return false;
            var containers = await Fx.RunDockerAsync(
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-full-{cluster}-"], ct);
            if (containers.Length > 0)
                return false;
            var volumes = await Fx.RunDockerAsync(
            ["volume", "ls", "-q", "--filter", $"name=pgw-backup-{cluster}-"], ct);
            return volumes.Length == 0;
        }, TimeSpan.FromSeconds(180), ct);
        cleaned.Should().BeTrue("deprovisioning должен убрать джобы, volume и префикс бэкапов");
    }

    // AAA: ротация per-cluster секретов включает backup_exec: NEW-пароль
    // подключается к мастеру ролью backup_exec (R2-гвард, spec §7.4)
    [Fact]
    public async Task Backup_Rotator_IncludesBackupExec()
    {
        // Arrange — кластер bkrot с завершённым полным; OLD backup_password
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-rot", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"bkrot{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var app = await StartBackupHostAsync("bkrot", ct);
        var completed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(300), ct);
        completed.Should().BeTrue("до ротации должен быть завершённый полный (G2 жил каждый тик)");

        var oldPassword = (await G.GetAsync(Endpoint, $"/clusters/{cluster}/backup_password", ct)).Value!.Value;

        // Act — заявка ротации (формат панели §9.8)
        await G.PutAsync(Endpoint, $"/pgworker/rotations/{cluster}",
            $$"""{"requested_unix":{{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}},"requested_by":"e2e"}""",
            null, ct);

        // Assert 1 — backup_password сменился, заявка удалена
        var rotated = await E2eFixture.WaitForAsync(async () =>
        {
            var current = await G.GetAsync(Endpoint, $"/clusters/{cluster}/backup_password", ct);
            return current.Value is { } kv && kv.Value != oldPassword
                && await GetOrNullAsync($"/pgworker/rotations/{cluster}") is null;
        }, TimeSpan.FromSeconds(240), ct); // t03: фон тяжелее (WAL-агенты) — 120 c тесно
        if (!rotated)
        {
            // диагностика: журнал ротации (last_error) + живая заявка + пароль
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var ticketKv = await GetOrNullAsync($"/pgworker/rotations/{cluster}");
            var pwKv = await GetOrNullAsync($"/clusters/{cluster}/backup_password");
            throw new ApplicationException(
                $"ротация не завершилась: journal=[{workKv?.Value[..Math.Min(400, workKv.Value.Length)]}] " +
                $"ticket=[{ticketKv?.Value ?? "-"}] password_changed={(pwKv!.Value != oldPassword)}");
        }
        rotated.Should().BeTrue("ротация должна перезаписать backup_password и закрыть заявку");
        var newPassword = (await G.GetAsync(Endpoint, $"/clusters/{cluster}/backup_password", ct)).Value!.Value;

        // Assert 2 — NEW-пароль подключается user=backup_exec к мастеру shard1:
        // DSN-ключ — multi-host с чужими кредами (bucket_admin), дубликаты
        // User/Password в строке недопустимы — собираем параметры явно
        // (пары host:port из ключа, Target Session Attributes=read-write).
        var dsn = (await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/dsn"))!.Value;
        dsn.Should().Contain(",", "multi-host DSN");
        var hosts = System.Text.RegularExpressions.Regex.Match(dsn, "host=([^ ]+)").Groups[1].Value.Split(',');
        var ports = System.Text.RegularExpressions.Regex.Match(dsn, "port=([^ ]+)").Groups[1].Value.Split(',');
        var multiHost = string.Join(",", hosts.Zip(ports, (h, p) => $"{h}:{p}"));
        var connected = await E2eFixture.WaitForAsync(async () =>
        {
            try
            {
                await using var con = new NpgsqlConnection(
                    $"Host={multiHost};Database={cluster};Username=backup_exec;Password={newPassword};" +
                    "Timeout=5;SSL Mode=Require;Trust Server Certificate=true;Target Session Attributes=read-write");
                await con.OpenAsync(ct);
                await using var cmd = new NpgsqlCommand("SELECT 1", con);
                return await cmd.ExecuteScalarAsync(ct) is 1;
            }
            catch (NpgsqlException)
            {
                return false; // failover-окно/рестарт — повторим
            }
        }, TimeSpan.FromSeconds(60), ct);
        connected.Should().BeTrue("роль backup_exec принимает новый пароль (гвард R2)");
    }


    // AAA: WAL-поток (t03, AC1/AC2/AC3): provisioned кластер + INSERT/pg_switch_wal
    // → сегменты в MinIO, ключ wal ACTIVE с chain_start/last_uploaded, цепочка
    // непрерывна, агент running, слот pgw_bkp_<C>_<X> на мастере.
    [Fact]
    public async Task WalStream_UploadsSegmentsContinuously()
    {
        // Arrange — гейт docker; skip-защита: общий образ t02/t03 обязан быть в дереве
        DockerTrait.SkipIfUnavailable();
        if (!File.Exists(Path.Combine(
                E2eFixture.FindRoot(AppContext.BaseDirectory), "docker", "PgWorker.Backup.Dockerfile")))
            Assert.Skip("docker/PgWorker.Backup.Dockerfile отсутствует — общий образ джобов/агентов приходит из t02");
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("wal-stream", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"shopb{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var app = await StartWalHostAsync("walstream", ct);

        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning должен дойти до DONE до WAL-нагрузки");

        // Мастер shard1 (published pg-порт) + admin/superuser-DSN: нагрузка и
        // pg_switch_wal (superuser-only) выполняются одним соединением — как
        // t02 SqlListAsync (dsn-креды app_user недоступны тесту напрямую).
        var (pgHost, pgPort) = await MasterPgAsync(cluster, "shard1", ct);
        var adminDsn = DatabaseProvisioner.BuildAdminDsn("localhost", pgPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));

        // Act — INSERT-нагрузка: большие строки + pg_switch_wal форсируют закрытие
        // сегментов 16 МБ (без таймаутов, spec §5).
        await GenerateWalAsync(adminDsn, ct);

        // BackupS3 host-клиент: воркер ходит на localhost (published порт MinIO),
        // агентам отдаётся advertised (Fx.S3Endpoint = host.docker.internal:<порт>).
        var hostEndpoint = Fx.S3Endpoint.Replace(
            "host.docker.internal:", "localhost:", StringComparison.Ordinal);
        var backupS3 = new PgWorker.Backups.BackupS3(new PgWorker.Backups.BackupsRuntimeOptions
        {
            Enabled = true,
            S3Endpoint = hostEndpoint,
            S3AdvertisedEndpoint = Fx.S3Endpoint,
            S3Bucket = Bucket,
            S3AccessKey = "minioadmin",
            S3SecretKey = "minioadmin",
            S3PathStyle = true,
            JobImage = E2eEnvironment.JobImage,
        });

        // Assert 1 — бюджет 120 c: сегменты в MinIO, все без .partial (AC1)
        List<PgWorker.Backups.WalObject> listed = [];
        var uploaded = await E2eFixture.WaitForAsync(async () =>
        {
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            if (!list.IsSuccess)
                return false;
            listed = [.. list.Value];
            return listed.Count >= 2;
        }, TimeSpan.FromSeconds(120), ct);
        if (!uploaded)
        {
            // диагностика провала доставки: журнал воркера, ключ wal, агенты, креды
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var pwKv = await GetOrNullAsync($"/clusters/{cluster}/backup_password");
            var agentsPs = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}} {{.State}}", "--filter", $"name=pgw-backup-wal-{cluster}-"], ct);
            var jobLogs = "(агентов нет)";
            foreach (var line in agentsPs.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                var name = line.Split(' ')[0];
                var logs = await Fx.RunDockerAsync(["logs", "--tail", "12", name], ct);
                jobLogs += $"\n== {name}: {logs[..Math.Min(700, logs.Length)]}";
            }

            throw new ApplicationException(
                $"сегменты не доставлены: journal=[{workKv?.Value[..Math.Min(400, workKv.Value.Length)]}] " +
                $"wal=[{walKv?.Value ?? "-"}] backup_password={(pwKv is null ? "-" : "есть")} " +
                $"agents=[{agentsPs.Replace('\n', ';')}] {jobLogs}");
        }
        uploaded.Should().BeTrue("сегменты обязаны появиться в MinIO за бюджет (AC1)");
        listed.Select(o => o.Name).Should().OnlyContain(n => !n.EndsWith(".partial"));

        // Assert 2 — ключ wal ACTIVE и СОГЛАСОВАННЫЙ снапшот «ключ × list»: воркер
        // и агент живут, пары (wal, listed) берутся поллингом до совпадения
        // last_uploaded из ключа с содержимым свежего листа (иначе ассерт сравнивает
        // разные моменты времени живого потока).
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);
        PgWorker.Etcd.Parsing.WalStreamState wal = null!;
        var consistent = await E2eFixture.WaitForAsync(async () =>
        {
            var read = await writer.ReadAsync(cluster, "shard1", ct);
            if (!read.IsSuccess || read.Value is not { State: PgWorker.Etcd.Parsing.WalStreamStatus.Active })
                return false;
            wal = read.Value;
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            if (!list.IsSuccess)
                return false;
            listed = [.. list.Value];
            return listed.Select(o => o.Name).Contains(wal.LastUploadedSegment);
        }, TimeSpan.FromSeconds(120), ct);
        consistent.Should().BeTrue("ключ wal обязан перейти в ACTIVE (AC2) и совпасть с S3-фактом");
        wal.Slot.Should().Be($"pgw_bkp_{cluster}_shard1");
        wal.MasterNode.Should().NotBeEmpty();
        wal.ChainStartSegment.Should().NotBeEmpty();
        (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - wal.LastUploadedUnix!.Value)
            .Should().BeLessThan(120, "last_uploaded_unix свежий (поток жив)");

        // Assert 3 — цепочка непрерывна от chain_start (AC3-факт): поллинг —
        // одиночный list ловит момент частичной доставки (сегменты грузятся
        // последовательно, list между ними видит временную «дыру»).
        var chainStart = PgWorker.Backups.WalFileName.TryParse(wal.ChainStartSegment)!.Value;
        var continuous = await E2eFixture.WaitForAsync(async () =>
        {
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            if (!list.IsSuccess)
                return false;
            listed = [.. list.Value];
            var check = PgWorker.Backups.WalChain.Check(chainStart, listed.Select(o => o.Name));
            return check.IsContinuous;
        }, TimeSpan.FromSeconds(120), ct);
        continuous.Should().BeTrue("цепочка от chain_start обязана стать непрерывной");

        // Assert 4 — агент running (docker ps) + слот на мастере (AC1)
        var agents = await Fx.RunDockerAsync(
            ["ps", "--filter", $"name=pgw-backup-wal-{cluster}-shard1", "--format", "{{.Names}} {{.State}}"], ct);
        agents.Should().Contain($"pgw-backup-wal-{cluster}-shard1 running");
        await using var adminConn = new NpgsqlConnection(adminDsn);
        await adminConn.OpenAsync(ct);
        await using var slotCmd = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = @slot)",
            adminConn) { Parameters = { new() { ParameterName = "slot", Value = $"pgw_bkp_{cluster}_shard1" } } };
        ((bool)(await slotCmd.ExecuteScalarAsync(ct))!).Should().BeTrue("слот создан воркером на мастере (AC1)");

        // Assert 5 — каркасный парсер t01/t02 читает wal-ключ без parseErrors (AC2)
        var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
        var parsed = BackupsParser.Parse(kvs, out var parseErrors);
        parseErrors.Should().BeEmpty();
        parsed.Value.Should().Contain(b => b.Cluster == cluster);
    }

    // AAA: on_create-verify (AC1): COMPLETED → verify PENDING → verify-джоб
    // pgw-backup-verify-* → verify.state=OK + checked_unix; парсеры без parseErrors
    [Fact]
    public async Task Backup_Verify_Ok_OnCreate()
    {
        // Arrange — окружение с MinIO; кластер bkvrfy<тег прогона>; policy on_create=true
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-verify", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"bkvrfy{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"full_max_age_sec":86400,"verify":{"on_create":true,"interval_sec":0}}""", null, ct);
        await using var app = await StartBackupHostAsync("bkverify", ct);

        // Act 1 — фаза PENDING пройдена: в ключе PENDING (t02 пишет при COMPLETED)
        // и/или жив контейнер pgw-backup-verify-<C>-* (PENDING держится в ключе
        // до итога — стабильное условие; контейнер — свидетельство джоба)
        var sawPending = await E2eFixture.WaitForAsync(async () =>
        {
            if ((await FullKeysAsync(cluster, "shard1"))
                .Any(f => f.Value.Contains(""""verify":{"state":"PENDING"""")))
                return true;
            var containers = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-verify-{cluster}-"], ct);
            return containers.Length > 0;
        }, TimeSpan.FromSeconds(180), ct);
        sawPending.Should().BeTrue("on_create: verify обязан стартовать (PENDING в ключе / контейнер pgw-backup-verify-*)");

        // Act 2 — ждём verify.state=OK (бюджет 300 c: полный ~минуты + verify-скачивание)
        var verified = await E2eFixture.WaitForAsync(async () =>
            (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains(""""verify":{"state":"OK"""")),
            TimeSpan.FromSeconds(300), ct);

        // Assert 1 — OK + checked_unix (фаза PENDING зафиксирована Act 1)
        verified.Should().BeTrue("on_create: verify должен дойти до OK (AC1)");
        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(done.Value)!;
        status["verify"].GetProperty("state").GetString().Should().Be("OK");
        status["verify"].GetProperty("checked_unix").GetInt64().Should().BeGreaterThan(0);

        // Assert 2 — verify-джоб отработал и снесён (контейнер/volume-префиксы чисты)
        var cleaned = await E2eFixture.WaitForAsync(async () =>
        {
            var containers = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-verify-{cluster}-"], ct);
            var volumes = await Fx.RunDockerAsync(
                ["volume", "ls", "-q", "--filter", $"name=pgw-backup-verify-{cluster}-"], ct);
            return containers.Length == 0 && volumes.Length == 0;
        }, TimeSpan.FromSeconds(60), ct);
        cleaned.Should().BeTrue("verify-джоб и volume сносятся после итога (AC8)");

        // Assert 3 — воркерный и панельный парсеры читают без parseErrors (AC1);
        // панельный Kv — отдельный тип (AdminPanel.Etcd.Client.Kv), маппинг 1:1
        var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
        var parsed = BackupsParser.Parse(kvs, out var parseErrors);
        parseErrors.Should().BeEmpty();
        var panelKvs = kvs.Select(kv => new AdminPanel.Etcd.Client.Kv(kv.Key, kv.Value, kv.ModRevision)).ToList();
        var panel = AdminPanel.Etcd.Parsing.BackupsParser.Parse(panelKvs);
        panel.Errors.Should().BeEmpty();
        panel.Clusters.Single(c => c.Cluster == cluster).ShardVerifyFailures.Should().BeEmpty();
    }

    // AAA: порча набора (AC2): удалить объект из full/<id>/ в MinIO → policy
    // interval_sec мал → перепроверка → verify.state=FAILED + error; переснятия
    // в окне теста нет (full_max_age_sec велик)
    [Fact]
    public async Task Backup_Verify_Corruption_Fails()
    {
        // Arrange — окружение + кластер; interval_sec=5 форсирует перепроверку
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-corrupt", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"bkcrpt{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"full_max_age_sec":86400,"verify":{"on_create":true,"interval_sec":5}}""", null, ct);
        await using var app = await StartBackupHostAsync("bkcorrupt", ct);

        // Assert 1 — первый verify OK (как в маркере)
        var firstOk = await E2eFixture.WaitForAsync(async () =>
            (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains(""""verify":{"state":"OK"""")),
            TimeSpan.FromSeconds(300), ct);
        firstOk.Should().BeTrue("исходный набор валиден");
        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        var id = done.Key.Split('/').Last();

        // Act — портим: mc rm один объект из full/<id>/ (PG_VERSION из листинга набора)
        await Fx.RunDockerAsync(
            ["run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
                "-c", $"mc alias set t http://e2e-minio:9000 minioadmin minioadmin >/dev/null"
                      + $" && mc rm t/{Bucket}/{cluster}/shard1/full/{id}/PG_VERSION"], ct);

        // Assert 2 — перепроверка по interval → FAILED + error (бюджет 180 c:
        // interval 5 c + тик + джоб со скачиванием)
        var failed = await E2eFixture.WaitForAsync(async () =>
            (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains(""""verify":{"state":"FAILED"""")),
            TimeSpan.FromSeconds(180), ct);
        failed.Should().BeTrue("порча набора обязана дать verify FAILED (AC2)");
        var corrupted = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("FAILED"));
        var corruptedStatus = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(corrupted.Value)!;
        corruptedStatus["verify"].GetProperty("error").GetString().Should().NotBeNullOrEmpty("причина pg_verifybackup — в verify.error");

        // Assert 3 — в момент провала verify переснятие не УСПЕЛО завершиться.
        // Точная механика (задача 11): после verify FAILED валидных полных нет →
        // IsDue=true СРАЗУ; переснятие сдерживает только BackoffPassed (n растёт
        // и от verify-фейлов; окно Retry.BaseSec=2 c из StartBackupHostAsync) —
        // новая ПОПЫТКА (PLANNED/RUNNING-ключ) допустима, но полный снимается
        // минуты → COMPLETED в момент этого ассерта обязан быть один.
        (await FullKeysAsync(cluster, "shard1")).Count(f => f.Value.Contains("COMPLETED"))
            .Should().Be(1, "новый COMPLETED-полный не успевает появиться в момент провала verify");
    }

    // ===== Хелперы =====

    // Воркер с включённой подсистемой бэкапов: S3 на локальный MinIO,
    // образ pgworker-backup:e2e, ускоренный бэкофф.
    private Task<HostInstance> StartBackupHostAsync(
        string name, CancellationToken ct, string? s3EndpointOverride = null)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Endpoint"] = s3EndpointOverride ?? Fx.S3Endpoint,
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Backups__S3__SecretKey"] = "minioadmin",
            ["PgWorker__Backups__Job__Image"] = E2eEnvironment.JobImage,
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
        }, ct: ct);

    private async Task<IReadOnlyList<Kv>> FullKeysAsync(string cluster, string shard)
        => (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/{shard}/full/",
            TestContext.Current.CancellationToken)).Value;

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    // mc ls --recursive s3-пути кластера (mc в сети MinIO окружения — по алиасу).
    private async Task<string> McLsAsync(string path)
        => await Fx.RunDockerAsync(
        [
            "run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
            "-c", $"mc alias set t http://e2e-minio:9000 minioadmin minioadmin >/dev/null && mc ls --recursive t/{Bucket}/{path}",
        ], TestContext.Current.CancellationToken);

    // Сид кластера в стиле панели (копия E2eRotateScenarios.SeedClusterAsync).
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
    // Воркер с ВКЛЮЧЁННОЙ подсистемой бэкапов: t02-комплект джобов + t03 Wal-поток.
    // S3 для воркера — published порт (localhost), для агентов/джобов — advertised
    // (host.docker.internal); пороги WAL-контроля — короткие.
    private Task<HostInstance> StartWalHostAsync(string name, CancellationToken ct)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Endpoint"] = Fx.S3Endpoint.Replace(
                "host.docker.internal:", "localhost:", StringComparison.Ordinal),
            ["PgWorker__Backups__S3__AdvertisedEndpoint"] = Fx.S3Endpoint,
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Backups__S3__SecretKey"] = "minioadmin",
            ["PgWorker__Backups__Job__Image"] = E2eEnvironment.JobImage,
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
            ["PgWorker__Backups__Wal__VerifyIntervalSec"] = "2",
            ["PgWorker__Backups__Wal__StaleSec"] = "600",
            ["PgWorker__Backups__Wal__LagMaxSegments"] = "100000",
        }, ct: ct);

    // Published pg-порт мастера шарда из portalloc (host-клиент: localhost).
    // Резолв фактического primary — по пробам Patroni /primary (t02-подход
    // WaitForMasterAsync: master-ключ host:0 при EnableDoorman=false
    // недискриминантен); primary появляется после dsn/RUNNING — ждём.
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

    // Нагрузка под admin/superuser: CREATE TABLE + 30 циклов INSERT больших строк
    // + pg_switch_wal (superuser-only) — форсированное закрытие сегментов 16 МБ.
    private static async Task GenerateWalAsync(string adminDsn, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        await using (var create = new NpgsqlCommand(
            "CREATE TABLE IF NOT EXISTS wal_load(id bigserial, payload text)", conn))
            await create.ExecuteNonQueryAsync(ct);
        for (var i = 0; i < 30; i++)
        {
            await using var insert = new NpgsqlCommand(
                "INSERT INTO wal_load(payload) SELECT repeat('x', 1048576) FROM generate_series(1, 8)", conn);
            await insert.ExecuteNonQueryAsync(ct);
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
