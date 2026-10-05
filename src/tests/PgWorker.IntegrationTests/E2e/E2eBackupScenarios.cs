using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
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

        // Act/Assert 1 — первая попытка FAILED с error ≤ 480 c: окно включает
        // provisioning (4 ноды, минуты) + джоб (pg_basebackup со spread-чекпоинтом
        // может ждать ближайший чекпоинт) + мгновенный mc-отказ (connection refused
        // mc не ретраит). 300 c на загруженном хосте не хватает (факт t04-гейта).
        var failed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("FAILED")),
            TimeSpan.FromSeconds(480), ct);
        failed.Should().BeTrue("попытка с недоступным S3 должна упасть в FAILED");
        var failedKv = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("FAILED"));
        var status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(failedKv.Value)!;
        status["error"].GetString().Should().NotBeNullOrEmpty("причина фиксируется в статусе");

        // Assert 2 — переснятие: вторая попытка с ДРУГИМ id (Retry BaseSec=2)
        var retried = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Count >= 2,
            TimeSpan.FromSeconds(240), ct);
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


    // AAA: WAL-поток (t03/t27, AC1/AC8): provisioned кластер + INSERT/pg_switch_wal
    // → сегменты в MinIO, ключ wal ACTIVE с chain_start/last_uploaded, цепочка
    // непрерывна, running-агент мастера, слот pgw_bkp_<C>_<X> на мастере, поле
    // agents в ключе (супервиз-факты per-node, t27 §3.5).
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
        var (_, pgPort, masterNodeUp) = await MasterPgAsync(cluster, "shard1", ct);
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

        // Assert 4 — running-агент МАСТЕРА (per-node имя t27) + слот на мастере (AC1)
        var (_, _, masterNode) = await MasterPgAsync(cluster, "shard1", ct);
        var agents = await Fx.RunDockerAsync(
            ["ps", "--filter", $"name=pgw-backup-wal-{cluster}-shard1-", "--format", "{{.Names}} {{.State}}"], ct);
        agents.Should().Contain($"pgw-backup-wal-{cluster}-shard1-{masterNode} running");
        // t27 §3.5: ключ wal несёт супервиз-факты agents (мастер running).
        // Перечитываем ключ поллингом: контроль пишет wal, супервиз добавляет
        // agents тем же тиком — снапшот Assert 2 может быть пойман между двумя
        // put (гонка живого потока, как в Assert 2).
        var agentsInKey = await E2eFixture.WaitForAsync(async () =>
        {
            var read = await writer.ReadAsync(cluster, "shard1", ct);
            return read.IsSuccess && read.Value?.Agents is { } facts
                   && facts.Any(a =>
                       a.Node == masterNode && a.State == PgWorker.Etcd.Parsing.WalAgentPresence.Running);
        }, TimeSpan.FromSeconds(30), ct);
        agentsInKey.Should().BeTrue("agents-факты пишутся супервизом (t27): мастер running");
        await using var adminConn = new NpgsqlConnection(adminDsn);
        await adminConn.OpenAsync(ct);
        await using var slotCmd = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = @slot)",
            adminConn) { Parameters = { new() { ParameterName = "slot", Value = $"pgw_bkp_{cluster}_shard1" } } };
        ((bool)(await slotCmd.ExecuteScalarAsync(ct))!).Should().BeTrue("слот создан воркером на мастере (AC1)");

        // Assert 5 — каркасный парсер t01/t02 читает wal-ключ без parseErrors (AC2)
        var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
        var parsed = BackupsParser.Parse(kvs, out var parseErrors);
        parseErrors.Should().BeEmpty(
            $"парсер обязан читать ключи воркера (в т.ч. wal с agents t27): " +
            $"ключи=[{string.Join("; ", kvs.Select(k => k.Key))}] ошибки=[{string.Join("; ", parseErrors)}]");
        parsed.Value.Should().Contain(b => b.Cluster == cluster);
    }

    // ── t27 Ф4: E2E двойной архивации (AC2–AC5; каждый Fact — свой guid-контур) ──

    // Фоновая WAL-нагрузка: цикл INSERT+pg_switch_wal до отмены (пересоздание
    // агента/смерть мастера не должны рвать цепочку). Возвращает Task — гасится ct.
    private static async Task BackgroundWalLoadAsync(string adminDsn, CancellationToken ct)
    {
        try
        {
            await using var conn = new NpgsqlConnection(adminDsn);
            await conn.OpenAsync(ct);
            await using (var create = new NpgsqlCommand(
                "CREATE TABLE IF NOT EXISTS wal_load(id bigserial, payload text)", conn))
                await create.ExecuteNonQueryAsync(ct);
            while (!ct.IsCancellationRequested)
            {
                await using var insert = new NpgsqlCommand(
                    "INSERT INTO wal_load(payload) SELECT repeat('x', 1048576) FROM generate_series(1, 4)", conn);
                await insert.ExecuteNonQueryAsync(ct);
                await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
                await switchWal.ExecuteScalarAsync(ct);
                // Раз в ~15 c: сегменты закрываются ЗАПОЛНЕННЫМИ (сотни пустых
                // 16 MiB-put'ов секундного цикла давали гигабайты перекладки).
                await Task.Delay(15000, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // штатная остановка нагрузки
        }
        catch (Exception) when (ct.IsCancellationRequested)
        {
            // соединение могло умереть вместе со сценарием — не маскируем отмену
        }
    }

    // Контейнер-id агента (docker ps -a): для сверки «контейнер новый» после рескрета.
    private async Task<string?> AgentContainerIdAsync(string agentName, CancellationToken ct)
    {
        var ids = await Fx.RunDockerAsync(
            ["ps", "-a", "--format", "{{.ID}}", "--filter", $"name=^{agentName}$"], ct);
        return ids.Split('\n', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault()?.Trim();
    }

    // TLI текущего primary шарда: Patroni GET /cluster ноды (timeline члена-лидера).
    private async Task<uint> PrimaryTimelineAsync(string cluster, string shard, CancellationToken ct)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        kv.Value.Should().NotBeNull("portalloc пишется при provisioning");
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!
            .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal)).ToList();
        // Опрос ВСЕХ нод (после docker stop мастер-ноды её patroni-порт закрыт);
        // ждём именно TLI ≥ 2: promote мог случиться только-что — /cluster члена
        // показывает его timeline не мгновенно (длительность failover-процесса).
        uint tli = 0;
        await E2eFixture.WaitForAsync(async () =>
        {
            foreach (var (key, addr) in entries)
            {
                try
                {
                    using var response = await PatroniHttp.GetAsync(
                        $"http://localhost:{addr.GetProperty("patroni").GetInt32()}/cluster", ct);
                    if (!response.IsSuccessStatusCode) continue;
                    using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
                    foreach (var member in doc.RootElement.GetProperty("members").EnumerateArray())
                    {
                        var role = member.GetProperty("role").GetString();
                        if (role is "master" or "leader" or "primary"
                            && member.TryGetProperty("timeline", out var t)
                            && t.TryGetUInt32(out var value)
                            && value > tli)
                        {
                            tli = value;
                        }
                    }
                }
                catch (Exception)
                {
                    // нода ещё не готова — следующая
                }
            }

            return tli >= 2;
        }, TimeSpan.FromSeconds(120), ct);
        return tli;
    }

    /// <summary>Гарантированный дамп диагностики Wal-сценария В САМОМ catch (до
    /// teardown): агенты, wal/work-ключи, Patroni, хвост host.log воркера —
    /// переживает любые dispose-порядки (файл /tmp/pgw-diag-&lt;cluster&gt;.txt).</summary>
    private async Task WalScenarioDiagDumpAsync(string cluster, string reason)
    {
        try
        {
            var agentsPs = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}} {{.State}}",
                 "--filter", $"name=pgw-backup-wal-{cluster}-"], TestContext.Current.CancellationToken);
            var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var claimKv = await GetOrNullAsync($"/pgworker/claims/{cluster}");
            var patroni = await PatroniClusterDumpAsync(cluster, TestContext.Current.CancellationToken);
            var hostLog = "";
            foreach (var host in Fx.Hosts)
                try
                {
                    var lines = File.ReadAllLines(Path.Combine(host.SnapshotsDir, "host.log"));
                    var noise = new[]
                    {
                        "ClientHandler", "LogicalHandler", "HTTP request", "HTTP response",
                        "End processing HTTP", "Received HTTP", "Sending HTTP",
                        "PgtuneInputsFactory", "wal_compression", "AwaitableSocketAsyncEventArgs",
                    };
                    var interesting = lines
                        .Where(l => !noise.Any(n => l.Contains(n, StringComparison.Ordinal)))
                        .Where(l => l.Contains("backup", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("wal", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("agent", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("error", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("warn", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("fail", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("exception", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("claim", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("supervise", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("repair", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("evacuat", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("provision", StringComparison.OrdinalIgnoreCase))
                        .ToList();
                    var opLines = lines
                        .Where(l => l.Contains("backup-wal", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("WalStreamProcess", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("supervise", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("repair", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("provision ", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("эвакуа", StringComparison.OrdinalIgnoreCase)
                            || l.Contains("REBUILDING", StringComparison.OrdinalIgnoreCase))
                        .ToList();
                    hostLog += $"\n== {host.Name} OPS({opLines.Count}):\n{string.Join("\n", opLines)}" +
                        $"\nTAIL({interesting.Count}):\n" +
                        string.Join("\n", interesting[^Math.Min(40, interesting.Count)..]);
                }
                catch (Exception e)
                {
                    hostLog += $"\n== {host.Name}: host.log недоступен: {e.Message}";
                }

            var agentLogs = "";
            foreach (var line in agentsPs.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                var name = line.Split(' ')[0];
                var logs = await Fx.RunDockerAsync(["logs", "--tail", "60", name],
                    TestContext.Current.CancellationToken);
                agentLogs += $"\n== {name}:\n{logs}";
            }

            var dump =
                $"reason={reason}\nagents=[{agentsPs.Replace('\n', ';')}]\n" +
                $"wal=[{walKv?.Value ?? "-"}]\nwork=[{workKv?.Value ?? "-"}]\n" +
                $"claim=[{claimKv?.Value ?? "-"}]\npatroni={patroni}\nAGENT.LOGS:{agentLogs}\nHOST.LOG:{hostLog}\n";
            await File.WriteAllTextAsync($"/tmp/pgw-diag-{cluster}.txt", dump);
            Console.WriteLine($"[DIAG] дамп: /tmp/pgw-diag-{cluster}.txt");
        }
        catch
        {
            // дамп — лучшие усилия, не маскируем исходный сбой сценария
        }
    }

    // Диагностика Patroni-контекста (sync-выбор): GET /cluster первой ноды шарда.
    private async Task<string> PatroniClusterDumpAsync(string cluster, CancellationToken ct)
    {
        try
        {
            var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
            var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!
                .Where(p => p.Key.StartsWith("shard1/", StringComparison.Ordinal)).ToList();
            foreach (var (key, addr) in entries)
            {
                try
                {
                    using var response = await PatroniHttp.GetAsync(
                        $"http://localhost:{addr.GetProperty("patroni").GetInt32()}/cluster", ct);
                    if (!response.IsSuccessStatusCode) continue;
                    var body = await response.Content.ReadAsStringAsync(ct);
                    return $"[{key}] " + body[..Math.Min(700, body.Length)];
                }
                catch (Exception)
                {
                    // нода недоступна — следующая
                }
            }

            return "<ни одна нода shard1 не отвечает /cluster>";
        }
        catch (Exception e)
        {
            return $"<patroni недоступен: {e.Message}>";
        }
    }

    /// <summary>Подъём полного WAL-контура сценария: окружение с MinIO, кластер,
    /// воркер (Backups:Enabled + Wal:AgentImage), ожидание provisioning, DSN мастера.
    /// </summary>
    // Владение окружением и воркером — у Fact (DisposeAsync при любом исходе);
    // хелпер только поднимает: воркер, отданный наружу, гасится вызывающим.
    private async Task<(E2eEnvironment Env, HostInstance App, string Cluster, string AdminDsn, string MasterNode)>
        StartWalScenarioAsync(string slug, string clusterPrefix, CancellationToken ct)
    {
        var phaseStart = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        Console.WriteLine($"[PHASE] {slug}: start (unix={phaseStart})");
        var fx = await E2eEnvironment.StartAsync(slug, withMinio: true, ct: ct);
        Fx = fx;
        Console.WriteLine($"[PHASE] {slug}: окружение поднято за " +
            (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - phaseStart) + " c (вкл. Release-сборку и образы)");
        var cluster = $"{clusterPrefix}{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        var app = await StartWalHostAsync(slug, ct);
        var provisioned = await E2eFixture.WaitForAsync(
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning обязан дойти до DONE до WAL-нагрузки");
        Console.WriteLine($"[PHASE] {slug}: provisioning DONE за " +
            (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - phaseStart) + " c от старта");
        var (_, pgPort, masterNode) = await MasterPgAsync(cluster, "shard1", ct);
        var adminDsn = DatabaseProvisioner.BuildAdminDsn("localhost", pgPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
        return (fx, app, cluster, adminDsn, masterNode);
    }

    // AAA (AC2 — ядро задачи): под WAL-нагрузкой docker rm -f агента мастера —
    // супервиз пересоздаёт агента, цепочка S3 непрерывна, BROKEN не возникает,
    // ключ возвращается в ACTIVE (потеря недоставленного невозможна по построению:
    // слот удерживает WAL, новый агент досыпает от хвоста S3).
    [Fact]
    public async Task WalStream_AgentRecreate_UnderLoad_NoGap()
    {
        // Arrange — окружение, кластер, воркер, фоновая нагрузка ≥ 2 мин
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-ac2", "shopac2", ct);
        await using var envOwner = fx;
        await using var appOwner = app;
        using var load = new CancellationTokenSource();
        var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
        var hostEndpoint = Fx.S3Endpoint.Replace(
            "host.docker.internal:", "localhost:", StringComparison.Ordinal);
        await using var backupS3 = new PgWorker.Backups.BackupS3(
            new PgWorker.Backups.BackupsRuntimeOptions
            {
                Enabled = true,
                S3Endpoint = hostEndpoint,
                S3Bucket = Bucket,
                S3AccessKey = "minioadmin",
                S3SecretKey = "minioadmin",
                S3PathStyle = true,
            });
        var agentName = $"pgw-backup-wal-{cluster}-shard1-{masterNode}";
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);

        try
        {
            // Дождаться первой доставки (агент жив, ≥1 сегмент в S3)
            var firstListCount = 0;
            var firstList = await E2eFixture.WaitForAsync(async () =>
            {
                var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                if (!list.IsSuccess || list.Value.Count == 0)
                    return false;
                firstListCount = list.Value.Count;
                return true;
            }, TimeSpan.FromSeconds(180), ct);
            if (!firstList)
            {
                // Диагностика провала первой доставки (канон e2e-launch: «что
                // произошло» — без перезапуска): агенты, журнал воркера, wal-ключ,
                // хвост host.log (HostInstance ещё жив — teardown в dispose).
                var agentsPs = await Fx.RunDockerAsync(
                    ["ps", "-a", "--format", "{{.Names}} {{.State}}",
                     "--filter", $"name=pgw-backup-wal-{cluster}-"], ct);
                var agentLogs = "";
                foreach (var line in agentsPs.Split('\n', StringSplitOptions.RemoveEmptyEntries))
                {
                    var name = line.Split(' ')[0];
                    var logs = await Fx.RunDockerAsync(["logs", "--tail", "25", name], ct);
                    agentLogs += $"\n== {name}: {logs[..Math.Min(900, logs.Length)]}";
                }

                var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
                var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
                var claimKv = await GetOrNullAsync($"/pgworker/claims/{cluster}");
                var hostLog = Fx.Hosts is { Count: > 0 }
                    ? string.Join("\n", Fx.Hosts.Select(h =>
                    {
                        try
                        {
                            // Значимые строки (без etcd-HTTP-шума): backup/wal/агент/ошибки
                            var lines = File.ReadAllLines(Path.Combine(h.SnapshotsDir, "host.log"));
                            var interesting = lines.Where(l =>
                                !l.Contains("HTTP request") && !l.Contains("HTTP response")
                                && !l.Contains("End processing HTTP")
                                && !l.Contains("PgtuneInputsFactory")
                                && !l.Contains("wal_compression")
                                && !l.Contains("Connection reset by peer")
                                && !l.Contains("AwaitableSocketAsyncEventArgs"))
                                .Where(l => l.Contains("backup", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("wal", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("agent", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("error", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("warn", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("fail", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("exception", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("claim", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("supervise", StringComparison.OrdinalIgnoreCase))
                                .ToList();
                            // op-логи (их немного, они решают «почему молчит подсистема»)
                            var opLines = lines
                                .Where(l => l.Contains("backup-wal", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("WalStreamProcess", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("pgw-backup-wal", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("supervise", StringComparison.OrdinalIgnoreCase)
                                    || l.Contains("клэйм", StringComparison.OrdinalIgnoreCase))
                                .ToList();
                            // Полный хвост (120 строк, без фильтров) — полный контекст
                            return $"[{h.Name}] OPS({opLines.Count}):\n{string.Join("\n", opLines)}" +
                                $"\nFULLTAIL:\n" +
                                string.Join("\n", lines[^Math.Min(120, lines.Length)..]);
                        }
                        catch (Exception e)
                        {
                            return $"<host.log недоступен: {e.Message}>";
                        }
                    }))
                    : "<hosts нет>";
                throw new ApplicationException(
                    "первая доставка не случилась за бюджет: " +
                    $"agents=[{agentsPs.Replace('\n', ';')}] wal=[{walKv?.Value ?? "-"}] " +
                    $"journal=[{workKv?.Value?[..Math.Min(400, workKv.Value?.Length ?? 0)]}] " +
                    $"claim=[{claimKv?.Value ?? "-"}] {agentLogs}\nHOST.LOG:\n{hostLog}");
            }

            firstList.Should().BeTrue("первая доставка до пересоздания");
            firstListCount.Should().BeGreaterThan(0);
            var idBefore = await AgentContainerIdAsync(agentName, ct);
            idBefore.Should().NotBeNullOrEmpty($"агент мастера {agentName} обязан быть жив");

            // Act — docker rm -f агента мастера ПОД нагрузкой
            Console.WriteLine($"[PHASE] wal-ac2: docker rm -f {agentName}");
            await Fx.RunDockerAsync(["rm", "-f", agentName], ct);

            // Assert — агент пересоздан супервизом (то же имя, контейнер новый)
            var recreated = await E2eFixture.WaitForAsync(async () =>
            {
                var id = await AgentContainerIdAsync(agentName, ct);
                return id is not null && id != idBefore;
            }, TimeSpan.FromSeconds(120), ct);
            recreated.Should().BeTrue("супервиз тика обязан пересоздать агента (restart-policy no)");

            // ≥1 новый сегмент после рескрета; цепочка непрерывна (WalChain union)
            var grew = await E2eFixture.WaitForAsync(async () =>
            {
                var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                return list.IsSuccess && list.Value.Count > firstListCount;
            }, TimeSpan.FromSeconds(120), ct);
            grew.Should().BeTrue("новый агент досыпает от хвоста S3 — доставка продолжается");

            // Ключ: BROKEN НЕ возникал весь сценарий (поллинг срезом); финал ACTIVE
            PgWorker.Etcd.Parsing.WalStreamStatus? sawStatus = null;
            var backActive = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                if (!read.IsSuccess || read.Value is null)
                    return false;
                sawStatus = read.Value.State;
                if (sawStatus == PgWorker.Etcd.Parsing.WalStreamStatus.Broken)
                    return true; // немедленный фейл ниже
                return sawStatus == PgWorker.Etcd.Parsing.WalStreamStatus.Active;
            }, TimeSpan.FromSeconds(120), ct);
            sawStatus.Should().NotBe(
                PgWorker.Etcd.Parsing.WalStreamStatus.Broken,
                "доставка от хвоста S3 исключает дыру — BROKEN запрещён (AC2)");
            backActive.Should().BeTrue("ключ wal возвращается в ACTIVE");
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            list.IsSuccess.Should().BeTrue();
            var wal = await writer.ReadAsync(cluster, "shard1", ct);
            wal.Value.Should().NotBeNull();
            var chain = PgWorker.Backups.WalChain.Check(
                PgWorker.Backups.WalFileName.TryParse(wal.Value!.ChainStartSegment)!.Value,
                list.Value.Select(o => o.Name));
            chain.IsContinuous.Should().BeTrue(chain.GapError ?? "цепочка непрерывна");
        }
        catch (Exception ex)
        {
            Fx.MarkFailed(); // артефакты телеметрии переживают teardown (канон e2e-launch)
            await WalScenarioDiagDumpAsync(cluster, "AC2: " + ex.Message);
            throw;
        }
        finally
        {
            await load.CancelAsync();
            try
            {
                await loader; // нагрузка гасится отменой
            }
            catch
            {
                // фон уже умер (соединение/отмена) — сценарий завершается
            }
        }
    }

    // AAA (AC3-E2E): двухнодовый шард (sync-standby появится сам, SyncStrict=true) —
    // ДВА running-агента (мастер + sync), оба пишут в один префикс wal/, цепочка
    // непрерывна (union), ключ ACTIVE с agents обеих нод, слоты на ОБОИХ нодах.
    [Fact]
    public async Task WalStream_DualArchiving_TwoAgents_OnePrefix()
    {
        // Arrange
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-ac3", "shopac3", ct);
        await using var envOwner = fx;
        await using var appOwner = app;
        var hostEndpoint = Fx.S3Endpoint.Replace(
            "host.docker.internal:", "localhost:", StringComparison.Ordinal);
        await using var backupS3 = new PgWorker.Backups.BackupS3(
            new PgWorker.Backups.BackupsRuntimeOptions
            {
                Enabled = true,
                S3Endpoint = hostEndpoint,
                S3Bucket = Bucket,
                S3AccessKey = "minioadmin",
                S3SecretKey = "minioadmin",
                S3PathStyle = true,
            });
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);

        // WAL-нагрузка ПЕРВАЯ: Patroni выбирает sync-реплику при живом потоке
        // репликации — без нагрузки sync не выберется и второй агент не поднимется.
        await GenerateWalAsync(adminDsn, ct);
        using var load = new CancellationTokenSource();
        var loader = BackgroundWalLoadAsync(adminDsn, load.Token);

        try
        {
        // Act — ждём ДВА running-агента (sync-standby появится сам) ≤ 300 c
        var bothAgents = await E2eFixture.WaitForAsync(async () =>
        {
            var ps = await Fx.RunDockerAsync(
            [
                "ps", "--filter", $"name=pgw-backup-wal-{cluster}-shard1-",
                "--format", "{{.Names}} {{.State}}",
            ], ct);
            return ps.Split('\n', StringSplitOptions.RemoveEmptyEntries).Count(l => l.EndsWith("running")) >= 2;
        }, TimeSpan.FromSeconds(300), ct);
        if (!bothAgents)
        {
            var agentsPs = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}} {{.State}}",
                 "--filter", $"name=pgw-backup-wal-{cluster}-"], ct);
            var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var patroniState = await PatroniClusterDumpAsync(cluster, ct);
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            throw new ApplicationException(
                "два агента не поднялись: " +
                $"agents=[{agentsPs.Replace('\n', ';')}] wal=[{walKv?.Value?[..Math.Min(600, walKv.Value?.Length ?? 0)]}] " +
                $"work=[{workKv?.Value?[..Math.Min(1200, workKv.Value?.Length ?? 0)]}] " +
                $"patroni={patroniState[..Math.Min(700, patroniState.Length)]}");
        }

        bothAgents.Should().BeTrue("двойная архивация: агенты мастера и sync-standby (AC3)");

        // Нагрузка + доставка: сегменты растут, ключ ACTIVE, agents — обе ноды
        await GenerateWalAsync(adminDsn, ct);
        var delivered = await E2eFixture.WaitForAsync(async () =>
        {
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            if (!list.IsSuccess || list.Value.Count < 2) return false;
            var read = await writer.ReadAsync(cluster, "shard1", ct);
            if (!read.IsSuccess || read.Value is not { State: PgWorker.Etcd.Parsing.WalStreamStatus.Active })
                return false;
            // agents — обе ноды running
            var nodes = read.Value.Agents?
                .Where(a => a.State == PgWorker.Etcd.Parsing.WalAgentPresence.Running)
                .Select(a => a.Node).ToHashSet();
            return nodes is { Count: >= 2 };
        }, TimeSpan.FromSeconds(180), ct);
        delivered.Should().BeTrue("ключ ACTIVE + agents обеих нод (AC3)");

        // Цепочка непрерывна от chain_start (union одного префикса)
        var wal = (await writer.ReadAsync(cluster, "shard1", ct)).Value!;
        var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
        list.IsSuccess.Should().BeTrue();
        var chain = PgWorker.Backups.WalChain.Check(
            PgWorker.Backups.WalFileName.TryParse(wal.ChainStartSegment)!.Value,
            list.Value.Select(o => o.Name));
        chain.IsContinuous.Should().BeTrue(chain.GapError ?? "union-цепочка непрерывна");

        // Слоты pgw_bkp_<C>_shard1 на ОБОИХ нодах (published pg-порты из portalloc)
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}", ct);
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!
            .Where(p => p.Key.StartsWith("shard1/", StringComparison.Ordinal)).ToList();
        foreach (var (key, addr) in entries)
        {
            var dsn = DatabaseProvisioner.BuildAdminDsn("localhost", addr.GetProperty("pg").GetInt32(),
                cluster, new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await using var conn = new NpgsqlConnection(dsn);
            await conn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(
                "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = @slot)", conn)
            {
                Parameters = { new() { ParameterName = "slot", Value = $"pgw_bkp_{cluster}_shard1" } },
            };
            ((bool)(await cmd.ExecuteScalarAsync(ct))!)
                .Should().BeTrue($"слот существует на ноде {key} (AC3, per-instance)");
        }
        }
        catch (Exception ex)
        {
            Fx.MarkFailed(); // артефакты телеметрии переживают teardown
            await WalScenarioDiagDumpAsync(cluster, "AC3: " + ex.Message);
            throw;
        }
        finally
        {
            await load.CancelAsync();
            try
            {
                await loader;
            }
            catch
            {
                // фон умер вместе с соединением — не маскируем исход
            }
        }
    }

    // AAA (AC4): остановка мастера — агент sync-реплики продолжает цепочку без дыр;
    // после promote воркер пересоздаёт агентов на новых ролях; ключ не BROKEN.
    [Fact]
    public async Task WalStream_MasterDown_ReplicaAgentContinues()
    {
        // Arrange — окружение, оба агента живы, нагрузка фоном
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-ac4", "shopac4", ct);
        await using var envOwner = fx;
        await using var appOwner = app;
        var hostEndpoint = Fx.S3Endpoint.Replace(
            "host.docker.internal:", "localhost:", StringComparison.Ordinal);
        await using var backupS3 = new PgWorker.Backups.BackupS3(
            new PgWorker.Backups.BackupsRuntimeOptions
            {
                Enabled = true,
                S3Endpoint = hostEndpoint,
                S3Bucket = Bucket,
                S3AccessKey = "minioadmin",
                S3SecretKey = "minioadmin",
                S3PathStyle = true,
            });
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);
        var bothAgents = await E2eFixture.WaitForAsync(async () =>
        {
            var ps = await Fx.RunDockerAsync(
            [
                "ps", "--filter", $"name=pgw-backup-wal-{cluster}-shard1-",
                "--format", "{{.Names}} {{.State}}",
            ], ct);
            return ps.Split('\n', StringSplitOptions.RemoveEmptyEntries).Count(l => l.EndsWith("running")) >= 2;
        }, TimeSpan.FromSeconds(300), ct);
        bothAgents.Should().BeTrue("оба агента до смерти мастера");
        using var load = new CancellationTokenSource();
        var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
        var beforeList = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
        beforeList.IsSuccess.Should().BeTrue();

        try
        {
            // Act — жёсткая потеря мастер-ноды (multi-host-риск «машина отключилась
            // целиком»): никто не делает управляемый switchover — Patroni сам
            // проводит failover силами выжившей реплики.
            var (_, _, currentMaster) = await MasterPgAsync(cluster, "shard1", ct);
            Console.WriteLine($"[PHASE] wal-ac4: docker stop pgw-{cluster}-shard1-{currentMaster}");
            await Fx.RunDockerAsync(["stop", $"pgw-{cluster}-shard1-{currentMaster}"], ct);
            var (_, _, newMasterNode) = await MasterPgAsync(cluster, "shard1", ct);
            newMasterNode.Should().NotBe(currentMaster, "после смерти мастера primary — реплика");
            Console.WriteLine($"[PHASE] wal-ac4: новый primary {newMasterNode}");

            // Фаза стабилизации: надзор пометил упавшую ноду unreachable и чинит её
            // repair-контуром (восстановление ноды — продуктовое поведение, минуты);
            // WalStream честно уходит в restore-гвард, пока шард восстанавливается.
            // Ждём снятия unreachable циклом 30-секундных окон (каждое окно —
            // проверка факта; суммарный лимит 600 c на восстановление ноды).
            TestContext.Current.TestOutputHelper?.WriteLine(
                "[PHASE] wal-ac4: ожидание снятия unreachable (repair-контур надзора)");
            var repairDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(600);
            while (DateTime.UtcNow < repairDeadline)
            {
                var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
                var stillUnreachable = workKv?.Value?.Contains("unreachable") == true;
                if (!stillUnreachable)
                    break;
                await Task.Delay(TimeSpan.FromSeconds(30), ct);
            }
            TestContext.Current.TestOutputHelper?.WriteLine(
                "[PHASE] wal-ac4: repair-фаза завершена/снята — замеры ключа");

            // Assert — доставка продолжается (новые сегменты после смерти мастера),
            // цепочка непрерывна (CheckWithRestart), ключ не BROKEN
            var grew = await E2eFixture.WaitForAsync(async () =>
            {
                var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                return list.IsSuccess && list.Value.Count > beforeList.Value.Count;
            }, TimeSpan.FromSeconds(300), ct);
            grew.Should().BeTrue("агент реплики продолжает доставку после смерти мастера");

            // Чтение ключа — поллингом (etcd-транспорт может дать transient-отказ:
            // ReadAsync Failed → Value null; факт ждём, не фиксируем одноразовым чтением)
            PgWorker.Etcd.Parsing.WalStreamState? keyState = null;
            var keySeen = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                if (!read.IsSuccess || read.Value is null)
                    return false;
                keyState = read.Value;
                return true;
            }, TimeSpan.FromSeconds(30), ct);
            keySeen.Should().BeTrue("wal-ключ обязан существовать после failover");
            keyState!.State.Should().NotBe(
                PgWorker.Etcd.Parsing.WalStreamStatus.Broken, "ключ не BROKEN (AC4)");
            var list = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
            var chain = PgWorker.Backups.WalChain.CheckWithRestart(
                PgWorker.Backups.WalFileName.TryParse(keyState!.ChainStartSegment)!.Value,
                list.Value!.Select(o => o.Name));
            chain.IsContinuous.Should().BeTrue(chain.GapError ?? "цепочка склеена через TLI");

            // После promote воркер пересоздаёт агентов на новых ролях: running-агент
            // новой мастер-ноды ≤ 300 c (смена источника — пересоздание).
            var newAgent = await E2eFixture.WaitForAsync(async () =>
            {
                var ps = await Fx.RunDockerAsync(
                    ["ps", "--format", "{{.Names}} {{.State}}",
                     "--filter", $"name=pgw-backup-wal-{cluster}-shard1-{newMasterNode}"], ct);
                return ps.Contains("running");
            }, TimeSpan.FromSeconds(300), ct);
            newAgent.Should().BeTrue($"агент новой мастер-ноды {newMasterNode} running");
        }
        catch (Exception ex)
        {
            Fx.MarkFailed(); // артефакты телеметрии переживают teardown
            await WalScenarioDiagDumpAsync(cluster, "AC4: " + ex.Message);
            throw;
        }
        finally
        {
            await load.CancelAsync();
            try
            {
                await loader;
            }
            catch
            {
                // фон умер вместе с соединением к мёртвому мастеру — ожидаемо
            }
        }
    }

    // AAA (AC5): promote реплики — TLI ≥ 2; объект wal/<tli>.history в S3
    // (приёмник грузит TIMELINE_HISTORY — вердикт Ф2); цепочка CheckWithRestart
    // непрерывна; ключ ACTIVE.
    [Fact]
    public async Task WalStream_Promote_TliHistory_ChainGlued()
    {
        // Arrange — окружение, оба агента, нагрузка, смерть мастера → promote
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var (fx, app, cluster, adminDsn, masterNode) = await StartWalScenarioAsync("wal-ac5", "shopac5", ct);
        await using var envOwner = fx;
        await using var appOwner = app;
        var hostEndpoint = Fx.S3Endpoint.Replace(
            "host.docker.internal:", "localhost:", StringComparison.Ordinal);
        await using var backupS3 = new PgWorker.Backups.BackupS3(
            new PgWorker.Backups.BackupsRuntimeOptions
            {
                Enabled = true,
                S3Endpoint = hostEndpoint,
                S3Bucket = Bucket,
                S3AccessKey = "minioadmin",
                S3SecretKey = "minioadmin",
                S3PathStyle = true,
            });
        var writer = new PgWorker.Backups.WalStatusWriter(G, [Endpoint]);
        var bothAgents = await E2eFixture.WaitForAsync(async () =>
        {
            var ps = await Fx.RunDockerAsync(
            [
                "ps", "--filter", $"name=pgw-backup-wal-{cluster}-shard1-",
                "--format", "{{.Names}} {{.State}}",
            ], ct);
            return ps.Split('\n', StringSplitOptions.RemoveEmptyEntries).Count(l => l.EndsWith("running")) >= 2;
        }, TimeSpan.FromSeconds(300), ct);
        bothAgents.Should().BeTrue("оба агента до promote");
        using var load = new CancellationTokenSource();
        var loader = BackgroundWalLoadAsync(adminDsn, load.Token);
        await GenerateWalAsync(adminDsn, ct); // база цепочки TLI 1 до promote

        try
        {
            // Act — жёсткая потеря мастер-ноды: Patroni failover реплики открывает
            // новый timeline (механика .history та же, что при любом promote)
            var (_, _, currentMaster) = await MasterPgAsync(cluster, "shard1", ct);
            Console.WriteLine($"[PHASE] wal-ac5: docker stop pgw-{cluster}-shard1-{currentMaster}");
            await Fx.RunDockerAsync(["stop", $"pgw-{cluster}-shard1-{currentMaster}"], ct);
            await MasterPgAsync(cluster, "shard1", ct); // ждём новый primary
            var tli = await PrimaryTimelineAsync(cluster, "shard1", ct);
            tli.Should().BeGreaterThanOrEqualTo(2u, "failover открывает новый timeline");

            // Assert — .history нового TLI в S3 (приёмник запрашивает TIMELINE_HISTORY)
            var history = await E2eFixture.WaitForAsync(async () =>
            {
                var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                return listed.IsSuccess && listed.Value.Any(o => o.Name == $"{tli:x8}.history");
            }, TimeSpan.FromSeconds(300), ct);
            history.Should().BeTrue($"wal/{tli:x8}.history загружен приёмником (AC5)");

            // Цепочка склеена через TLI-переход; ключ ACTIVE
            var glueOk = await E2eFixture.WaitForAsync(async () =>
            {
                var read = await writer.ReadAsync(cluster, "shard1", ct);
                if (!read.IsSuccess || read.Value is null) return false;
                var listed = await backupS3.ListWalAsync(cluster, "shard1", ct: ct);
                if (!listed.IsSuccess || listed.Value.Count == 0) return false;
                var chainStart = PgWorker.Backups.WalFileName.TryParse(
                    read.Value.ChainStartSegment);
                if (chainStart is null) return false;
                var chain = PgWorker.Backups.WalChain.CheckWithRestart(
                    chainStart.Value, listed.Value.Select(o => o.Name));
                return chain.IsContinuous
                    && read.Value.State == PgWorker.Etcd.Parsing.WalStreamStatus.Active;
            }, TimeSpan.FromSeconds(300), ct);
            glueOk.Should().BeTrue("цепочка CheckWithRestart непрерывна, ключ ACTIVE (AC5)");
        }
        catch (Exception ex)
        {
            Fx.MarkFailed(); // артефакты телеметрии переживают teardown
            await WalScenarioDiagDumpAsync(cluster, "AC5: " + ex.Message);
            throw;
        }
        finally
        {
            await load.CancelAsync();
            try
            {
                await loader;
            }
            catch
            {
                // фон умер вместе с соединением — ожидаемо после смерти мастера
            }
        }
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
        // Kv общий (Shared.Etcd.Client, t08) — тот же набор подаётся в оба парсера
        var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
        var parsed = BackupsParser.Parse(kvs, out var parseErrors);
        parseErrors.Should().BeEmpty();
        var panel = AdminPanel.Etcd.Parsing.BackupsParser.Parse(kvs);
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

    // ===== Дрилл восстановимости (reliability t02, spec Фаза 5) =====

    // Минимальный снапшот панели для правил дрилла («панельный алерт» в E2E:
    // фактические kvs кластера кормят правило — образец панельных юнитов).
    private static AdminPanel.Core.EtcdSnapshot SnapshotWith(
        AdminPanel.Core.ClusterBackupsInfo panelCluster, string activeCluster, string shard)
        => new(
            DateTimeOffset.UtcNow,
            new AdminPanel.Core.EtcdStatus(true, [], [], [], null, false, DateTimeOffset.UtcNow, 0),
            [new AdminPanel.Core.ClusterInfo(
                activeCluster, activeCluster, 2, 1755800000, AdminPanel.Core.ClusterState.Active,
                [new AdminPanel.Core.ShardInfo(
                    shard, "", [""], 0, null, null, 1, null, [], null)],
                [], [])],
            [], [], [], [panelCluster], [], [], [], [], [], [], 0);

    private static AdminPanel.Core.Alerting.AlertContext DefaultAlertContext()
        => new(null, DateTimeOffset.UtcNow, 3);

    // AAA (AC1/AC3/AC4): первый дрилл стартует сам после COMPLETED-полного,
    // докатывает WAL, выходит из recovery (SUCCEEDED + restored_to_lsn),
    // контур снесён (контейнер/volume отсутствуют, phase снят).
    [Fact]
    public async Task Drill_Succeeds_CleansUp()
    {
        // Arrange — кластер bkdrll<тег> + policy (verify off, drill включён) + воркер.
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-drill", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"bkdrll{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"full_max_age_sec":86400,"verify":{"on_create":false},"drill":{"interval_days":1}}""", null, ct);
        await using var app = await StartBackupHostAsync("bkdrill", ct);

        // Act 1 — COMPLETED-полный (бюджет 300 c)
        var completed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(300), ct);
        completed.Should().BeTrue("полный обязан сняться");

        // Act 2 — дрилл дошёл до SUCCEEDED и довёл снос (чистый итог: phase снят)
        // — бюджет 600 c: скачать + накат; phase снимается тиком сноса ПОСЛЕ
        // вердикта, поэтому ждём именно БЕЗ phase (не гонка за вердиктом).
        var succeeded = await E2eFixture.WaitForAsync(async () =>
        {
            var kv = await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct);
            return kv.Value?.Value.Contains("\"SUCCEEDED\"") == true
                   && !kv.Value.Value.Contains("phase");
        }, TimeSpan.FromSeconds(600), ct);
        if (!succeeded)
        {
            // Телеметрия упавшего дрилла: дамп до teardown (канон e2e-launch)
            var drillKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/drill");
            var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var fulls = await FullKeysAsync(cluster, "shard1");
            var hostLog = "";
            foreach (var host in Fx.Hosts)
                try
                {
                    var lines = File.ReadAllLines(Path.Combine(host.SnapshotsDir, "host.log"));
                    var interesting = lines.Where(l =>
                        !l.Contains("ClientHandler") && !l.Contains("LogicalHandler")
                        && !l.Contains("HTTP request") && !l.Contains("HTTP response")
                        && !l.Contains("End processing HTTP") && !l.Contains("Received HTTP")
                        && !l.Contains("Sending HTTP") && !l.Contains("PgtuneInputsFactory")
                        && !l.Contains("wal_compression")).ToList();
                    hostLog += $"\n== {host.Name} ({interesting.Count}):\n" +
                        string.Join("\n", interesting[^Math.Min(60, interesting.Count)..]);
                }
                catch (Exception e)
                {
                    hostLog += $"\n== {host.Name}: host.log недоступен: {e.Message}";
                }

            await File.WriteAllTextAsync($"/tmp/pgw-diag-{cluster}.txt",
                $"reason=drill not SUCCEEDED\nfulls=[{string.Join("; ", fulls.Select(f => f.Key + "=" + f.Value[..Math.Min(160, f.Value.Length)]))}]\n" +
                $"drill=[{drillKv?.Value ?? "-"}]\nwal=[{walKv?.Value ?? "-"}]\nwork=[{workKv?.Value ?? "-"}]\nHOST.LOG:{hostLog}\n",
                TestContext.Current.CancellationToken);
        }

        succeeded.Should().BeTrue("первый дрилл стартует немедленно и обязан выйти из recovery");

        // Assert — restored_to_lsn в ключе; чистый итог: без phase
        var drill = (await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct)).Value!.Value;
        drill.Should().Contain("\"restored_to_lsn\":\"", "LSN восстановления фиксируется");
        drill.Should().NotContain("phase", "чистый терминальный итог — контур снесён");

        // Assert — прод-ноды шарда живы весь дрилл (AC2: изоляция — Patroni-контур не тронут)
        var nodes = await Fx.RunDockerAsync(
            ["ps", "--format", "{{.Names}}", "--filter", $"name=pgw-{cluster}-shard1-"], ct);
        nodes.Length.Should().BeGreaterThan(0, "дрилл не демонтирует прод-ноды шарда (state RUNNING)");

        // Assert — контейнер и volume дрилла отсутствуют (ассерт чистоты, AC4/AC2)
        var cleaned = await E2eFixture.WaitForAsync(async () =>
        {
            var containers = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-drill-{cluster}-"], ct);
            var volumes = await Fx.RunDockerAsync(
                ["volume", "ls", "-q", "--filter", $"name=pgw-backup-drill-{cluster}-"], ct);
            return containers.Length == 0 && volumes.Length == 0;
        }, TimeSpan.FromSeconds(60), ct);
        cleaned.Should().BeTrue("drill-контур сносится после итога");

        // Assert — оба парсера без ошибок; панельные правила молчат на успехе (AC5)
        var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
        var parsed = BackupsParser.Parse(kvs, out var parseErrors);
        parseErrors.Should().BeEmpty();
        var panel = AdminPanel.Etcd.Parsing.BackupsParser.Parse(kvs);
        panel.Errors.Should().BeEmpty();
        var panelCluster = panel.Clusters.Single(c => c.Cluster == cluster);
        var failedAlerts = new AdminPanel.Core.Alerting.Rules.BackupDrillFailedRule().Evaluate(
            SnapshotWith(panelCluster, cluster, "shard1"), DefaultAlertContext()).ToList();
        failedAlerts.Should().BeEmpty("SUCCEEDED — без алерта провала");
        var staleAlerts = new AdminPanel.Core.Alerting.Rules.BackupDrillStaleRule().Evaluate(
            SnapshotWith(panelCluster, cluster, "shard1"), DefaultAlertContext()).ToList();
        staleAlerts.Should().BeEmpty("свежий SUCCEEDED — без алерта молчания");
    }

    // AAA (AC1/AC3/AC5): дыра WAL-цепочки → валидационный FAILED без джоба
    // (контейнера нет, ключ без phase) + панельный алерт backup-drill-failed.
    // Дрилы выключены глобально env-ом; включаются policy-полем — тестирует и
    // применение policy на лету.
    [Fact]
    public async Task Drill_Failed_OnCorruptedWal()
    {
        // Arrange — кластер bkdrfl<тег>; дрилы выключены глобально (env IntervalDays=0).
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var fx = await E2eEnvironment.StartAsync("bk-drillf", withMinio: true, ct: ct);
        Fx = fx;
        var cluster = $"bkdrfl{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var app = await StartBackupHostAsync("bkdrillf", ct,
            extraEnv: new Dictionary<string, string>
            {
                ["PgWorker__Backups__Drill__IntervalDays"] = "0",
            });

        // Arrange — COMPLETED-полный снялся; дрилов нет (выключены env-ом).
        var completed = await E2eFixture.WaitForAsync(
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(300), ct);
        completed.Should().BeTrue("полный обязан сняться");
        var before = await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct);
        before.Value.Should().BeNull("IntervalDays=0 — запусков нет");

        // Arrange — цепочка после wal_start хвостом ≥2 сегментов: idle-DB закрывает
        // сегмент 16 МБ минутами, поэтому форсируем pg_switch_wal на мастере.
        // ВАЖНО: wal/ содержит сегменты и НИЖЕ wal_start полного (агент начинает
        // поток раньше бэкапа) — дыра «второй объект списка» может лечь ниже
        // chain_start и цепью не быть (прогон 2026-10-01: дрилл легитимно
        // SUCCEEDED за 6 c). Жертва — СТРОГО Next(wal_start) полного, дыра —
        // при наличии более позднего сегмента.
        var (_, pgPort, _) = await MasterPgAsync(cluster, "shard1", ct);
        var adminDsn = DatabaseProvisioner.BuildAdminDsn("localhost", pgPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
        await using (var conn = new NpgsqlConnection(adminDsn))
        {
            await conn.OpenAsync(ct);
            for (var i = 0; i < 4; i++)
                await using (var cmd = new NpgsqlCommand("SELECT pg_switch_wal()", conn))
                    await cmd.ExecuteScalarAsync(ct);
        }

        var hostEndpoint = Fx.S3Endpoint.Replace(
            "host.docker.internal:", "localhost:", StringComparison.Ordinal);
        var corruptS3 = new PgWorker.Backups.BackupS3(new PgWorker.Backups.BackupsRuntimeOptions
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
        
        // Act — жертва = САМ wal_start COMPLETED-полного (валидация дрилла идёт
        // от него; он гарантированно в wal/ — дублирование t02). Дыра обязана
        // иметь «найдено» — сегмент позже wal_start (гвард ниже). ВАЖНО: сегменты
        // НИЖЕ wal_start (агент начинает поток раньше бэкапа) цепью не являются —
        // дыра «второй объект списка» может лечь ниже chain_start и Check её не
        // видит (прогон 2026-10-01: дрилл легитимно SUCCEEDED за 6 c). Затем
        // включаем дриллы policy-полем (применяется следующим тиком без рестарта
        // — AC7). Пересъём BROKEN-цепочки занимает минуты — дыра живёт до
        // валидации дрилла (тик 1 с).
        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        var fullStatus = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(done.Value)!;
        var victim = PgWorker.Backups.WalFileName.TryParse(
            fullStatus["wal_start_segment"].GetString()!)!.Value;
        var victimReady = await E2eFixture.WaitForAsync(async () =>
        {
            var list = await corruptS3.ListWalAsync(cluster, "shard1", ct: ct);
            if (!list.IsSuccess)
                return false;
            var positions = list.Value
                .Select(o => PgWorker.Backups.WalFileName.TryParse(o.Name))
                .OfType<PgWorker.Backups.WalFileName>()
                .Distinct()
                .ToList();
            return positions.Any(w => (w.Tli, w.Log, w.Seg) == (victim.Tli, victim.Log, victim.Seg))
                   && positions.Any(w => w.Tli > victim.Tli
                       || (w.Tli == victim.Tli
                           && (w.Log > victim.Log
                               || (w.Log == victim.Log && w.Seg > victim.Seg))));
        }, TimeSpan.FromSeconds(300), ct);
        if (!victimReady)
        {
            // диагностика провала доставки жертвы (без перезапуска)
            var walObjectsDiag = await corruptS3.ListWalAsync(cluster, "shard1", ct: ct);
            var walKvDiag = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var workKvDiag = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var agentsDiag = await Fx.RunDockerAsync(
                ["ps", "-a", "--format", "{{.Names}} {{.State}}", "--filter", $"name=pgw-backup-wal-{cluster}-"], ct);
            throw new ApplicationException(
                $"жертва {victim.Name} не доставлена: " +
                $"walObjects=[{string.Join(",", walObjectsDiag.IsSuccess ? walObjectsDiag.Value.Select(o => o.Name) : [])}] " +
                $"wal=[{walKvDiag?.Value ?? "-"}] " +
                $"journal=[{workKvDiag?.Value[..Math.Min(400, workKvDiag?.Value.Length ?? 0)]}] " +
                $"agents=[{agentsDiag.Replace('\n', ';')}]");
        }
        // Останавливаем WAL-агента ДО порчи: иначе гонка самозалечивания — BROKEN
        // → пересъём/агент успевают вернуть сегмент до валидации дрилла (тик 1 с;
        // прогон мерж-гейта 2026-10-01: дрилл легитимно SUCCEEDED на зажившей
        // цепи). Остановленный агент ничего не ре-аплоадит; планировщик BROKEN-
        // пересъёма завершается ПОСЛЕ валидации дрилла (дыра живёт ≥1 тик).
        var agents = await Fx.RunDockerAsync(
            ["ps", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-wal-{cluster}-shard1"], ct);
        var agentName = agents.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(n => n.Trim()).FirstOrDefault();
        agentName.Should().NotBeNullOrEmpty("агент shard1 жив — предусловие стопа");
        await Fx.RunDockerAsync(["stop", agentName!], ct);

        (await corruptS3.DeleteKeysAsync([$"{cluster}/shard1/wal/{victim.Name}"], ct))
            .IsSuccess.Should().BeTrue("сегмент-жертва обязан удалиться");
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"full_max_age_sec":86400,"verify":{"on_create":false},"drill":{"interval_days":1}}""", null, ct);

        // Assert — валидационный FAILED (бюджет 180 c: тик + валидация list).
        var failed = await E2eFixture.WaitForAsync(async () =>
        {
            var kv = await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct);
            return kv.Value?.Value.Contains("\"FAILED\"") == true;
        }, TimeSpan.FromSeconds(180), ct);
        if (!failed)
        {
            // диагностика провала (без перезапуска): ключ дрилла, журнал, wal, полные
            var drillKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/drill");
            var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
            var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            var fullsNow = await FullKeysAsync(cluster, "shard1");
            var walNow = await corruptS3.ListWalAsync(cluster, "shard1", ct: ct);
            throw new ApplicationException(
                $"валидационный FAILED не зафиксирован: drill=[{drillKv?.Value ?? "-"}] " +
                $"journal=[{workKv?.Value[..Math.Min(400, workKv?.Value.Length ?? 0)]}] " +
                $"wal=[{walKv?.Value ?? "-"}] " +
                $"fulls=[{string.Join(";", fullsNow.Select(f => f.Value[..Math.Min(120, f.Value.Length)]))}] " +
                $"walObjects=[{string.Join(",", walNow.IsSuccess ? walNow.Value.Select(o => o.Name) : [])}]");
        }
        var drill = (await G.GetAsync(Endpoint, $"/pgworker/backups/{cluster}/shard1/drill", ct)).Value!.Value;
        // границы дыры — ASCII-имена сегментов (кириллица в JSON экранируется
        // сериализатором как \uXXXX — текст «дыра» в сыром значении не ищется);
        // FAILED без phase — чистый терминальный итог (джоб не запускался).
        // Границы динамические (жертва — сам wal_start, Act выше): «ожидался» —
        // wal_start жертвы, «найден» — ближайший доставленный сегмент после неё.
        var after = await corruptS3.ListWalAsync(cluster, "shard1", ct: ct);
        after.IsSuccess.Should().BeTrue("лист wal после порчи доступен");
        var found = after.Value
            .Select(o => o.Name)
            .Where(n => string.CompareOrdinal(n, victim.Name) > 0)
            .OrderBy(n => n, StringComparer.Ordinal)
            .FirstOrDefault();
        found.Should().NotBeNull("после жертвы есть более поздний сегмент — дыра имеет «найдено»");
        drill.Should().Contain(victim.Name, "граница дыры — wal_start")
            .And.Contain(found!, "граница дыры — найденный сегмент")
            .And.NotContain("phase", "без джоба — чистый итог");

        // Assert — контейнеров дрилла нет вовсе (джоб не запускался).
        var containers = await Fx.RunDockerAsync(
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name=pgw-backup-drill-{cluster}-"], ct);
        containers.Should().BeEmpty("джоб не запускался — валидационный отказ");

        // Assert — панельный алерт провала горит на фактических kvs кластера.
        var kvs = (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct)).Value;
        var panelCluster = AdminPanel.Etcd.Parsing.BackupsParser.Parse(kvs)
            .Clusters.Single(c => c.Cluster == cluster);
        var alerts = new AdminPanel.Core.Alerting.Rules.BackupDrillFailedRule().Evaluate(
            SnapshotWith(panelCluster, cluster, "shard1"), DefaultAlertContext()).ToList();
        alerts.Should().ContainSingle(a => a.Kind == "backup-drill-failed", "провал дрилла — critical-алерт");
    }

    // ===== Хелперы =====

    // Воркер с включённой подсистемой бэкапов: S3 на локальный MinIO,
    // образ pgworker-backup:e2e, ускоренный бэкофф.
    private Task<HostInstance> StartBackupHostAsync(
        string name, CancellationToken ct, string? s3EndpointOverride = null,
        IReadOnlyDictionary<string, string>? extraEnv = null)
    {
        var env = new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Endpoint"] = s3EndpointOverride ?? Fx.S3Endpoint,
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Backups__S3__SecretKey"] = "minioadmin",
            ["PgWorker__Backups__Job__Image"] = E2eEnvironment.JobImage,
            ["PgWorker__Backups__Wal__AgentImage"] = E2eEnvironment.WalImage,
            ["PgWorker__Backups__Retry__BaseSec"] = "2",
            ["PgWorker__Backups__Retry__MaxSec"] = "4",
        };
        foreach (var (key, value) in extraEnv ?? new Dictionary<string, string>())
            env[key] = value;
        return Fx.StartHostAsync(name, extraEnv: env, ct: ct);
    }

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
            ["PgWorker__Backups__Wal__AgentImage"] = E2eEnvironment.WalImage,
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

    private async Task<(string Host, int Port, string Node)> MasterPgAsync(string cluster, string shard, CancellationToken ct)
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
        return (entry.GetProperty("host").GetString()!, entry.GetProperty("pg").GetInt32(),
            primary!);

    }

    // Нагрузка под admin/superuser: CREATE TABLE + 30 циклов INSERT больших строк
    // + pg_switch_wal (superuser-only) — форсированное закрытие сегментов 16 МБ.
    private static async Task GenerateWalAsync(string adminDsn, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        try
        {
            await using (var create = new NpgsqlCommand(
                "CREATE TABLE IF NOT EXISTS wal_load(id bigserial, payload text)", conn))
                await create.ExecuteNonQueryAsync(ct);
        }
        catch (Npgsql.PostgresException e) when (e.SqlState == "23505")
        {
            // Гонка CREATE IF NOT EXISTS двух сессий (catalog-индекс): таблица
            // создаётся другой сессией — не ошибка нагрузки.
        }
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
