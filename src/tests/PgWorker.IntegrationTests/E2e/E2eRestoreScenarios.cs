using System.Diagnostics;
using System.Text.Json;
using Npgsql;
using PgWorker.Backups.Restore;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E восстановления шарда из бэкапов (t05, spec §4 Ф5): изолированное окружение
// E2eEnvironment (своя сеть/etcd/MinIO по guid, динамические порты, полный
// teardown с ассертом чистоты в DisposeAsync — docs/e2e-isolation.md) +
// provisioned-кластер с включённой подсистемой бэкапов → реальный COMPLETED
// полный + WAL-цепочка → restore-заявка (прямой PUT PLANNED-ключа, принятое
// отступление плана: API-путь покрыт WAF-тестами Task 13) → COMPLETED + данные.
// Заявки ставятся тестом; демонтаж/джоб/rejoin/сброс wal-ключа — механика
// воркера (RestoreProcess), E2E проверяет её end-to-end на живом docker.
public class E2eRestoreScenarios
{
    private const string Bucket = "pgworker-backups";

    // Окружение Fact'а (своя сеть/etcd/MinIO); создаётся в начале сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private EtcdGateway G => Fx.Gateway;

    // ===== Сценарий 1 (AC1+AC4): restore latest rebuilds уничтоженный шард =====

    // AAA: гибель нод+volume шарда → PLANNED → (валидация/демонтаж/джоб/rejoin
    // воркером) → COMPLETED с lsn; данные восстановлены (вкл. послебэкапные —
    // WAL накатился до latest); ноды RUNNING, dsn жив; wal-ключ сброшен и
    // заведён заново, полный переснят (инвариант цепочки §3.5).
    [Fact]
    public async Task Restore_Latest_RebuildsDestroyedShard()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await RunScenarioAsync("rs-latest", async fx =>
        {
            // Arrange 1 — окружение + кластер rst<тег> + воркер с бэкап-комплектом
            var cluster = $"rst{fx.ClusterTag}";
            await SeedClusterAsync(cluster);
            await using var app = await StartRestoreHostAsync("rslatest", ct);

            var provisioned = await WaitPhaseAsync(
                "provisioning", () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("provisioning должен дойти до DONE до бэкапов");

            // Arrange 2 — контрольные данные ДО полного (RPO-полный)
            var (pgHost, pgPort) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn(pgHost, pgPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await ExecAsync(adminDsn,
                "CREATE TABLE e2e_restore_probe(id int, note text)", ct);
            await ExecAsync(adminDsn,
                "INSERT INTO e2e_restore_probe SELECT g, 'pre' FROM generate_series(1, 5) g", ct);
            var dsnBefore = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/dsn");

            // Arrange 3 — реальный COMPLETED полный + послебэкапные данные в архиве
            var completed = await WaitPhaseAsync(
                "full-completed", async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
                TimeSpan.FromSeconds(300), ct);
            if (!completed)
                throw new ApplicationException("полный не дошёл до COMPLETED: " +
                    await DumpDiagnosticsAsync(cluster, "shard1"));
            var completedFull = (await FullKeysAsync(cluster, "shard1"))
                .Single(f => f.Value.Contains("COMPLETED"));
            var oldId = completedFull.Key.Split('/').Last();
            var walStartSeg = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(completedFull.Value)!
                ["wal_start_segment"].GetString();
            walStartSeg.Should().NotBeNullOrEmpty("wal_start_segment заполняется с UPLOADING");
            // Послебакапные данные (накатятся WAL'ом до latest)
            await ExecAsync(adminDsn,
                "INSERT INTO e2e_restore_probe SELECT g, 'post' FROM generate_series(6, 10) g", ct);
            // Закрываем сегменты до НЕПРЕРЫВНОЙ цепочки от wal_start полного —
            // условие restore-валидации. Точная позиция текущего сегмента не важна
            // (pg_basebackup в конце сам делает switch): emit+switch по одному,
            // пока WalChain не сойдётся (флейк гейта: слепой бёрст из 8 switch
            // оставлял дыру на wal_start, инцидент E2E-гейта t05).
            var chainDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(180);
            var chainOk = false;
            while (!chainOk && DateTime.UtcNow < chainDeadline)
            {
                await SwitchWalsAsync(adminDsn, 1, ct);
                chainOk = await WalChainContinuousAsync(cluster, "shard1", walStartSeg!, ct);
            }

            chainOk.Should().BeTrue("цепочка от wal_start обязана сойтись до заявки: "
                + await DumpDiagnosticsAsync(cluster, "shard1"));

            // Act — «погиб диск»: полный docker-снос нод шарда (контейнеры+тома) +
            // restore-заявка latest (backup_id пуст → валидация возьмёт новейший
            // COMPLETED из etcd); FAILED «дыра» — ретрай новой заявкой (гонка t03)
            await KillShardAsync(cluster, "shard1", ct);
            var done = await RestoreWithRetryAsync(cluster, "shard1", target: "latest", ct: ct);

            // Assert 1 — COMPLETED с restored_to_lsn
            done["state"].GetString().Should().Be("COMPLETED");
            done["restored_to_lsn"].GetString().Should().NotBeNullOrEmpty();

            // Assert 2 — данные восстановлены (10 строк: 5 pre + 5 post — WAL до latest).
            // Ретрай: сразу после COMPLETED Patroni доводит конфиг мастера (рестарт
            // рвёт соединения — EOF), это переходный оконный артефакт rejoin'а.
            var (afterHost, afterPort) = await MasterPgAsync(cluster, "shard1", ct);
            var afterDsn = DatabaseProvisioner.BuildAdminDsn(afterHost, afterPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            string rows = "";
            for (var attempt = 1; attempt <= 5; attempt++)
            {
                try
                {
                    rows = await ScalarAsync(afterDsn,
                        "SELECT count(*) || '|' || count(*) FILTER (WHERE note = 'post')" +
                        " FROM e2e_restore_probe", ct);
                    break;
                }
                catch (NpgsqlException) when (attempt < 5)
                {
                    await Task.Delay(8000, ct);
                }
            }

            var parts = rows.Split('|');
            parts[0].Should().Be("10", "все контрольные строки восстановлены");
            parts[1].Should().Be("5", "послебэкапные строки доставлены накатом WAL до latest");

            // Assert 3 — ноды RUNNING, dsn не изменился
            var nodeA = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/nodes/shard1a/state");
            var nodeB = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/nodes/shard1b/state");
            nodeA!.Value.Should().Be("RUNNING");
            nodeB!.Value.Should().Be("RUNNING");
            var dsnAfter = await GetOrNullAsync($"/clusters/{cluster}/shards/shard1/dsn");
            dsnAfter!.Value.Should().Be(dsnBefore!.Value, "portalloc/dsn restore не трогает");

            // Assert 4 (AC4) — wal-ключ сброшен и заведён заново (ACTIVE), полный
            // переснят (новый COMPLETED ≠ старому), агент вернулся running
            var reinited = await WaitPhaseAsync("wal-reinit", async () =>
            {
                var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
                if (walKv is null || !walKv.Value.Contains("ACTIVE"))
                    return false;
                var fulls = await FullKeysAsync(cluster, "shard1");
                var newCompleted = fulls.Any(f => f.Value.Contains("COMPLETED")
                    && f.Key.Split('/').Last() != oldId);
                if (!newCompleted)
                    return false;
                var agents = await fx.RunDockerAsync(
                ["ps", "--format", "{{.Names}}", "--filter",
                    $"name=pgw-backup-wal-{cluster}-shard1"], ct);
                return agents.Length > 0;
            }, TimeSpan.FromSeconds(300), ct);
            if (!reinited)
                throw new ApplicationException(
                    "инвариант AC4 не сошёлся: " + await DumpDiagnosticsAsync(cluster, "shard1"));
        }, ct);
    }

    // ===== Сценарий 2 (AC2): PITR target_time — откат до порчи =====

    // AAA: данные T0 → полный → данные T1 → фиксация T_cut → пауза → DROP
    // (порча уходит в архив) → restore target=T_cut → COMPLETED: таблица жива,
    // строки T0+T1 на месте, порчи нет; wal-ключ сброшен, новый полный снят.
    [Fact]
    public async Task Restore_TargetTime_PitrRollback()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await RunScenarioAsync("rs-time", async fx =>
        {
            // Arrange 1 — окружение + кластер rtp<тег>
            var cluster = $"rtp{fx.ClusterTag}";
            await SeedClusterAsync(cluster);
            await using var app = await StartRestoreHostAsync("rstime", ct);

            var provisioned = await WaitPhaseAsync(
                "provisioning", () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("provisioning должен дойти до DONE до бэкапов");

            // Arrange 2 — данные T0 → полный COMPLETED
            var (pgHost, pgPort) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn(pgHost, pgPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await ExecAsync(adminDsn, "CREATE TABLE pitr_probe(id int, note text)", ct);
            await ExecAsync(adminDsn,
                "INSERT INTO pitr_probe SELECT g, 't0' FROM generate_series(1, 5) g", ct);
            await SwitchWalsAsync(adminDsn, 4, ct);
            var completed = await WaitPhaseAsync(
                "full-completed", async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
                TimeSpan.FromSeconds(300), ct);
            completed.Should().BeTrue("полный обязан дойти до COMPLETED");

            // Arrange 3 — данные T1; ещё сегменты; фиксация цели. Позиции — от
            // wal_start полного: сегменты с T1 обязаны быть в архиве ДО заявки.
            var completedFull2 = (await FullKeysAsync(cluster, "shard1"))
                .Single(f => f.Value.Contains("COMPLETED"));
            var wsSeg = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(completedFull2.Value)!
                ["wal_start_segment"].GetString();
            wsSeg.Should().NotBeNullOrEmpty("wal_start_segment заполняется с UPLOADING");
            var ws = WalPosOf(wsSeg!);
            await ExecAsync(adminDsn,
                "INSERT INTO pitr_probe SELECT g, 't1' FROM generate_series(6, 10) g", ct);
            await SwitchWalsAsync(adminDsn, 6, ct); // закрыты ws..ws+5
            var t1Archived = await WalArchivedAsync(cluster, "shard1", ws + 5, ct);
            t1Archived.Should().BeTrue("сегменты с T1 обязаны попасть в архив: "
                + await DumpDiagnosticsAsync(cluster, "shard1"));
            var tCut = DateTime.UtcNow;
            await Task.Delay(2000, ct);

            // Arrange 4 — порча: DROP уходит в последующие (заархивированные) сегменты
            await ExecAsync(adminDsn, "DROP TABLE pitr_probe", ct);
            await SwitchWalsAsync(adminDsn, 3, ct); // закрыты ws+6..ws+8
            var dropArchived = await WalArchivedAsync(cluster, "shard1", ws + 8, ct);
            dropArchived.Should().BeTrue("сегмент с DROP обязан попасть в архив: "
                + await DumpDiagnosticsAsync(cluster, "shard1"));

            // Act — заявка PITR на T_cut (до порчи); ретрай на гонку t03 (дыра)
            var chainOk2 = await WalChainContinuousAsync(cluster, "shard1", wsSeg!, ct);
            chainOk2.Should().BeTrue("цепочка до заявки PITR обязана быть непрерывной: "
                + await DumpDiagnosticsAsync(cluster, "shard1"));
            var done = await RestoreWithRetryAsync(cluster, "shard1",
                target: $"time:{tCut:yyyy-MM-ddTHH:mm:ssZ}", ct: ct);

            // Assert 1 — COMPLETED
            done["restored_to_lsn"].GetString().Should().NotBeNullOrEmpty();

            // Assert 2 — таблица ЖИВА, строки T0+T1 (порча не накатилась).
            // Ретрай: сразу после COMPLETED Patroni доводит конфиг мастера (рестарт
            // рвёт соединения), это переходный оконный артефакт rejoin'а.
            var (afterHost, afterPort) = await MasterPgAsync(cluster, "shard1", ct);
            var afterDsn = DatabaseProvisioner.BuildAdminDsn(afterHost, afterPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            string rows = "";
            for (var attempt = 1; attempt <= 5; attempt++)
            {
                try
                {
                    rows = await ScalarAsync(afterDsn,
                        "SELECT count(*) || '|' || count(*) FILTER (WHERE note = 't1') FROM pitr_probe", ct);
                    break;
                }
                catch (NpgsqlException) when (attempt < 5)
                {
                    await Task.Delay(8000, ct);
                }
            }

            var parts = rows.Split('|');
            parts[0].Should().Be("10", "T0+T1 восстановлены, DROP не накатился");
            parts[1].Should().Be("5");

            // Assert 3 (AC4) — wal-ключ сброшен/заведён заново, новый полный снят
            var reinited = await WaitPhaseAsync("wal-reinit", async () =>
            {
                var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
                return walKv is not null && walKv.Value.Contains("ACTIVE");
            }, TimeSpan.FromSeconds(300), ct);
            reinited.Should().BeTrue("после PITR цепочка заводится заново (инвариант §3.5)");
        }, ct);
    }

    // ===== Сценарий 3 (AC3+AC6-D2): DR — новый кластер из S3-префикса =====

    // AAA: кластер A с данными и полным → deprovision (etcd-контур пуст ВКЛЮЧАЯ
    // /pgworker/backups/A/, S3 жив) → пересоздание декларацией → restore обеих
    // шардов с source=A/shard1 (list-S3 без etcd-статусов) → COMPLETED + данные.
    [Fact]
    public async Task Restore_NewCluster_FromSourcePrefix()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await RunScenarioAsync("rs-dr", async fx =>
        {
            // Arrange 1 — окружение + кластер rdr<тег>, данные, полный COMPLETED
            var cluster = $"rdr{fx.ClusterTag}";
            await SeedClusterAsync(cluster);
            await using var app = await StartRestoreHostAsync("rsdr", ct);

            var provisioned = await WaitPhaseAsync(
                "provisioning", () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
            provisioned.Should().BeTrue("provisioning должен дойти до DONE до бэкапов");
            var (pgHost, pgPort) = await MasterPgAsync(cluster, "shard1", ct);
            var adminDsn = DatabaseProvisioner.BuildAdminDsn(pgHost, pgPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            await ExecAsync(adminDsn, "CREATE TABLE dr_probe(id int, note text)", ct);
            await ExecAsync(adminDsn,
                "INSERT INTO dr_probe SELECT g, 'keeper' FROM generate_series(1, 7) g", ct);
            var completed = await WaitPhaseAsync(
                "full-completed", async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
                TimeSpan.FromSeconds(300), ct);
            completed.Should().BeTrue("полный обязан дойти до COMPLETED до deprovision");

            // Act 1 — deprovision: state=TO_REMOVE → контур A пуст (D2-ассерт: и
            // /clusters/A/, и /pgworker/backups/A/), S3-префикс жив
            var config = await G.GetAsync(Endpoint, $"/clusters/{cluster}/config", ct);
            config.Value.Should().NotBeNull();
            var doc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(config.Value!.Value)!;
            doc["state"] = JsonSerializer.SerializeToElement("TO_REMOVE");
            await G.PutAsync(Endpoint, $"/clusters/{cluster}/config",
                JsonSerializer.Serialize(doc), null, ct);
            var deprovisioned = await WaitPhaseAsync("deprovision", async () =>
            {
                var clusterPrefix = await G.RangeAsync(Endpoint, $"/clusters/{cluster}/", ct);
                var backupsPrefix = await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/", ct);
                // Failed (разовый сбой) ≠ «пусто»: считаем условие не выполненным —
                // поллинг повторит, ложный успешный ассерт исключён.
                if (!clusterPrefix.IsSuccess || !backupsPrefix.IsSuccess)
                    return false;
                return clusterPrefix.Value.Count == 0 && backupsPrefix.Value.Count == 0;
            }, TimeSpan.FromSeconds(300), ct);
            deprovisioned.Should().BeTrue("deprovision обязан вычистить контур кластера (вкл. restore/full/wal)");
            await using var backupS3 = HostS3Client();
            var fullsInS3 = await backupS3.ListFullsAsync(cluster, "shard1", ct: ct);
            fullsInS3.IsSuccess.Should().BeTrue();
            fullsInS3.Value.Should().NotBeEmpty("S3 переживает кластер — фактический DR-источник");

            // Act 2 — пересоздание декларацией + restore обеих шардов из A/shard1
            // (source-override; ретрай на гонку t03 — дыры цепочки source-префикса)
            await SeedClusterAsync(cluster);
            var reprovisioned = await WaitPhaseAsync(
                "reprovisioning", () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
            reprovisioned.Should().BeTrue("кластер обязан подняться заново (пустые шарды)");
            var drDone = await RestoreWithRetryAsync(cluster, "shard1",
                target: "latest", source: $"{cluster}/shard1", ct: ct);
            var drDone2 = await RestoreWithRetryAsync(cluster, "shard2",
                target: "latest", source: $"{cluster}/shard1", ct: ct);

            // Assert — оба restore COMPLETED (RestoreWithRetry не вернёт FAILED);
            // контрольные строки на месте
            var (drHost, drPort) = await MasterPgAsync(cluster, "shard1", ct);
            var drDsn = DatabaseProvisioner.BuildAdminDsn(drHost, drPort, cluster,
                new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
            var rows = await ScalarAsync(drDsn, "SELECT count(*) FROM dr_probe", ct);
            rows.Should().Be("7", "данные совпадают с моментом бэкапа (RPO = точка полного): "
                + await DumpDiagnosticsAsync(cluster, "shard1"));
        }, ct);
    }

    // ===== Хелперы (копии образцов E2eBackupScenarios/E2eRetentionScenarios —
    // файлы сценариев независимы, паттерн репо) =====

    // Запуск сценария под политикой телеметрии (docs/e2e-launch.md): упавший
    // тест помечает окружение MarkFailed — teardown ОСТАНАВЛИВАЕТ контейнеры
    // (не удаляет: логи/тома остаются для отчёта «что произошло»), перезапуск
    // теста ради логов запрещён. Зачистка — вручную по README-cleanup.txt.
    private async Task RunScenarioAsync(
        string slug, Func<E2eEnvironment, Task> body, CancellationToken ct)
    {
        var fx = await E2eEnvironment.StartAsync(slug, withMinio: true, ct: ct);
        Fx = fx;
        try
        {
            await body(fx);
        }
        catch
        {
            fx.MarkFailed();
            throw;
        }
        finally
        {
            await fx.DisposeAsync();
        }
    }

    // Ожидание фазы с телеметрией (docs/e2e-launch.md): длительность в журнал
    // теста всегда; фаза дольше 60 с — docker-логи окружения снимаются в
    // артефакты немедленно (отчёт «почему так долго» собирается по логам,
    // без перезапуска), независимо от исхода фазы.
    private async Task<bool> WaitPhaseAsync(
        string phase, Func<Task<bool>> condition, TimeSpan timeout, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        var ok = await E2eFixture.WaitForAsync(condition, timeout, ct);
        sw.Stop();
        Console.WriteLine($"[PHASE] {phase}: ok={ok}, elapsed={sw.Elapsed.TotalSeconds:F1}s");
        if (sw.Elapsed > TimeSpan.FromSeconds(60))
            await Fx.CollectDiagnosticsAsync($"slow-phase-{phase}-{(int)sw.Elapsed.TotalSeconds}s");
        return ok;
    }

    // Воркер с бэкап-комплектом (t02+t03+t05): паттерн StartBackupHostAsync —
    // S3Endpoint = advertised (host.docker.internal:<порт>): джобы t02 берут
    // PGW_BK_S3_ENDPOINT из сырого S3Endpoint (внутри контейнера localhost
    // недостижим!), агенты t03 — advertised. Пороги WAL-контроля — короткие.
    private Task<HostInstance> StartRestoreHostAsync(string name, CancellationToken ct)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Endpoint"] = Fx.S3Endpoint,
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
            ["PgWorker__Backups__Restore__RecoveryTimeoutSec"] = "600",
            // Бюджет rejoin 120 c (прод 300 c): выборы лидера 15–55 c
            // (вариативно) + старт basebackup реплик; 120 c накрывает вариации
            // нагрузки хоста, не маскируя деградацию
            ["PgWorker__Thresholds__PatroniBootSec"] = "120",
        }, ct: ct);

    // Host-клиент BackupS3 (порт published): ассерты S3 и DR-list.
    private PgWorker.Backups.BackupS3 HostS3Client()
        => new(new PgWorker.Backups.BackupsRuntimeOptions
        {
            Enabled = true,
            S3Endpoint = Fx.S3Endpoint.Replace(
                "host.docker.internal:", "localhost:", StringComparison.Ordinal),
            S3Bucket = Bucket,
            S3AccessKey = "minioadmin",
            S3SecretKey = "minioadmin",
            S3PathStyle = true,
        });

    // PUT PLANNED-заявки restore прямым ключом (принятое отступление Task 15:
    // прецедент E2eRotateScenarios; API-путь покрыт WAF-тестами RestoreApiTests).
    // backup_id пуст → валидация воркера резолвит новейший COMPLETED (etcd → S3).
    private async Task<string> PutRestorePlannedAsync(
        string cluster, string shard, string target, string? source = null,
        CancellationToken ct = default)
    {
        var id = $"{DateTimeOffset.UtcNow:yyyyMMddHHmmss}Z-{shard}";
        var op = new RestoreOperationState(id, RestoreStatus.Planned, "",
            source ?? $"{cluster}/{shard}", target, $"{shard}a",
            DateTimeOffset.UtcNow.ToUnixTimeSeconds(), "e2e");
        await G.PutAsync(Endpoint, PgWorker.Backups.BackupNames.RestoreKey(cluster, shard, id),
            RestoreStatusJson.Serialize(op), null, ct);
        return id;
    }

    // Поллинг restore-ключа до терминального статуса. FAILED «дыра WAL-цепочки» —
    // внешняя гонка t03 (пересоздание WAL-агента теряет сегмент, канон откладывает
    // reconnect-устойчивость в t07): ChainGapException — сигнал РЕТРАЯ заявки.
    private sealed class ChainGapException(string message) : ApplicationException(message);

    private async Task<Dictionary<string, JsonElement>?> WaitRestoreAsync(
        string cluster, string shard, string id, TimeSpan budget, CancellationToken ct)
    {
        Dictionary<string, JsonElement>? status = null;
        var sw = Stopwatch.StartNew();
        var done = await E2eFixture.WaitForAsync(async () =>
        {
            var raw = await GetOrNullAsync(PgWorker.Backups.BackupNames.RestoreKey(cluster, shard, id));
            if (raw is null)
                return false;
            status = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(raw.Value);
            return status is not null && (status["state"].GetString() is "COMPLETED" or "FAILED");
        }, budget, ct);
        sw.Stop();
        // Телеметрия (docs/e2e-launch.md): длительность restore — в журнал всегда;
        // медленная (> 60 с), но успешная фаза тоже снимает docker-логи окружения.
        Console.WriteLine(
            $"[PHASE] restore {cluster}/{shard}/{id}: state={status?["state"].GetString()}, elapsed={sw.Elapsed.TotalSeconds:F1}s");
        if (sw.Elapsed > TimeSpan.FromSeconds(60))
            await Fx.CollectDiagnosticsAsync($"slow-restore-{cluster}-{shard}-{(int)sw.Elapsed.TotalSeconds}s");
        if (!done || status!["state"].GetString() == "FAILED")
        {
            var error = status?.TryGetValue("error", out var err) == true ? err.GetString() : null;
            if (error is not null && error.Contains("дыра WAL-цепочки", StringComparison.Ordinal))
                throw new ChainGapException(
                    $"restore {cluster}/{shard}/{id}: {error}");
            throw new ApplicationException(
                $"restore {cluster}/{shard}/{id} не дошёл до COMPLETED: "
                + (status is null ? "ключа нет" : status["state"].GetString())
                + (error is null ? "" : $", error={error}")
                + "; " + await DumpDiagnosticsAsync(cluster, shard));
        }

        return done ? status : null;
    }

    // Непрерывность цепочки от wal_start полного (WalChain.Check — тот же
    // валидатор, что в restore): цель — ставит заявку только на консистентной
    // цепочке; гонки пересоздания t03-агента ждутся поллингом.
    private async Task<bool> WalChainContinuousAsync(
        string cluster, string shard, string walStartSegment, CancellationToken ct)
        => await E2eFixture.WaitForAsync(async () =>
        {
            var chainStart = PgWorker.Backups.WalFileName.TryParse(walStartSegment);
            if (chainStart is null)
                return true; // битый wal_start — не цепочечная проблема валидатора
            var names = await ListWalNamesAsync(cluster, shard);
            return PgWorker.Backups.WalChain.Check(chainStart.Value, names).IsContinuous;
        }, TimeSpan.FromSeconds(90), ct);

    // Заявка restore с ретраем на внешнюю гонку t03: FAILED «дыра WAL-цепочки» →
    // новая заявка (permanent-FAILED предшественника — история; шард после FAILED
    // остаётся разобранным — повторная заявка сама проводит демонтаж 404=ок,
    // джоб пересоздаёт volume ноды, rejoin поднимает ноды). До 3 попыток.
    // Бюджет 300 c (воркеровский PatroniBootSec=120 перекрыт в хосте): любую
    // проваленную фазу воркер превращает в FAILED с причиной — тест её увидит,
    // а не будет покрывать ожиданием.
    private async Task<Dictionary<string, JsonElement>> RestoreWithRetryAsync(
        string cluster, string shard, string target, string? source = null,
        int attempts = 3, CancellationToken ct = default)
    {
        for (var attempt = 1; ; attempt++)
        {
            var restoreId = await PutRestorePlannedAsync(cluster, shard, target, source, ct);
            try
            {
                var status = await WaitRestoreAsync(cluster, shard, restoreId,
                    TimeSpan.FromSeconds(300), ct);
                return status!;
            }
            catch (ChainGapException) when (attempt < attempts)
            {
                Console.Error.WriteLine(
                    $"e2e[restore]: {cluster}/{shard} попытка {attempt} — дыра цепочки (гонка t03), повторяю заявку");
                await Task.Delay(3000, ct);
            }
        }
    }

    // «Погиб диск»: снос контейнеров+томов нод шарда (own-only имена — префикс
    // кластера уникален на прогон); rm несуществующего — идемпотентно tolerated.
    // volume rm 409 «in use» — гонка демон-удаления контейнера (docker rm -f
    // возвращается раньше фактического освобождения ссылки): РЕТРАИ с бюджетом
    // (прецедент teardown E2eEnvironment), иначе демонтаж воркера циклит transient.
    private async Task KillShardAsync(string cluster, string shard, CancellationToken ct)
    {
        foreach (var node in new[] { $"{shard}a", $"{shard}b" })
        {
            try
            {
                await Fx.RunDockerAsync(["rm", "-f", $"pgw-{cluster}-{shard}-{node}"], ct);
            }
            catch (Exception)
            {
                // уже нет (гонка демонтажа) — не помеха
            }

            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    await Fx.RunDockerAsync(
                        ["volume", "rm", "-f", $"pgw-{cluster}-{shard}-{node}-data"], ct);
                    break;
                }
                catch (Exception) when (attempt < 14)
                {
                    // «in use»: держатель доремувливается демоном — повтор
                    await Task.Delay(2000, ct);
                }
            }
        }
    }

    // Диагностика провала: журнал воркера + restore-ключи/полные шарда + docker-объекты
    // кластера (фильтр по ТЕГУ кластера — substring ловит и pgw-backup-restore-*).
    private async Task<string> DumpDiagnosticsAsync(string cluster, string shard)
    {
        var workKv = await GetOrNullAsync($"/pgworker/work/{cluster}");
        var restoreKvs = await G.RangeAsync(
            Endpoint, $"/pgworker/backups/{cluster}/{shard}/restore/",
            TestContext.Current.CancellationToken);
        var fullKvs = await FullKeysAsync(cluster, shard);
        var walKv = await GetOrNullAsync($"/pgworker/backups/{cluster}/{shard}/wal");
        var walNames = await ListWalNamesAsync(cluster, shard);
        // Patroni-scope шарда целиком: рудименты прошлой жизни кластера
        // (initialize/status с чужим system-id) — прямой кандидат в причины
        // «контейнеры Up, а Patroni API молчит».
        var scopeKvs = await G.RangeAsync(
            Endpoint, $"/service/{cluster}-{shard}/", TestContext.Current.CancellationToken);
        var scopeDump = scopeKvs.IsSuccess
            ? string.Join(";", (scopeKvs.Value ?? []).Select(
                k => $"{k.Key.Replace($"/service/{cluster}-{shard}/", "")}={k.Value[..Math.Min(200, k.Value.Length)]}"))
            : $"range failed: {scopeKvs.Error?.Message}";
        // Состояния нод шарда: PROVISIONING-метка выдаёт EnsureDeclaredNodes
        // (супервиз) как создателя контейнера в гонке rejoin'а.
        var nodeStates = await G.RangeAsync(
            Endpoint, $"/clusters/{cluster}/shards/{shard}/nodes/", TestContext.Current.CancellationToken);
        var nodeDump = nodeStates.IsSuccess
            ? string.Join(";", (nodeStates.Value ?? []).Select(k =>
                $"{k.Key.Replace($"/clusters/{cluster}/shards/{shard}/nodes/", "")}={k.Value}"))
            : $"range failed: {nodeStates.Error?.Message}";
        string jobs;
        try
        {
            jobs = await Fx.RunDockerAsync(
            ["ps", "-a", "--format", "{{.ID}} {{.Names}} {{.Status}}",
                "--filter", $"name={cluster}"],
            TestContext.Current.CancellationToken);
            jobs += " | volumes=" + await Fx.RunDockerAsync(
            ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name={cluster}"],
            TestContext.Current.CancellationToken);
        }
        catch (Exception e)
        {
            jobs = $"docker ps failed: {e.Message}";
        }

        // Журнал воркера целиком (политика телеметрии: без обрезок — по нему
        // отвечают «почему фаза не уложилась»; объёмы в e2e — единицы КБ).
        var journal = workKv?.Value ?? "<absent>";
        return $"journal=[{journal}] " +
               $"restores=[{string.Join(";", (restoreKvs.Value ?? []).Select(k => k.Value))}] " +
               $"fulls=[{string.Join(";", fullKvs.Select(k => k.Value))}] " +
               $"walKey=[{walKv?.Value ?? "<absent>"}] " +
               $"wal=[{string.Join(",", walNames.OrderBy(n => n, StringComparer.Ordinal))}] " +
               $"patroniScope=[{scopeDump}] " +
               $"nodes=[{nodeDump}] " +
               $"jobs=[{jobs}]";
    }

    // Range Failed (разовый сбой etcd/транспорт) — пустой список: поллинг
    // повторит в следующем такте, null-краш в LINQ не допустим (инцидент гейта).
    private async Task<IReadOnlyList<Kv>> FullKeysAsync(string cluster, string shard)
        => (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/{shard}/full/",
            TestContext.Current.CancellationToken)).Value ?? [];

    // Имена заархивированных сегментов (host-клиент, префикс wal/).
    private async Task<List<string>> ListWalNamesAsync(string cluster, string shard)
    {
        await using var s3 = HostS3Client();
        var listed = await s3.ListWalAsync(cluster, shard, ct: TestContext.Current.CancellationToken);
        if (!listed.IsSuccess)
            Console.Error.WriteLine($"e2e[archive]: LIST FAILED {listed.Error?.Message}");
        return listed.IsSuccess ? [.. listed.Value.Select(o => o.Name)] : [];
    }

    // pg_switch_wal × n (superuser-only): форсированное закрытие сегментов.
    // ⚠️ pg_switch_wal на простаивающей базе НЕ материализует сегменты: switch
    // завершается только при следующей WAL-записи (без записей повторные вызовы
    // возвращают тот же LSN — сегменты не создаются и в S3 не попадают, гейт
    // 2026-09-12). Поэтому перед каждым switch пишем реальную запись WAL
    // (pg_logical_emit_message, wal_level=logical) — сегмент обязан закрыться
    // и уйти в архив.
    private static async Task SwitchWalsAsync(string adminDsn, int count, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        for (var i = 0; i < count; i++)
        {
            await using var emit = new NpgsqlCommand(
                "SELECT pg_logical_emit_message(false, 'e2e', repeat('x', 128))", conn);
            await emit.ExecuteScalarAsync(ct);
            await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
            await switchWal.ExecuteScalarAsync(ct);
        }
    }

    // Позиция сегмента (лог×256 + смещение) — канон WalFileName (как E2eRetention).
    private static long WalPosOf(string segment)
    {
        var parsed = PgWorker.Backups.WalFileName.TryParse(segment);
        return parsed is { } w ? (long)w.Log * 256 + w.Seg : -1;
    }

    // ДОСТАВКА цепочки: последний закрытый pg_switch_wal'ом сегмент (targetPos)
    // обязан появиться в wal/-префиксе ДО restore-заявки. Счётная проверка
    // «≥N объектов» не годится: непрерывность валидатор проверяет от wal_start
    // полного, недоставленный средний сегмент = permanent FAILED (гейт-факт).
    private async Task<bool> WalArchivedAsync(
        string cluster, string shard, long targetPos, CancellationToken ct)
        => await E2eFixture.WaitForAsync(async () =>
        {
            var names = await ListWalNamesAsync(cluster, shard);
            return names.Select(WalPosOf).DefaultIfEmpty(-1).Max() >= targetPos;
        }, TimeSpan.FromSeconds(180), ct);

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    // ВРЕМЕННАЯ диагностика t05 E2E удалена (гейт 2026-09-12): причина найдена —
    // pg_switch_wal без WAL-записей не материализует сегменты (см. SwitchWalsAsync).

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
        return Convert.ToString(await cmd.ExecuteScalarAsync(ct)) ?? "";
    }

    // pg_switch_wal × n (superuser-only): форсированное закрытие пустых
    // сегментов 16 МБ — сегменты с данными уходят в архив promptly.

    // Published pg-порт мастера шарда из portalloc (резолв фактического primary
    // по пробам Patroni /primary; host-клиент — localhost).
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

    // Сид кластера (копия E2eRetentionScenarios.SeedClusterAsync).
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
}
