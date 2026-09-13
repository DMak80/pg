using System.Diagnostics;
using System.Text.Json;
using Npgsql;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Docker;
using PgWorker.Provisioning.Sql;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// E2E супервизора бэкапов (t07, spec §5 Ф5): изолированное окружение
// E2eEnvironment (своя сеть/etcd/MinIO, динамические порты, полный teardown
// в DisposeAsync — docs/e2e-isolation.md; телеметрия — docs/e2e-launch.md).
// Два сценария AC10: (1) ChainBroken_SelfHeals — дыра ВНУТРИ цепочки живого
// кластера → wal=BROKEN → планировщик переснимает полный → цепь непрерывна от
// нового wal_start → ACTIVE, агент жив; (2) OrphanRegistry_TtlDelete —
// чужой префикс без владельца → реестр /pgworker/backups/orphans (OBSERVED) →
// сжатый TTL → объекты удалены, запись погашена.
public class E2eSupervisorScenarios
{
    private const string Bucket = "pgworker-backups";

    // Окружение Fact'а (своя сеть/etcd/MinIO); создаётся в начале сценария.
    private E2eEnvironment Fx = null!;

    private string Endpoint => Fx.EtcdEndpoint;

    private PgWorker.Etcd.Client.EtcdGateway G => Fx.Gateway;

    // AAA (AC1/AC10): живой кластер с валидной цепочкой → дыра (rm сегмента ВНУТРИ
    // цепочки) → контроль → wal=BROKEN → планировщик переснимает полный → цепь
    // непрерывна от нового wal_start → wal=ACTIVE, агент жив
    [Fact]
    public async Task Backup_ChainBroken_SelfHeals()
    {
        // Arrange 1 — окружение bk-spr (withMinio), кластер bkspr<тег>, policy
        //   full_max_age_sec=3600 (НЕ из-за возраста — триггер только BROKEN),
        //   verify on_create=false (не мешает), Wal VerifyIntervalSec=5,
        //   Supervisor IntervalSec=60; воркер
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await RunScenarioAsync("bk-spr", async fx =>
        {
        Fx = fx;
        var cluster = $"bkspr{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await G.PutAsync(Endpoint, $"/pgworker/backups/{cluster}/policy",
            """{"retention":{"days":7,"weeks":4,"months":6},"full_max_age_sec":3600,"verify":{"on_create":false}}""",
            null, ct);
        await using var app = await StartSupervisorHostAsync(cluster, ct);

        // Arrange 2 — provisioning DONE; генерация WAL ДО полного (цепочка длиннее
        // одного сегмента), реальный COMPLETED полный; wal-ключ ACTIVE
        var provisioned = await WaitPhaseAsync("provisioning",
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning должен дойти до DONE до бэкапов");
        var (pgHost, pgPort) = await MasterPgAsync(cluster, "shard1", ct);
        var adminDsn = DatabaseProvisioner.BuildAdminDsn(pgHost, pgPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", ""));
        await SwitchWalsAsync(adminDsn, 16, ct);
        var real = await WaitForCompletedFullAsync(cluster);
        var walActive = await WaitPhaseAsync("wal-active",
            async () => (await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal"))
                ?.Value.Contains("ACTIVE") == true,
            TimeSpan.FromSeconds(120), ct);
        walActive.Should().BeTrue("wal-ключ обязан перейти в ACTIVE после первого полного");
        // Догон цепи за старт первого полного (инцидент прогона 2026-09-13:
        // окно [chain_start..last_uploaded] не росло — причина найдена: голый
        // pg_switch_wal на пустой базе no-op, см. SwitchWalsAsync). Дыра по
        // плану — СЕРЕДИНА цепи: генерируем WAL ПОСЛЕ полного, агент унесёт
        // last_uploaded выше chain_start на реальное число сегментов.
        var (tipHost, tipPort) = await MasterPgAsync(cluster, "shard1", ct);
        await SwitchWalsAsync(DatabaseProvisioner.BuildAdminDsn(tipHost, tipPort, cluster,
            new InstallSecrets(E2eFixture.SuPassword, "", "", "")), 6, ct);
        // Стабилизация контура (инцидент прогона 2026-09-13: смена мастера
        // посреди сценария меняла контекст цепи): пауза 20 c — Patroni-выборы
        // и догон реплик до инъекции дыры.
        await Task.Delay(TimeSpan.FromSeconds(20), ct);

        // Arrange 3 — ДЫРА: rm сегмента ВНУТРИ контролируемой цепи — строго между
        // chain_start и last_uploaded wal-ключа (сегменты ниже chain_start
        // контролем не смотрятся — ratchet/старт от полного). Окно цепи
        // ПЕРЕСЧИТЫВАЕТСЯ каждый тик ожидания: сразу после первого ACTIVE
        // chain_start == last_uploaded (один сегмент — инцидент прогона
        // 2026-09-13: зафиксированное окно не могло набрать ≥2) — ждём, пока
        // агент унесёт цепь вперёд и в окне появится ≥2 сегмента.
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
        var inChain = new List<string>();
        string? chainStartBefore = null;
        string? hole = null;
        string? holeRaw = null;
        var enough = await WaitPhaseAsync("wal-chain-depth", async () =>
        {
            var walRaw = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            if (walRaw is null)
                return false;
            var doc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(walRaw.Value)!;
            var chainStart = doc["chain_start_segment"].GetString();
            var lastUploaded = doc["last_uploaded_segment"].GetString();
            if (string.IsNullOrEmpty(chainStart) || string.IsNullOrEmpty(lastUploaded))
                return false;
            var listed = await backupS3.ListPrefixAsync($"{cluster}/shard1/wal/", ct: ct);
            // Имена в S3 — hex как его отдал PG (регистр любой); контроль
            // (WalChain/GapError/ключ wal) работает в каноническом lowercase
            // (WalFileName.Name) — сверяем и сортируем по канону, удаляем по
            // сырому ключу.
            var candidates = listed.Value
                .Select(o => o.Key.Split('/')[^1])
                .Select(raw => (Raw: raw, Canon: PgWorker.Backups.WalFileName.TryParse(raw)?.Name))
                .Where(x => x.Canon is not null)
                .Select(x => (x.Raw, Canon: x.Canon!))
                .Where(x => string.CompareOrdinal(x.Canon, chainStart) >= 0
                            && string.CompareOrdinal(x.Canon, lastUploaded) <= 0)
                .OrderBy(x => x.Canon, StringComparer.Ordinal).ToList();
            inChain = candidates.Select(x => x.Canon).ToList();
            chainStartBefore = chainStart;
            if (hole is null)
            {
                // Дыра — СЕРЕДИНА первого окна ≥2 сегментов (не хвост цепи);
                // фиксируется один раз — дальше ждём ухода цепи от неё.
                if (candidates.Count < 2)
                    return false;
                var picked = candidates[candidates.Count / 2];
                hole = picked.Canon;
                holeRaw = picked.Raw;
            }

            // Переснятый полный обязан стартовать ВЫШЕ дыры: ждём ≥2 сегментов
            // после неё (дыра глубоко в истории — инвариант плана «не последний»)
            return candidates.Count(x => string.CompareOrdinal(x.Canon, hole) > 0) >= 2;
        }, TimeSpan.FromSeconds(120), ct);
        enough.Should().BeTrue(
            $"дыра обязана уйти в историю цепи: chain_start={chainStartBefore}, " +
            $"дыра={hole}, окно=[{string.Join(",", inChain)}]");
        // Предусловие пересъёма (spec §3.2): полный стартует с позиции источника
        // (реплики) — она обязана быть ВЫШЕ дыры, иначе ratchet (arch/19 §3:
        // «полные ниже границы игнорируются») честно не заживит разрыв. Гейт
        // отделяет здоровье репликации среды от логики t07: обе ноды шарда
        // (primary по current_wal_lsn, реплика по last_wal_replay_lsn) — не
        // ниже начала сегмента, следующего за дырой.
        var replayTarget = LsnOf(PgWorker.Backups.WalFileName.TryParse(hole!)!.Value.Next().Name);
        var replicasPast = await WaitPhaseAsync("replicas-past-hole", async () =>
        {
            foreach (var (_, port) in await Shard1PortsAsync(cluster))
            {
                try
                {
                    await using var conn = new NpgsqlConnection(
                        DatabaseProvisioner.BuildAdminDsn("localhost", port, cluster,
                            new InstallSecrets(E2eFixture.SuPassword, "", "", "")));
                    await conn.OpenAsync(ct);
                    // ВАЖНО (прогон 2026-09-13, c344501d): параметр-строка без каста
                    // даёт pg_wal_lsn_diff(pg_lsn, text) → 42883 на каждом опросе,
                    // catch глотал — гейт «ждал» 300 с, не проверив ничего.
                    await using var cmd = new NpgsqlCommand(
                        "SELECT NOT pg_is_in_recovery() OR " +
                        "pg_wal_lsn_diff(pg_last_wal_replay_lsn(), @lsn::pg_lsn) >= 0", conn);
                    cmd.Parameters.AddWithValue("lsn", replayTarget);
                    if ((await cmd.ExecuteScalarAsync(ct)) is not true)
                        return false;
                }
                catch (NpgsqlException)
                {
                    return false; // нода ещё поднимается/рестартуется — повтор
                }
            }
            return true;
        }, TimeSpan.FromSeconds(300), ct);
        replicasPast.Should().BeTrue(
            $"реплики шарда1 обязаны догнать дыру {hole} (LSN {replayTarget}) — " +
            "иначе среда нездорова (застрявшая реплика), логика t07 не проверяется");
        await McRmAsync($"{cluster}/shard1/wal/{holeRaw}");

        // Act 1 — WaitForAsync: wal-ключ содержит "BROKEN" (контроль каждые 5 c)
        var brokenRaw = default(Kv?);
        var broken = await WaitPhaseAsync("wal-broken", async () =>
        {
            brokenRaw = await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal");
            return brokenRaw?.Value.Contains("BROKEN") == true;
        }, TimeSpan.FromSeconds(120), ct);
        if (!broken)
            throw new ApplicationException(
                $"wal-ключ не дошёл до BROKEN: [{brokenRaw?.Value ?? "нет ключа"}]");

        // Assert 1 — error содержит границы дыры: GapError называет отсутствующий
        // сегмент («дыра WAL-цепочки: ожидался <hole>, найден …»)
        var brokenDoc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(brokenRaw!.Value)!;
        brokenDoc["state"].GetString().Should().Be("BROKEN");
        brokenDoc["error"].GetString().Should().Contain(hole, "GapError называет границу дыры");
        brokenDoc["chain_start_segment"].GetString().Should().NotBeNullOrEmpty("граница разрыва в chain_start");

        // Act 2 — новый полный с id ≠ старого (планировщик реагирует на BROKEN),
        // затем COMPLETED (pg_basebackup со spread-чекпоинтом)
        var newFull = default(string);
        var planned = await WaitPhaseAsync("replan-full", async () =>
        {
            var fulls = await FullKeysAsync(cluster, "shard1");
            var fresh = fulls.FirstOrDefault(f =>
                f.Key.Split('/').Last() != real && f.Value.Contains("PLANNED")
                || f.Key.Split('/').Last() != real && f.Value.Contains("RUNNING")
                || f.Key.Split('/').Last() != real && f.Value.Contains("UPLOADING"));
            if (fresh is not null)
                newFull = fresh.Key.Split('/').Last();
            return newFull is not null;
        }, TimeSpan.FromSeconds(300), ct);
        planned.Should().BeTrue("BROKEN лечится пересъёмом — планировщик обязан создать новый полный");
        var completed = await WaitPhaseAsync("refull-completed", async () =>
        {
            var fulls = await FullKeysAsync(cluster, "shard1");
            var done = fulls.FirstOrDefault(f =>
                f.Key.Split('/').Last() == newFull && f.Value.Contains("COMPLETED"));
            return done is not null;
        }, TimeSpan.FromSeconds(600), ct);
        completed.Should().BeTrue($"переснятый полный {newFull} обязан дойти до COMPLETED");

        // Act 3 — wal.state=ACTIVE (заживление одним тиком после COMPLETED)
        var healed = await WaitPhaseAsync("wal-healed",
            async () => (await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal"))
                ?.Value.Contains("ACTIVE") == true,
            TimeSpan.FromSeconds(120), ct);
        if (!healed)
        {
            // Диагностика без перезапуска (docs/e2e-launch.md): все статусы
            // полных (wal_start переснятых — ключ к ratchet-разбору), wal-ключ,
            // факт S3-цепи.
            var fullDump = string.Join(";\n", (await FullKeysAsync(cluster, "shard1"))
                .Select(f => $"{f.Key.Split('/').Last()}={f.Value}"));
            var walDump = (await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal"))?.Value;
            var s3Dump = string.Join(",", (await backupS3.ListPrefixAsync($"{cluster}/shard1/wal/", ct: ct)).Value
                .Select(o => o.Key.Split('/')[^1]).OrderBy(n => n, StringComparer.Ordinal));
            throw new ApplicationException(
                $"wal-ключ не зажил до ACTIVE: wal=[{walDump}]\nfulls=[{fullDump}]\ns3=[{s3Dump}]");
        }

        // Assert 2 — chain_start нового ACTIVE ≥ wal_start нового полного; агент жив
        var healedWal = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(
            (await GetOrNullAsync($"/pgworker/backups/{cluster}/shard1/wal"))!.Value)!;
        var chainStart = healedWal["chain_start_segment"].GetString()!;
        var newStatus = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(
            (await FullKeysAsync(cluster, "shard1")).Single(f => f.Key.Split('/').Last() == newFull).Value)!;
        var newWalStart = newStatus["wal_start_segment"].GetString()!;
        // 24-hex имя при ordinal-сравнении = порядок (TLI, log, seg) — ratchet
        string.CompareOrdinal(chainStart, newWalStart).Should()
            .BeGreaterThanOrEqualTo(0, "контроль идёт от точки не ниже wal_start нового полного");
        var agent = await RunDockerAsync(
            ["ps", "--filter", $"name=pgw-backup-wal-{cluster}-shard1", "--format", "{{.Names}} {{.Status}}"]);
        agent.Should().Contain($"pgw-backup-wal-{cluster}-shard1").And.Contain("Up",
            "агент поднят зажившим тиком (BROKEN снят)");
        }, ct);
    }

    // AAA (AC7/AC10): shard-префикс без владельца → реестр OBSERVED → сжатый TTL →
    // DELETING → объекты удалены → запись гаснет
    [Fact]
    public async Task Backup_OrphanRegistry_TtlDelete()
    {
        // Arrange 1 — окружение bk-orf (withMinio), кластер bkorf<тег>, воркер с
        //   Supervisor { IntervalSec=60, OrphanTtlSec=120 } (сжатое время)
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await RunScenarioAsync("bk-orf", async fx =>
        {
        Fx = fx;
        var cluster = $"bkorf{Fx.ClusterTag}";
        var ghost = $"ghost{Fx.ClusterTag}";
        await SeedClusterAsync(cluster);
        await using var app = await StartOrphanHostAsync(cluster, ct);

        // Arrange 2 — provisioning DONE (воркер — единственный инстанс = лидер);
        //   mc-посев сиротского префикса ghost<тег>/shard1/full/20260901.../x
        //   (кластера ghost<тег> нет в /clusters/)
        var provisioned = await WaitPhaseAsync("provisioning",
            () => ProvisionedAsync(cluster), TimeSpan.FromSeconds(360), ct);
        provisioned.Should().BeTrue("provisioning обязан дойти до DONE (воркер-лидер)");
        await McCpAsync($"{ghost}/shard1/full/20260911110000Z/base.tar", "orphan-seed");

        // Act 1 — реестр: WaitFor ключа orphans с ghost<тег>/shard1 + OBSERVED
        var orphansRaw = default(Kv?);
        var observed = await WaitPhaseAsync("orphan-observed", async () =>
        {
            orphansRaw = await GetOrNullAsync("/pgworker/backups/orphans");
            return orphansRaw?.Value.Contains($"{ghost}/shard1") == true
                   && orphansRaw.Value.Contains("OBSERVED");
        }, TimeSpan.FromSeconds(180), ct);
        if (!observed)
            throw new ApplicationException(
                $"сирота не попала в реестр: [{orphansRaw?.Value ?? "нет ключа"}]");

        // Act 2 — TTL 120 c + проходы 60 c: объекты удалены (mc ls пуст) И записи нет
        var swept = await WaitPhaseAsync("orphan-ttl-delete", async () =>
        {
            var raw = await GetOrNullAsync("/pgworker/backups/orphans");
            var listed = await McLsAsync($"{ghost}/shard1/");
            return listed.Count == 0
                   && (raw is null
                       || !(raw.Value.Contains($"{ghost}/shard1")
                            && raw.Value.Contains("OBSERVED")));
        }, TimeSpan.FromSeconds(420), ct);
        if (!swept)
            throw new ApplicationException(
                $"сирота не удалена по TTL: [{(await GetOrNullAsync("/pgworker/backups/orphans"))?.Value}] " +
                $"mc ls=[{string.Join(";", await McLsAsync($"{ghost}/shard1/"))}]");

        // Assert — first_seen переносился (запись жила ≥ 2 прохода) — покрыт
        // интеграционными тестами Merge; здесь: удаление произошло ПОСЛЕ TTL
        // (объекты жили минимум до второго прохода — Observed-фаза пройдена).
        // Финальное состояние: mc ls пуст, записи в реестре нет (WaitFor выше).
        }, ct);
    }

    // ===== Хелперы (копии образца E2eRetentionScenarios — файлы сценариев
    // независимы, паттерн репо) =====

    // Политика телеметрии (docs/e2e-launch.md, образец E2eRestoreScenarios):
    // упавший тест помечает окружение MarkFailed — teardown ОСТАНАВЛИВАЕТ
    // контейнеры, но не удаляет (host.log/тома остаются для разбора «что
    // произошло»); перезапуск ради логов запрещён; зачистка — вручную.
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

    // Реальный COMPLETED полный (без требований к позиции wal_start).
    private async Task<string> WaitForCompletedFullAsync(string cluster)
    {
        var completed = await WaitPhaseAsync("first-full-completed",
            async () => (await FullKeysAsync(cluster, "shard1")).Any(f => f.Value.Contains("COMPLETED")),
            TimeSpan.FromSeconds(300), TestContext.Current.CancellationToken);
        completed.Should().BeTrue("реальный полный обязан дойти до COMPLETED");
        var done = (await FullKeysAsync(cluster, "shard1")).Single(f => f.Value.Contains("COMPLETED"));
        return done.Key.Split('/').Last();
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

    // Воркер самолечения: S3 на локальный MinIO, ускоренный бэкофф, ретенция
    // выключена (IntervalSec большой — чистка не мешает сценарию), Wal-контроль
    // каждые 5 c (скорость BROKEN), сверка супервизора раз в 60 c.
    private Task<HostInstance> StartSupervisorHostAsync(string name, CancellationToken ct)
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
            ["PgWorker__Backups__Policy__FullMaxAgeSec"] = "3600",
            ["PgWorker__Backups__Policy__VerifyOnCreate"] = "false",
            ["PgWorker__Backups__Retention__IntervalSec"] = "3600",
            ["PgWorker__Backups__Wal__VerifyIntervalSec"] = "5",
            ["PgWorker__Backups__Supervisor__IntervalSec"] = "60",
        }, ct: ct);

    // Воркер сценария сирот: реестр+TTL в сжатом времени; verify off.
    private Task<HostInstance> StartOrphanHostAsync(string name, CancellationToken ct)
        => Fx.StartHostAsync(name, extraEnv: new Dictionary<string, string>
        {
            ["PgWorker__Backups__Enabled"] = "true",
            ["PgWorker__Backups__S3__Endpoint"] = Fx.S3Endpoint,
            ["PgWorker__Backups__S3__Bucket"] = Bucket,
            ["PgWorker__Backups__S3__AccessKey"] = "minioadmin",
            ["PgWorker__Backups__S3__SecretKey"] = "minioadmin",
            ["PgWorker__Backups__Job__Image"] = E2eEnvironment.JobImage,
            ["PgWorker__Backups__Policy__VerifyOnCreate"] = "false",
            ["PgWorker__Backups__Supervisor__IntervalSec"] = "60",
            ["PgWorker__Backups__Supervisor__OrphanTtlSec"] = "120",
        }, ct: ct);

    private async Task<IReadOnlyList<Kv>> FullKeysAsync(string cluster, string shard)
        => (await G.RangeAsync(Endpoint, $"/pgworker/backups/{cluster}/{shard}/full/",
            TestContext.Current.CancellationToken)).Value;

    private async Task<Kv?> GetOrNullAsync(string key)
        => (await G.GetAsync(Endpoint, key, TestContext.Current.CancellationToken)).Value;

    private Task<string> RunDockerAsync(string[] args)
        => Fx.RunDockerAsync(args, TestContext.Current.CancellationToken);

    // mc rm одного объекта (mc в сети MinIO окружения).
    private Task McRmAsync(string key)
        => Fx.RunDockerAsync(
        [
            "run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
            "-c", $"mc alias set t http://e2e-minio:9000 minioadmin minioadmin >/dev/null"
                  + $" && mc rm t/{Bucket}/{key}",
        ], TestContext.Current.CancellationToken);

    // mc ls префикса → список имён объектов (после удаления — пусто).
    private async Task<List<string>> McLsAsync(string prefix)
    {
        var output = await Fx.RunDockerAsync(
        [
            "run", "--rm", "--network", Fx.NetName, "--entrypoint", "/bin/sh", E2eEnvironment.McImage,
            "-c", $"mc alias set t http://e2e-minio:9000 minioadmin minioadmin >/dev/null"
                  + $" && mc ls --recursive t/{Bucket}/{prefix} || true",
        ], TestContext.Current.CancellationToken);
        return output
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(l => l.Length > 0)
            .ToList();
    }

    // mc-посев объекта с фиксированным телом (образец E2eRetentionScenarios).
    private Task McCpAsync(string key, string content)
        => Fx.RunDockerAsync(
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

    // Закрытие count сегментов WAL. ВАЖНО (инцидент прогона 2026-09-13):
    // голый pg_switch_wal на пустой базе — NO-OP (PostgreSQL не создаёт пустые
    // сегменты: «has no effect if there has been no WAL traffic since the last
    // WAL switch»), 22 вызова дали ~5 сегментов, кончик цепи замер, гейт
    // глубины цепи честно истёк. Поэтому перед каждым переключением пишем
    // РЕАЛЬНЫЙ WAL — pg_logical_emit_message (wal_level=logical в кластере,
    // таблиц не требует): ~17 МБ на сегмент гарантированно закрывает его.
    private static async Task SwitchWalsAsync(string adminDsn, int count, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        for (var i = 0; i < count; i++)
        {
            for (var j = 0; j < 17; j++)
            {
                await using var message = new NpgsqlCommand(
                    "SELECT pg_logical_emit_message(false, 'e2e', repeat('w', 1048576))", conn);
                await message.ExecuteScalarAsync(ct);
            }
            await using var switchWal = new NpgsqlCommand("SELECT pg_switch_wal()", conn);
            await switchWal.ExecuteScalarAsync(ct);
        }
    }

    // Имя сегмента (24 hex) → LSN «X/Y» начала сегмента (для pg_wal_lsn_diff).
    private static string LsnOf(string name)
    {
        var log = Convert.ToUInt32(name.Substring(8, 8), 16);
        var seg = Convert.ToUInt32(name.Substring(16, 8), 16);
        var bytes = (log * 0x100UL + seg) * 16L * 1024 * 1024;
        return $"{bytes >> 32:X}/{bytes & 0xFFFFFFFF:X}";
    }

    // Published pg-порты ОБОИХ нод шарда1 из portalloc (host опускаем — ходим
    // с хоста по localhost, как MasterPgAsync).
    private async Task<List<(string Node, int Port)>> Shard1PortsAsync(string cluster)
    {
        var kv = await G.GetAsync(Endpoint, $"/pgworker/portalloc/{cluster}",
            TestContext.Current.CancellationToken);
        kv.Value.Should().NotBeNull("portalloc пишется при provisioning");
        var entries = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(kv.Value!.Value)!;
        return entries
            .Where(p => p.Key.StartsWith("shard1/", StringComparison.Ordinal))
            .Select(p => (p.Key.Split('/')[1], p.Value.GetProperty("pg").GetInt32()))
            .ToList();
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
