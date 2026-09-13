using System.Globalization;
using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;

namespace PgWorker.Backups.Process;

// RestoreProcess (t05, arch/19 §3.5): машина восстановления шарда из бэкапа.
// Поддерживается ТОЛЬКО в Mode=Plain (подсистема бэкапов plain-only с t02/t03;
// swarm-драйвер джобы бэкапов не исполняет — прецедент
// SwarmClusterDriver.EnsureBackupAgentAsync). Тик под клэймом <C>: активная
// заявка (максимум одна на шард) проводится по фазам
// PLANNED (валидация: усыновление/полный/manifest/цепочка) →
// RUNNING (демонтаж + ephemeral restore-джоб, Task 9) →
// REJOINING (EnsureNode + Patroni-пробы, Task 10) →
// COMPLETED (del wal-ключа → планировщик t02 переснимает полный).
// permanent/transient: валидационные отказы — permanent-FAILED (повтор заявки
// оператором); docker/S3-транспортные отказы — transient (статус не меняем,
// следующий тик повторит). Takeover: всё состояние — в etcd-статусе заявки;
// in-memory только диагностика ожиданий (Task 10).
public sealed class RestoreProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    IBackupS3 s3,
    ClaimStore claims,
    WorkJournal journal,
    BackupsRuntimeOptions options,
    InstallSecrets secrets,
    EtcdEndpoints etcdEndpoints,
    IClusterSecretEnsurer appSecret,
    ShardProbe probe,
    ThresholdsOptions thresholds,
    TimeProvider time,
    ILogger<RestoreProcess>? logger = null)
{
    private const string Op = "backup-restore";

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
    {
        // Каркас (Task 8): параметры фаз RUNNING/REJOINING (демонтаж/джоб/rejoin,
        // Tasks 9–10) — discard до реализации фаз, сигнатура финальная.
        _ = driver; _ = secrets; _ = etcdEndpoints; _ = appSecret; _ = probe; _ = thresholds;

        var cluster = snap.Config.Cluster;

        // Мутации /pgworker/backups/* — только держатель клэйма (arch/19 §4).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"backup-restore {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // G0: подсистема выключена — процесс не исполняется (врезка ReconcileLoop
        // тоже гвардит, но процесс самодостаточен при прямом вызове).
        if (!options.Enabled)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var mine = backups.FirstOrDefault(b => b.Cluster == cluster);
        var actives = mine?.Shards
            .SelectMany(kv => kv.Value.Restores.Select(r => (Shard: kv.Key, Op: r)))
            .Where(p => p.Op.State is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining)
            .OrderBy(p => p.Op.Id).ThenBy(p => p.Shard).ToList() ?? [];
        if (actives.Count == 0)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        // Процессный гвард «максимум один активный на шард» (§3.1): API-гвард
        // (txn 409) обычно не допускает дублей, но ручная запись/гонка могут —
        // младшие дубли того же шарда гасим permanent-FAILED, старейший
        // исполняется.
        var oldest = actives[0];
        foreach (var dup in actives.Skip(1).Where(p => p.Shard == oldest.Shard))
            await FailPermanentAsync(cluster, dup.Shard, dup.Op,
                error: $"дубль заявки: активен старейший {oldest.Op.Id}", ct);

        var shard = snap.Shards.FirstOrDefault(s => s.Name == oldest.Shard);
        if (shard is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // шард убрали — заявку закроет remove-shard ветка
        try
        {
            return oldest.Op.State switch
            {
                RestoreStatus.Planned => await ValidateAsync(snap, shard, oldest.Op, mine, ct),
                RestoreStatus.Running => await RunAsync(snap, shard, oldest.Op, ct),      // Task 9
                RestoreStatus.Rejoining => await RejoinAsync(snap, shard, oldest.Op, ct), // Task 10
                _ => Result<ProcessOutcome>.Success(ProcessOutcome.Done),
            };
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // ошибка шарда не роняет тик (§3.4)
            logger?.LogError(ex, "backup-restore {Cluster}/{Shard}: {Message}", cluster, shard.Name, ex.Message);
            await journal.WritePhaseAsync(cluster, Op, "crashed", claims.InstanceId, ex.Message, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
    }

    // ── PLANNED: валидация заявки (все шаги идемпотентны) ──

    private async Task<Result<ProcessOutcome>> ValidateAsync(
        ClusterSnapshot snap, ShardSpec shard, RestoreOperationState op,
        ClusterBackups? mine, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // 1. Усыновлённые (object) ноды — ручной путь, restore plain-only.
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return await TransientAsync(cluster, $"portalloc-unavailable/{shard.Name}/{op.Id}",
                addresses.Error!.Message, ct);
        var objectNode = addresses.Value.Keys
            .Where(k => k.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
            .Any(k => addresses.Value[k].Object is { Length: > 0 });
        if (objectNode)
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                "restore усыновлённых шардов не поддерживается (ручной путь — docs/backup-restore.md)", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // 2. Source-префикс («<srcC>/<srcX>»; default — собственный).
        var parts = op.Source.Split('/', 2);
        if (parts.Length != 2 || parts[0].Length == 0 || parts[1].Length == 0)
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                $"битый source '{op.Source}'", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        var (srcC, srcX) = (parts[0], parts[1]);
        var ownSource = srcC == cluster && srcX == shard.Name;

        // 3. backup_id: заявка → свой свежий COMPLETED (etcd) → DR-list S3.
        FullBackupState? ownFresh = null;
        var backupId = op.BackupId;
        if (backupId.Length == 0 && ownSource && mine?.Shards.TryGetValue(shard.Name, out var sb) == true)
            ownFresh = sb.Full.Where(f => f.State == FullBackupStatus.Completed)
                .OrderByDescending(f => f.Id, StringComparer.Ordinal).FirstOrDefault();
        if (backupId.Length == 0 && ownFresh is { } fresh)
            backupId = fresh.Id;
        if (backupId.Length == 0)
        {
            // DR-ветка (source-override или etcd-статусов нет): новейший = max Id
            // (id — сортируемая метка времени, Ordinal).
            var fulls = await s3.ListFullsAsync(srcC, srcX, ct: ct);
            if (!fulls.IsSuccess)
                return await TransientAsync(cluster, $"s3-unavailable/{shard.Name}/{op.Id}",
                    fulls.Error!.Message, ct);
            if (fulls.Value.Count == 0)
            {
                await FailPermanentAsync(cluster, shard.Name, op,
                    $"полные в {srcC}/{srcX} не найдены", ct);
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
            }
            backupId = fulls.Value[^1];
        }

        // 4. Факт целостности кандидата: upload t02 (mc cp --recursive) не
        // атомарен и не гарантирует «манифест последним» — упавший на середине
        // джоб оставляет частичный префикс full/<id>/, а при DR etcd-статусов
        // нет; инвариант «префикс ⇔ манифест» механикой t02 НЕ обеспечивается →
        // частичный кандидат отсеивается проверкой манифеста.
        var manifest = await s3.DownloadTextAsync(srcC, srcX, $"full/{backupId}/backup_manifest", ct);
        if (!manifest.IsSuccess)
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                $"полный {backupId} без backup_manifest (недокачан/бит)", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // 5. Стартовая точка WAL: свой свежий полный → etcd-статус; иначе
        // backup_label из S3 (BackupLabel.WalStartSegment — sed-эквивалент t02).
        string? walStart = ownFresh?.WalStartSegment;
        if (walStart is not { Length: > 0 })
        {
            var label = await s3.DownloadTextAsync(srcC, srcX, $"full/{backupId}/backup_label", ct);
            walStart = label.IsSuccess ? Restore.BackupLabel.WalStartSegment(label.Value) : null;
            if (walStart is null)
            {
                await FailPermanentAsync(cluster, shard.Name, op,
                    $"full/{backupId}: backup_label недоступен/бит", ct);
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
            }
        }
        var chainStart = WalFileName.TryParse(walStart);
        if (chainStart is null)
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                $"full/{backupId}: wal_start '{walStart}' не разбирается", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // 6. Непрерывность WAL-цепочки от стартовой точки (WalChain t03):
        // дыра → permanent с границами; S3-отказ → transient (статус не меняем).
        var wal = await s3.ListWalAsync(srcC, srcX, ct: ct);
        if (!wal.IsSuccess)
            return await TransientAsync(cluster, $"s3-unavailable/{shard.Name}/{op.Id}",
                wal.Error!.Message, ct);
        var chain = WalChain.Check(chainStart.Value, wal.Value.Select(o => o.Name));
        if (!chain.IsContinuous)
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                chain.GapError ?? "дыра WAL-цепочки", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // 7. Успех: RUNNING (journal-before-manipulations — демонтаж в Task 9
        // начинается только после видимого RUNNING).
        var running = op with
        {
            State = RestoreStatus.Running,
            BackupId = backupId,
            StartedUnix = NowUnix(),
        };
        var put = await PutStatusAsync(cluster, shard.Name, running, ct);
        if (!put.IsSuccess)
            return Result<ProcessOutcome>.Failed(put.Error!);
        await journal.WritePhaseAsync(cluster, Op, $"validated/{shard.Name}/{op.Id}", claims.InstanceId, null, ct);
        return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
    }

    // ── RUNNING: демонтаж шарда + ephemeral restore-джоб (§3.4) ──

    private async Task<Result<ProcessOutcome>> RunAsync(
        ClusterSnapshot snap, ShardSpec shard, RestoreOperationState op, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Первая нода (джоб пишет восстановленный PGDATA в её data-volume);
        // адрес из portalloc, движок — по хосту (null → transient).
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return await TransientAsync(cluster, $"portalloc-unavailable/{shard.Name}/{op.Id}",
                addresses.Error!.Message, ct);
        var first = shard.Nodes.Count > 0 ? shard.Nodes.Min(n => n.Name) : null;
        if (first is null
            || !addresses.Value.TryGetValue($"{shard.Name}/{first}", out var addr))
        {
            // portalloc рассинхронизирован с декларацией — заявка не исполнима
            await FailPermanentAsync(cluster, shard.Name, op,
                $"нода {first ?? "?"} шарда {shard.Name} не найдена в portalloc", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        var engine = driver.EngineFor(addr.Host);
        if (engine is null)
            return await TransientAsync(cluster, $"engine-unavailable/{shard.Name}/{op.Id}",
                $"docker-хост {addr.Host} не известен", ct);

        // Цель: "time:<RFC3339>" → строка конфига recovery_target_time; latest → "".
        // RFC3339 нормализуем к PG-формату: парсер recovery_target_time не
        // принимает ни «T»-сепаратор ISO-8601, ни суффикс «Z» (инцидент E2E:
        // FATAL invalid value) — приняты «пробел» и «+00:00». Битое время
        // (ручная запись мимо API) — permanent FAILED.
        string targetTime = "";
        if (op.Target.StartsWith("time:", StringComparison.Ordinal))
        {
            if (!DateTimeOffset.TryParse(op.Target["time:".Length..],
                    CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal,
                    out var targetTs))
            {
                await FailPermanentAsync(cluster, shard.Name, op,
                    $"target_time не RFC3339: {op.Target["time:".Length..]}", ct);
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
            }

            targetTime = targetTs.ToUniversalTime().ToString(
                "yyyy-MM-dd HH:mm:sszzz", CultureInfo.InvariantCulture);
        }

        var name = BackupNames.RestoreContainerName(cluster, shard.Name, op.Id);
        var list = await engine.ListContainersAsync(name, all: true, ct);
        if (!list.IsSuccess)
            return await TransientAsync(cluster, $"docker-unavailable/{shard.Name}/{op.Id}",
                list.Error!.Message, ct);
        var found = list.Value.FirstOrDefault(c => c.Names.Contains(name));

        // Идемпотентный запуск: джоба нет → демонтаж + create + start; старт —
        // в обоих случаях (created прошлым тиком / только что созданный); отказ
        // create/start → transient, следующий тик повторит.
        if (found is null)
        {
            // Демонтаж — ТОЛЬКО до первого запуска джоба: повтор при живом/
            // завершённом джобе упирается 409 «volume is in use» (джоб держит
            // data-volume первой ноды) и блокирует обработку его итога — статус
            // замирал в RUNNING навсегда (инцидент E2E-гейта t05). Контейнер
            // существует ⇒ демонтаж уже выполнен тиком запуска.
            var demolished = await DemolishAsync(cluster, shard, op, ct);
            if (!demolished.IsSuccess)
                return Result<ProcessOutcome>.Failed(demolished.Error!);

            // BACKUP_ID — резолвнутый валидацией полный (op.BackupId), НЕ id
            // заявки: джоб качает full/<backup_id>/ (инцидент E2E: в спеку
            // уходил op.Id — mc «Object does not exist»).
            if (op.BackupId.Length == 0)
            {
                await FailPermanentAsync(cluster, shard.Name, op,
                    "backup_id не зафиксирован в заявке (ожидается после валидации)", ct);
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
            }

            var spec = Restore.RestoreJobSpec.Build(options, cluster, shard.Name, op.BackupId,
                $"pgw-{cluster}-{shard.Name}-{first}-data", targetTime,
                SrcCluster(op), SrcShard(op));
            var created = await engine.CreateContainerAsync(spec, name, ct);
            if (!created.IsSuccess)
                return await TransientAsync(cluster, $"docker-unavailable/{shard.Name}/{op.Id}",
                    created.Error!.Message, ct);
        }

        if (found is not { State: "running" or "exited" })
        {
            var started = await engine.StartContainerAsync(name, ct);
            if (!started.IsSuccess)
                return await TransientAsync(cluster, $"docker-unavailable/{shard.Name}/{op.Id}",
                    started.Error!.Message, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
        }

        // created — аномалия (start потерялся между тиками): довыгоняем (304 = ок).
        if (found is { State: "created" })
        {
            await engine.StartContainerAsync(name, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
        }

        // Логи — транспорт guarded: недоступны → transient (статус не меняем).
        var logs = await engine.GetContainerLogsAsync(name, tail: 200, ct);
        if (!logs.IsSuccess)
            return await TransientAsync(cluster, $"docker-unavailable/{shard.Name}/{op.Id}",
                logs.Error!.Message, ct);
        var markers = Restore.RestoreJobLog.Parse(logs.Value);

        if (found is { State: "running" or "restarting" })
        {
            // фаза джоба → Phase статуса (пишем только при изменении —
            // панель видит downloading/recovering, arch/19 §4)
            if (markers.Phase is { Length: > 0 } phase && phase != op.Phase)
            {
                var putPhase = await PutStatusAsync(cluster, shard.Name,
                    op with { Phase = phase }, ct);
                if (!putPhase.IsSuccess)
                    return Result<ProcessOutcome>.Failed(putPhase.Error!);
            }

            // бюджет наката (§3.5): started_unix + RecoveryTimeoutSec + 60 (запас
            // на download) — докилл и permanent-FAILED (Phase не меняем).
            var startedUnix = op.StartedUnix ?? NowUnix();
            var budgetSec = options.RestoreRecoveryTimeoutSec + 60;
            if (NowUnix() - startedUnix > budgetSec)
            {
                await engine.RemoveContainerAsync(name, force: true, ct);
                await FailPermanentAsync(cluster, shard.Name, op,
                    $"recovery-бюджет исчерпан ({budgetSec} c)", ct);
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
            }

            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress); // жив — ждём
        }

        // exited: exit-код — истина итога (инспект недоступен → transient —
        // без гварда успешный restore ушёл бы в ЛОЖНЫЙ FAILED, прецедент t02).
        var inspect = await engine.InspectContainerAsync(found.Id, ct);
        if (!inspect.IsSuccess)
            return await TransientAsync(cluster, $"docker-unavailable/{shard.Name}/{op.Id}",
                inspect.Error!.Message, ct);
        var exitCode = inspect.Value.ExitCode ?? -1;
        if (exitCode == 0 && markers.Result is { Ok: true } ok)
        {
            // SUCCESS: REJOINING (Task 10 поднимет ноды); RestoredToLsn/SystemId
            // — из result (system_id старым образам джоба не знаком — null)
            var rejoining = op with
            {
                State = RestoreStatus.Rejoining,
                RestoredToLsn = ok.RestoredToLsn,
                SystemId = ok.SystemId,
            };
            var put = await PutStatusAsync(cluster, shard.Name, rejoining, ct);
            if (!put.IsSuccess)
                return Result<ProcessOutcome>.Failed(put.Error!);
            await journal.WritePhaseAsync(cluster, Op, $"restored/{shard.Name}/{op.Id}",
                claims.InstanceId, null, ct);
            await engine.RemoveContainerAsync(name, force: true, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
        }

        // FAIL: причина из result-JSON или exit-код; чистим контейнер; volume
        // первой ноды удаляем (мог остаться битым — restoration = конец, шард
        // остаётся разобранным, разбор по runbook).
        var error = markers.Result is { Ok: false, Error: { } reason } ? reason : $"exit {exitCode}";
        await engine.RemoveContainerAsync(name, force: true, ct);
        await engine.RemoveVolumeAsync($"pgw-{cluster}-{shard.Name}-{first}-data", ct);
        await FailPermanentAsync(cluster, shard.Name, op, error, ct);
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Демонтаж шарда под restore (§3.4, идемпотентно — каждый шаг 404 = ок):
    // агенты бэкапов сносятся первыми (WAL-агент на снесённом мастере не живёт),
    // ноды удаляются с data-volume («погиб диск»), HA-scope Patroni чистится
    // (Д3-образец ProvisioningProcess.ResetScopeAsync; дубль осознан — прецедент
    // кодовой базы); request_* НЕ трогаем (заявка ресурсов — декларация).
    private async Task<Result> DemolishAsync(
        string cluster, ShardSpec shard, RestoreOperationState op, CancellationToken ct)
    {
        var agents = await driver.RemoveBackupAgentsAsync(cluster, shard.Name, ct);
        if (!agents.IsSuccess)
            return agents;

        foreach (var node in shard.Nodes)
        {
            // REBUILDING — видимый статус демонтажа (панель/гварды), Removing не трогаем.
            if (node.State != NodeState.Removing)
            {
                var put = await etcd.PutAsync(endpoints[0],
                    $"/clusters/{cluster}/shards/{shard.Name}/nodes/{node.Name}/state",
                    "REBUILDING", null, ct);
                if (!put.IsSuccess)
                    return put;
            }
            var removed = await driver.RemoveNodeAsync(cluster, shard.Name, node.Name, ct);
            if (!removed.IsSuccess)
                return removed;
        }

        var scope = $"{cluster}-{shard.Name}";
        // status чистим ОБЯЗАТЕЛЬНО: в нём optime прошлой жизни лидера; без чистки
        // восстановленная нода (LSN = точке restore, ниже старого optime при PITR-откате)
        // «отстаёт» больше maximum_lag_on_failover и навечно отказывается избираться
        foreach (var key in new[] { "initialize", "leader", "sync", "status" })
        {
            var del = await etcd.DeleteAsync(endpoints[0], $"/service/{scope}/{key}", prefix: false, ct);
            if (!del.IsSuccess)
                return del;
        }
        foreach (var prefix in new[] { $"/service/{scope}/optime/", $"/service/{scope}/members/" })
        {
            var del = await etcd.DeleteAsync(endpoints[0], prefix, prefix: true, ct);
            if (!del.IsSuccess)
                return del;
        }

        // Щит против re-bootstrap (инцидент E2E-гейта t05, 2026-09-13): в окне
        // демонтаж→джоба→rejoin (~15–30 c) пустая нода, поднятая кем-то параллельно
        // (супервиз/гонка тиков), без initialize успевает initdb'нуться и стать
        // лидером НОВОГО пустого кластера — восстановленная навечно получает
        // «system ID mismatch». Заполнитель закрывает бутстрап; в rejoin'е его
        // перезапишет настоящий system_id восстановленного PGDATA.
        var shield = await etcd.PutAsync(
            endpoints[0], $"/service/{scope}/initialize", "restore-in-progress", null, ct);
        if (!shield.IsSuccess)
            return shield;

        return await journal.WritePhaseAsync(cluster, Op, $"demolished/{shard.Name}/{op.Id}",
            claims.InstanceId, null, ct);
    }

    // Source «<srcC>/<srcX>»: компоненты для env спеки (валидация PLANNED уже
    // проверила формат).
    private static string SrcCluster(RestoreOperationState op)
        => op.Source.Split('/', 2)[0];

    private static string SrcShard(RestoreOperationState op)
        => op.Source.Split('/', 2)[1];

    // ── REJOINING: ensure нод + Patroni-пробы + COMPLETED (§3.4, AC4) ──

    // Трекер ожидания Patroni (диагностика takeover: состояние в etcd-статусе
    // заявки, трекер — только бюджет ожидания; прецедент _patroniWaitSince).
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> _rejoinWaitSince = new();

    private async Task<Result<ProcessOutcome>> RejoinAsync(
        ClusterSnapshot snap, ShardSpec shard, RestoreOperationState op, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        var waitKey = $"{cluster}/{shard.Name}/{op.Id}";

        // Креды (P1.5-копия): ensured тройка — env нод (mover/bucket_admin —
        // канонические ключи etcd).
        var creds = await appSecret.EnsureAsync(cluster, snap.Config, ct);
        if (!creds.IsSuccess)
            return Result<ProcessOutcome>.Failed(creds.Error!);
        var clusterSecrets = secrets with
        {
            BucketAdminUser = creds.Value.BucketAdmin.User,
            BucketAdminPassword = creds.Value.BucketAdmin.Password,
            MoverPassword = creds.Value.MoverPassword,
        };

        // Адреса/топология/заявка ресурсов (portalloc жив — restore его не трогал).
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return await TransientAsync(cluster, $"portalloc-unavailable/{shard.Name}/{op.Id}",
                addresses.Error!.Message, ct);
        var topology = Topology(cluster, shard.Name, addresses.Value);
        var resources = await ReadShardResourcesAsync(cluster, shard.Name, ct);

        // Первая нода — на volume с восстановленным PGDATA (джоб t05 уже
        // записал его в pgw-<C>-<X>-<n>-data; docker смонтирует существующий).
        var ordered = shard.Nodes.Select(n => n.Name).OrderBy(n => n, StringComparer.Ordinal).ToList();
        var first = ordered[0];
        if (!addresses.Value.TryGetValue($"{shard.Name}/{first}", out var firstAddr))
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                $"нода {first} шарда {shard.Name} не найдена в portalloc", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        // Щит initialize (см. DemolishAsync): заполнитель «restore-in-progress»
        // перезаписывается system_id восстановленного PGDATA ДО ensure первой
        // ноды — patroni восстановленной ноды видит совпадение и лидерится, а
        // пустые ноды в гонке initdb'нуться не могут. Старый образ джоба не
        // присылает system_id — щит остаётся заполнителем (только warn).
        if (op.SystemId is { Length: > 0 } sysid)
        {
            var putShield = await etcd.PutAsync(
                endpoints[0], $"/service/{cluster}-{shard.Name}/initialize", sysid, null, ct);
            if (!putShield.IsSuccess)
                return await TransientAsync(cluster, $"etcd-unavailable/{shard.Name}/{op.Id}",
                    putShield.Error!.Message, ct);
        }
        else
        {
            logger?.LogWarning(
                "backup-restore {Cluster}/{Shard}/{Op}: system_id не получен от джобы — initialize остаётся заполнителем",
                cluster, shard.Name, op.Id);
        }

        var firstEnsure = await driver.EnsureNodeAsync(
            topology, first, firstAddr, clusterSecrets, etcdEndpoints, resources, ct);
        if (!firstEnsure.IsSuccess)
            return await TransientAsync(cluster, $"docker-unavailable/{shard.Name}/{op.Id}",
                firstEnsure.Error!.Message, ct);

        // Идентифицирующая Patroni-проба (P2.2-образец): лидер восстановленного
        // шарда должен быть ВЫБРАН (role primary) до подъёма реплик. state=running
        // недостаточно: джоба промоутила PG до Patroni, «running» бывает мгновенно,
        // до победы в выборах — ранний подъём реплик ломает выборы навечно
        // (не-избранный лидер уходит в following, «not the healthiest»).
        var members = await probe.GetClusterAsync(firstAddr, ct);
        var firstReady = members.IsSuccess
                         && members.Value.Any(m => m.Name == first
                             && m.Role is "leader" or "primary" or "standby_leader"
                             && m.State is "running" or "streaming");
        if (!firstReady)
            return await RejoinWaitAsync(cluster, shard.Name, op, waitKey, firstAddr, ct);

        // Остальные ноды — чистыми (драйвер создаст volume, реплики догоняются
        // pg_basebackup от лидера).
        foreach (var node in ordered.Skip(1))
        {
            if (!addresses.Value.TryGetValue($"{shard.Name}/{node}", out var addr))
            {
                await FailPermanentAsync(cluster, shard.Name, op,
                    $"нода {node} шарда {shard.Name} не найдена в portalloc", ct);
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
            }
            var ensured = await driver.EnsureNodeAsync(
                topology, node, addr, clusterSecrets, etcdEndpoints, resources, ct);
            if (!ensured.IsSuccess)
                return await TransientAsync(cluster, $"docker-unavailable/{shard.Name}/{op.Id}",
                    ensured.Error!.Message, ct);
        }

        // Пробы всех нод (тот же трекер-бюджет). Контракт (решение t05, 2026-09-12):
        // COMPLETED ждёт ЛИДЕР running/streaming + реплики, УСПЕШНО СТАРТОВАВШИЕ
        // синхронизацию — "creating replica" (pg_basebackup от лидера пошёл) или
        // уже готовые ("running"/"streaming"). Полного окончания basebackup ждёт
        // не заявка, а Patroni (свой retry) — rejoin на нём не висит. Patroni 4.x
        // у healthy-реплики отдаёт state "streaming" (инцидент E2E-гейта t05).
        var clusterState = await probe.GetClusterAsync(firstAddr, ct);
        var allStarted = clusterState.IsSuccess
                         && ordered.All(n => clusterState.Value.Any(
                             m => m.Name == n
                                  && m.State is "running" or "streaming" or "creating replica"));
        if (!allStarted)
            return await RejoinWaitAsync(cluster, shard.Name, op, waitKey, firstAddr, ct);

        // COMPLETED (AC4): ноды RUNNING; wal-ключ шарда удаляется — сброс цепочки,
        // планировщик t02 немедленно переснимает полный; мастер-ключ обновит сам
        // лидер (lease-скрипт P11) — RestoreProcess его не пишет.
        foreach (var node in ordered)
        {
            var put = await etcd.PutAsync(endpoints[0],
                $"/clusters/{cluster}/shards/{shard.Name}/nodes/{node}/state", "RUNNING", null, ct);
            if (!put.IsSuccess)
                return Result<ProcessOutcome>.Failed(put.Error!);
        }

        var completed = op with
        {
            State = RestoreStatus.Completed,
            FinishedUnix = NowUnix(),
        };
        var putDone = await PutStatusAsync(cluster, shard.Name, completed, ct);
        if (!putDone.IsSuccess)
            return Result<ProcessOutcome>.Failed(putDone.Error!);
        var delWal = await etcd.DeleteAsync(endpoints[0],
            $"/pgworker/backups/{cluster}/{shard.Name}/wal", prefix: false, ct);
        if (!delWal.IsSuccess)
            return Result<ProcessOutcome>.Failed(delWal.Error!);
        await journal.WritePhaseAsync(cluster, Op, $"done/{shard.Name}/{op.Id}", claims.InstanceId, null, ct);
        _rejoinWaitSince.TryRemove(waitKey, out _);
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Бюджет ожидания Patroni (PatroniBootSec): не готово → InProgress; исчерпан
    // → permanent-FAILED (трекер снять — новая заявка получает полный бюджет).
    // Телеметрия (t05, гейт 2026-09-13): каждый 10-й тик пишет warn с живым
    // снимком пробы — журнал воркера обязан объяснять «почему так долго» без
    // перезапуска теста (в терминальном FAILED он приходит слишком поздно).
    private async Task<Result<ProcessOutcome>> RejoinWaitAsync(
        string cluster, string shard, RestoreOperationState op, string waitKey,
        NodeAddress? firstAddr, CancellationToken ct)
    {
        var now = NowUnix();
        var since = _rejoinWaitSince.GetOrAdd(waitKey, now);
        var waited = now - since;
        if (waited > thresholds.PatroniBootSec)
        {
            _rejoinWaitSince.TryRemove(waitKey, out _);
            // Диагностика в причине: снимок Patroni-модели на момент фейла —
            // кто лидер, в каком состоянии ноды (иначе фейл нем и флейки нечему учить)
            var snapshot = "";
            if (firstAddr is { } addr)
            {
                var diag = await probe.GetClusterAsync(addr, ct);
                snapshot = diag.IsSuccess
                    ? "; members=" + string.Join(",",
                        diag.Value.Select(m => $"{m.Name}:{m.Role}:{m.State}"))
                    : "; probe=" + diag.Error!.Message;
            }
            await FailPermanentAsync(cluster, shard, op,
                $"Patroni не поднялся за {thresholds.PatroniBootSec} с{snapshot}", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // Каждый 10-й тик (тик ≈ 1 c): живой снимок пробы в журнал воркера.
        if (waited > 0 && waited % 10 == 0)
        {
            var progress = "";
            if (firstAddr is { } addr)
            {
                var diag = await probe.GetClusterAsync(addr, ct);
                progress = diag.IsSuccess
                    ? "members=" + string.Join(",", diag.Value.Select(m => $"{m.Name}:{m.Role}:{m.State}"))
                    : "probe=" + diag.Error!.Message;
            }

            logger?.LogWarning(
                "backup-restore {Cluster}/{Shard}/{Op}: rejoin-wait {Waited}s/{Budget}s, probe {Addr}: {Progress}",
                cluster, shard, op.Id, waited, thresholds.PatroniBootSec, firstAddr?.ToString() ?? "-", progress);
        }

        return await TransientAsync(cluster, $"rejoin-wait/{shard}/{op.Id}", null, ct);
    }

    // Приватная копия ProvisioningProcess.Topology (прецедент кодовой базы).
    private static ShardTopology Topology(
        string cluster, string shard, IReadOnlyDictionary<string, NodeAddress> addresses)
        => new(cluster, shard, $"{cluster}-{shard}",
            addresses
                .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                .ToDictionary(p => p.Key.Split('/')[1], p => p.Value));

    // Упрощённая копия ProvisioningProcess.ReadShardResourcesAsync: заявка
    // request_cpu/request_mem scope; нет/битые → null (лимиты не заданы).
    private async Task<NodeResources?> ReadShardResourcesAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var scope = $"{cluster}-{shard}";
        var cpu = await etcd.GetAsync(endpoints[0], $"/service/{scope}/request_cpu", ct);
        if (!cpu.IsSuccess)
            return null;
        var mem = await etcd.GetAsync(endpoints[0], $"/service/{scope}/request_mem", ct);
        return mem.IsSuccess ? NodeResourcesParser.Parse(cpu.Value?.Value, mem.Value?.Value) : null;
    }

    // ── Хелперы etcd/журнала ──

    private long NowUnix() => time.GetUtcNow().ToUnixTimeSeconds();

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await etcd.GetAsync(endpoints[0], $"/pgworker/portalloc/{cluster}", ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());
        return Portalloc.Parse(cluster, kv.Value);
    }

    // failover-Put статуса заявки (по образцу BackupProcess.PutAsync).
    private async Task<Result> PutStatusAsync(
        string cluster, string shard, RestoreOperationState state, CancellationToken ct)
    {
        var value = Restore.RestoreStatusJson.Serialize(state);
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, BackupNames.RestoreKey(cluster, shard, state.Id),
                value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    // permanent-отказ: FAILED + finished_unix + причина + журнал (повтор заявки — оператор).
    private async Task FailPermanentAsync(
        string cluster, string shard, RestoreOperationState op, string error, CancellationToken ct)
    {
        logger?.LogWarning("backup-restore {Cluster}/{Shard}/{Id}: FAILED — {Error}", cluster, shard, op.Id, error);
        var put = await PutStatusAsync(cluster, shard, op with
        {
            State = RestoreStatus.Failed,
            FinishedUnix = NowUnix(),
            Error = error,
        }, ct);
        if (!put.IsSuccess)
            logger?.LogError("backup-restore {Cluster}/{Shard}/{Id}: статус FAILED не записан — {Error}",
                cluster, shard, op.Id, put.Error?.Message);
        await journal.WritePhaseAsync(cluster, Op, $"failed/{shard}/{op.Id}", claims.InstanceId, error, ct);
    }

    // transient-отказ: статус не меняем, журнал-факт, следующий тик повторит.
    private async Task<Result<ProcessOutcome>> TransientAsync(
        string cluster, string phase, string? error, CancellationToken ct)
    {
        await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error, ct);
        return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
    }
}
