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

    // ── RUNNING (Task 9) / REJOINING (Task 10) — стабы до соседних задач ──

    private Task<Result<ProcessOutcome>> RunAsync(
        ClusterSnapshot snap, ShardSpec shard, RestoreOperationState op, CancellationToken ct)
        => Task.FromResult(Result<ProcessOutcome>.Success(ProcessOutcome.InProgress));

    private Task<Result<ProcessOutcome>> RejoinAsync(
        ClusterSnapshot snap, ShardSpec shard, RestoreOperationState op, CancellationToken ct)
        => Task.FromResult(Result<ProcessOutcome>.Success(ProcessOutcome.InProgress));

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
