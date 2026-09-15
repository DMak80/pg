using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Backups.Supervisor;

/// <summary>BackupSupervisorProcess — per-cluster сверка S3↔etcd (t07, arch/19
/// §4) под клэймом &lt;C&gt; по расписанию Supervisor:IntervalSec: префиксы
/// full/&lt;id&gt;/ живого шарда БЕЗ etcd-ключа — мусор (выбора кандидатов
/// restore нет) — немедленный batch-delete с list-подтверждением и journal-фактом
/// swept-full. WAL-префикс НЕ трогаем (владельцы — контроль цепочки t03 и
/// ретенция t06). Гварды: клэйм, Enabled (runtime()==null — no-op), Active;
/// шард в активном restore — skip (гвард владельца). Таймауты операций — в их
/// процессах (§3.3), здесь только сверка хранилища. Ошибка шарда не роняет
/// остальные (образец WalStreamProcess); повторный проход — no-op
/// (list подтверждает пустоту, образец FinishDeletionAsync t06).</summary>
public sealed class BackupSupervisorProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IBackupS3 s3,
    ClaimStore claims,
    WorkJournal journal,
    Func<BackupsRuntimeOptions?> runtime,
    TimeProvider time,
    ILogger? logger = null)
{
    private const string Op = "backup-supervisor";

    // Расписание per-cluster (ключ — имя кластера → unix последнего прохода;
    // образец RetentionProcess).
    private readonly ConcurrentDictionary<string, long> _lastPassUnix = [];

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, ClusterBackups? backups, CancellationToken ct)
    {
        // etcd/endpoints — параметры контракта DI (симметрия BackupOrphanSweeper);
        // per-cluster сверка читает etcd-владельцев из аргумента backups (парс
        // префикса циклом) — прямой доступ не нужен (прецедент RestoreProcess).
        _ = etcd; _ = endpoints;

        var cluster = snap.Config.Cluster;

        // Мутации S3/etcd — только держатель клэйма (инвариант arch/14 §4.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // G0: подсистема выключена — no-op (Enabled-стоп-семантика WalStreamProcess).
        var options = runtime();
        if (options is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        // Только Active-кластер (spec §3.4 per-cluster).
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();
        if (nowUnix - _lastPassUnix.GetOrAdd(cluster, 0L) < options.SupervisorIntervalSec)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        _lastPassUnix[cluster] = nowUnix;

        foreach (var shard in snap.Shards)
        {
            if (shard.ToRemove)
                continue; // демонтаж — домен RemoveShardProcess

            try
            {
                var shardBackups = backups?.Shards.GetValueOrDefault(shard.Name);

                // Гвард владельца (t05 §3.4): шард в активном restore — контуры
                // бэкапов его не трогают (объекты качает restore-джоб).
                if (shardBackups?.Restores.Any(r => r.State
                        is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining) == true)
                    continue;

                await SweepShardAsync(cluster, shard.Name, shardBackups, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // ошибка шарда не роняет остальные (образец WalStreamProcess)
                logger?.LogError(ex, "{Op} {Cluster}/{Shard}: {Message}", Op, cluster, shard.Name, ex.Message);
                await journal.WritePhaseAsync(cluster, Op, "shard-error", claims.InstanceId,
                    $"{shard.Name}: {ex.Message}", ct);
            }
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Сверка одного шарда: list full/-префиксов → отбор без etcd-ключа →
    // batch-delete каждого с list-подтверждением пустоты (иначе журнал-ошибка —
    // следующий проход повторит, идемпотентно).
    private async Task SweepShardAsync(
        string cluster, string shard, ShardBackups? shardBackups, CancellationToken ct)
    {
        var listed = await s3.ListFullsAsync(cluster, shard, ct: ct);
        if (!listed.IsSuccess)
            throw new ApplicationException($"list fulls {cluster}/{shard}: {listed.Error!.Message}");

        var unowned = SupervisorPlanner.SelectUnownedFulls(listed.Value, shardBackups?.Full ?? []);
        foreach (var id in unowned)
        {
            var prefix = $"{cluster}/{shard}/full/{id}/";
            var objects = await s3.ListPrefixAsync(prefix, ct: ct);
            if (!objects.IsSuccess)
                throw new ApplicationException($"list {prefix}: {objects.Error!.Message}");
            if (objects.Value.Count > 0)
            {
                var deleted = await s3.DeleteKeysAsync(
                    objects.Value.Select(o => o.Key).ToList(), ct);
                if (!deleted.IsSuccess)
                    throw new ApplicationException($"delete {prefix}: {deleted.Error!.Message}");
                var recheck = await s3.ListPrefixAsync(prefix, ct: ct);
                if (!recheck.IsSuccess || recheck.Value.Count > 0)
                    throw new ApplicationException(
                        $"удаление {prefix} не завершилось — повторит следующий проход");
            }

            // Объектов не было (пустой префикс) или удалены — журнал-факт.
            await journal.WritePhaseAsync(cluster, Op, $"swept-full/{shard}/{id}",
                claims.InstanceId, null, ct);
            logger?.LogInformation("{Op} {Cluster}/{Shard}: префикс {Id} без etcd-ключа удалён",
                Op, cluster, shard, id);
        }
    }
}
