using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Backups.Supervisor;

/// <summary>BackupSupervisorProcess — per-cluster сверка S3↔etcd (arch/19
/// §4) под клэймом &lt;C&gt; по расписанию Supervisor:IntervalSec: префиксы
/// full/&lt;id&gt;/ живого шарда БЕЗ etcd-ключа — мусор (выбора кандидатов
/// restore нет) — немедленный batch-delete с list-подтверждением и journal-фактом
/// swept-full. WAL-префикс НЕ трогаем (владельцы — контроль цепочки и
/// ретенция). Гварды: клэйм, Enabled (runtime()==null — no-op), Active;
/// шард в активном restore — skip (гвард владельца). Таймауты операций — в их
/// процессах (§3.3), здесь только сверка хранилища. Ошибка шарда не роняет
/// остальные (образец WalStreamProcess); повторный проход — no-op
/// (list подтверждает пустоту, образец FinishDeletionAsync).</summary>
public sealed class BackupSupervisorProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    BackupS3Factory s3Factory,
    ClaimStore claims,
    WorkJournal journal,
    Func<BackupsRuntimeOptions?> runtime,
    TimeProvider time,
    Shared.Core.Hosting.ILoopProgress? progress = null,
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
                $"{Op} {cluster}: claim is not ours (or lost) — mutations are forbidden"));

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

        //  (arch/19 §5): пул-клиент кластера (удаления сирот full/<id>/ —
        // первая доступная нода: put/delete-семантика пула). null (пул не
        // выбран) → проход молчит; Failed → journal, ретраи проходом.
        var poolRead = await s3Factory.ResolveAsync(cluster, ct);
        if (!poolRead.IsSuccess)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-pool", claims.InstanceId,
                poolRead.Error!.Message, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        if (poolRead.Value is not { } poolCtx)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        // Клиент — кэш фабрики (breaker переживает тики) — НЕ диспоузим.
        var s3 = s3Factory.Create(poolCtx);

        foreach (var shard in snap.Shards)
        {
            if (shard.ToRemove)
                continue; // демонтаж — домен RemoveShardProcess

            try
            {
                var shardBackups = backups?.Shards.GetValueOrDefault(shard.Name);

                // Гвард владельца (§3.4): шард в активном restore — контуры
                // бэкапов его не трогают (объекты качает restore-джоб).
                if (shardBackups?.Restores.Any(r => r.State
                        is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining) == true)
                    continue;

                // Возрастной гейт мусора (arch/19 §4 «Гейты мусора»):
                // policy-ключ кластера перекрывает дефолт конфига.
                var garbageMinAgeSec = backups?.Policy?.GarbageMinAgeSec ?? options.GarbageMinAgeSec;
                await SweepShardAsync(cluster, shard.Name, shardBackups, garbageMinAgeSec, s3, ct);
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
    // гейты мусора (arch/19 §4): свежее чтение ключа + возрастной гейт →
    // batch-delete каждого с list-подтверждением пустоты (иначе журнал-ошибка —
    // следующий проход повторит, идемпотентно).
    private async Task SweepShardAsync(
        string cluster, string shard, ShardBackups? shardBackups,
        long garbageMinAgeSec, IBackupS3 s3, CancellationToken ct)
    {
        var listed = await S3PulseAsync(token => s3.ListFullsAsync(cluster, shard, ct: token), ct);
        if (!listed.IsSuccess)
            throw new ApplicationException($"list fulls {cluster}/{shard}: {listed.Error!.Message}");

        var unowned = SupervisorPlanner.SelectUnownedFulls(listed.Value, shardBackups?.Full ?? []);
        foreach (var id in unowned)
        {
            // Гейт 1 : свежее чтение etcd-ключа — снапшот тика снят ДО
            // планирования; планировщик этого же тика мог запланировать и
            // запустить полный ПОСЛЕ снапшота (заливка уже в S3, ключа в
            // снапшоте нет — ложный unowned). Ключ появился → живой полный.
            var fresh = await GetFullKeyAsync(cluster, shard, id, ct);
            if (!fresh.IsSuccess)
                throw new ApplicationException(
                    $"fresh key check {cluster}/{shard}/{id}: {fresh.Error!.Message}");
            if (fresh.Value is not null)
                continue;

            var prefix = $"{cluster}/{shard}/full/{id}/";
            var objects = await S3PulseAsync(token => s3.ListPrefixAsync(prefix, ct: token), ct);
            if (!objects.IsSuccess)
                throw new ApplicationException($"list {prefix}: {objects.Error!.Message}");
            if (objects.Value.Count > 0)
            {
                // Гейт 2 : самый старый объект префикса моложе порога →
                // заливка in-flight (снапшот мог отстать, гейт 1 — страховка
                // фантомов) — не мусор, остаётся следующему проходу.
                var oldest = objects.Value.Min(o => o.LastModified);
                if (!SupervisorPlanner.IsGarbageMatured(oldest, garbageMinAgeSec, time.GetUtcNow()))
                {
                    logger?.LogInformation(
                        "{Op} {Cluster}/{Shard}: prefix {Id} is younger than {Age}s — not garbage yet",
                        Op, cluster, shard, id, garbageMinAgeSec);
                    continue;
                }

                var deleted = await S3PulseAsync(token => s3.DeleteKeysAsync(
                    objects.Value.Select(o => o.Key).ToList(), token), ct);
                if (!deleted.IsSuccess)
                    throw new ApplicationException($"delete {prefix}: {deleted.Error!.Message}");
                var recheck = await S3PulseAsync(token => s3.ListPrefixAsync(prefix, ct: token), ct);
                if (!recheck.IsSuccess || recheck.Value.Count > 0)
                    throw new ApplicationException(
                        $"deletion of {prefix} did not complete — next pass will retry");
            }

            // Объектов не было (пустой префикс) или удалены — запись журнала.
            await journal.WritePhaseAsync(cluster, Op, $"swept-full/{shard}/{id}",
                claims.InstanceId, null, ct);
            logger?.LogInformation("{Op} {Cluster}/{Shard}: prefix {Id} without etcd key deleted",
                Op, cluster, shard, id);
        }
    }

    // Свежее чтение ключа полного (failover по endpoints — образец
    // BackupOrphanSweeper.ReadRegistryAsync): снапшот тика мог отстать от
    // планировщика этого же тика; все endpoints недоступны → Failed (transient).
    private async Task<Result<Kv?>> GetFullKeyAsync(
        string cluster, string shard, string id, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, BackupNames.FullKey(cluster, shard, id), ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return Result<Kv?>.Failed(last.Error
            ?? new ApplicationException($"fresh key {cluster}/{shard}/{id}: no live etcd endpoints"));
    }

    /// <summary>S3-вызовы сверки — с пульсом прогресса (S3Pulse, канон):
    /// прямой вызов на недоступном endpoint висит до HttpClient-таймаута без
    /// отметок — watchdog (порт сноса 30 c) гасил reconcile посреди сверки
    /// (bad-S3 — штатный прод-контур, arch/19 §4 «transient — следующий проход»).</summary>
    private async Task<Result<T>> S3PulseAsync<T>(
        Func<CancellationToken, Task<Result<T>>> call, CancellationToken ct)
        => await S3Pulse.CallAsync(progress, call, ct);

    // Негенерик-перегрузка: вызовы без значения (DeleteKeysAsync → Result).
    private async Task<Result> S3PulseAsync(
        Func<CancellationToken, Task<Result>> call, CancellationToken ct)
        => await S3Pulse.CallAsync(progress, call, ct);
}
