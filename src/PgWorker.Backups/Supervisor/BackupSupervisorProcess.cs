using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Backups.Supervisor;

/// <summary>BackupSupervisorProcess — per-cluster сверка S3↔etcd (arch/19
/// §4) под клэймом &lt;C&gt; по расписанию Supervisor:IntervalSec: объекты
/// full/&lt;id&gt;.tar живого шарда БЕЗ etcd-ключа — мусор (выбора кандидатов
/// restore нет) — удаление ОДНОГО объекта с list-подтверждением и
/// journal-фактом swept-full; висячие multipart-сессии старше порога гасятся
/// Abort'ом (spec t01 §3.7). WAL-префикс НЕ трогаем (владельцы — контроль цепочки и
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
                await SweepShardAsync(
                    cluster, shard.Name, shardBackups, garbageMinAgeSec, s3, poolCtx, options, ct);
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

    // Сверка одного шарда: list full/-объектов → отбор без etcd-ключа →
    // гейты мусора (arch/19 §4): свежее чтение ключа + возрастной гейт
    // КОНКРЕТНОГО объекта → удаление одного full/<id>.tar с list-подтверждением
    // (иначе журнал-ошибка — следующий проход повторит, идемпотентно); в конце —
    // гигиена висячих multipart-сессий шарда (spec t01 §3.7).
    private async Task SweepShardAsync(
        string cluster, string shard, ShardBackups? shardBackups,
        long garbageMinAgeSec, IBackupS3 s3, BackupPoolContext pool,
        BackupsRuntimeOptions options, CancellationToken ct)
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

            var fullPrefix = $"{cluster}/{shard}/full/";
            var key = $"{fullPrefix}{id}.tar";
            var objects = await S3PulseAsync(token => s3.ListPrefixAsync(fullPrefix, ct: token), ct);
            if (!objects.IsSuccess)
                throw new ApplicationException($"list {fullPrefix}: {objects.Error!.Message}");
            var target = objects.Value.FirstOrDefault(o =>
                string.Equals(o.Key, key, StringComparison.Ordinal));
            if (target is not null)
            {
                // Гейт 2 : возраст КОНКРЕТНОГО объекта моложе порога → заливка
                // in-flight (снапшот мог отстать, гейт 1 — страховка фантомов) —
                // не мусор, остаётся следующему проходу.
                if (!SupervisorPlanner.IsGarbageMatured(target.LastModified, garbageMinAgeSec, time.GetUtcNow()))
                {
                    logger?.LogInformation(
                        "{Op} {Cluster}/{Shard}: object {Id}.tar is younger than {Age}s — not garbage yet",
                        Op, cluster, shard, id, garbageMinAgeSec);
                    continue;
                }

                if (s3 is not IDeleteObjectS3 deleter)
                    throw new ApplicationException(
                        $"{Op} {cluster}/{shard}: pool client does not support single-object delete");
                var deleted = await S3PulseAsync(
                    token => deleter.DeleteObjectAsync(cluster, shard, $"full/{id}.tar", token), ct);
                if (!deleted.IsSuccess)
                    throw new ApplicationException($"delete {key}: {deleted.Error!.Message}");
                var recheck = await S3PulseAsync(token => s3.ListPrefixAsync(fullPrefix, ct: token), ct);
                if (!recheck.IsSuccess
                    || recheck.Value.Any(o => string.Equals(o.Key, key, StringComparison.Ordinal)))
                    throw new ApplicationException(
                        $"deletion of {key} did not complete — next pass will retry");
            }

            // Объекта не было или удалён — запись журнала.
            await journal.WritePhaseAsync(cluster, Op, $"swept-full/{shard}/{id}",
                claims.InstanceId, null, ct);
            logger?.LogInformation("{Op} {Cluster}/{Shard}: object {Id}.tar without etcd key deleted",
                Op, cluster, shard, id);
        }

        await AbortStaleMultipartAsync(cluster, shard, garbageMinAgeSec, pool, options, ct);
    }

    // NEW-гигиена (spec t01 §3.7): висячие multipart-сессии шарда (незавершённые
    // попытки полного после отказа/смерти приёмника) старше порога мусора —
    // Abort на СВОЁМ endpoint'е сессии. Write-список полного фиксируется ПО
    // РОЛИ на старте операции, роль супервизор не знает — обходятся ОБА списка
    // пула (дедуп по endpoint); транспортный отказ endpoint'а — warning и
    // продолжение (следующий проход повторит).
    private async Task AbortStaleMultipartAsync(
        string cluster, string shard, long garbageMinAgeSec, BackupPoolContext pool,
        BackupsRuntimeOptions options, CancellationToken ct)
    {
        var endpoints = pool.WriteMaster.Concat(pool.WriteReplica)
            .Distinct(StringComparer.Ordinal)
            .ToList();
        foreach (var endpoint in endpoints)
        {
            await using var client = new BackupS3(new S3ClientOptions(
                endpoint, options.S3Bucket, pool.AccessKey, pool.SecretKey,
                CaBundlePath: options.S3CaBundlePath, MaxErrorRetry: 0));
            var uploads = await S3PulseAsync(
                token => client.ListMultipartUploadsAsync($"{cluster}/{shard}/", token), ct);
            if (!uploads.IsSuccess)
            {
                logger?.LogWarning(
                    "{Op} {Cluster}/{Shard}: list multipart on {Endpoint} failed: {Error}",
                    Op, cluster, shard, endpoint, uploads.Error!.Message);
                continue;
            }

            foreach (var upload in uploads.Value)
            {
                if (!SupervisorPlanner.IsGarbageMatured(
                        upload.Initiated, garbageMinAgeSec, time.GetUtcNow()))
                    continue;
                var aborted = await S3PulseAsync(
                    token => client.AbortMultipartUploadAsync(upload.Key, upload.UploadId, token), ct);
                var uploadPrefix = upload.UploadId[..Math.Min(8, upload.UploadId.Length)];
                await journal.WritePhaseAsync(cluster, Op, $"aborted-mpu/{shard}/{uploadPrefix}",
                    claims.InstanceId, aborted.IsSuccess ? null : aborted.Error!.Message, ct);
                if (aborted.IsSuccess)
                {
                    logger?.LogInformation(
                        "{Op} {Cluster}/{Shard}: aborted stale multipart upload {Key} ({UploadId} started {Initiated})",
                        Op, cluster, shard, upload.Key, uploadPrefix, upload.Initiated);
                }
            }
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
