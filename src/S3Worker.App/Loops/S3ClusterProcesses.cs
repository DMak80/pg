using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using S3Worker.App;
using S3Worker.Core.Model;
using S3Worker.Etcd.Parsing;
using S3Worker.Provisioning.Processes;
using Shared.Etcd.Client;

namespace S3Worker.App.Loops;

// Агрегатор процессов для ReconcileLoop (порт ClusterProcesses PgWorker/Kafka/
// Valkey): цикл не знает конкретных машин состояний — только эту грань
// (мокабельно в unit-тестах цикла). Active-ветка (arch/24 §5): надзор (C) →
// add-node (D) → remove-node (E); живая заявка ротации — эксклюзивная ветка.

/// <summary>Один reconcile-тик над всеми кластерами (процессы arch/24 §5).</summary>
public interface IS3ClusterProcesses
{
    /// <summary>Число клэймов, держимых этим инстансом (health-секция claims).</summary>
    Task<int> TickAsync(CancellationToken ct);
}

/// <summary>
/// Реализация поверх координации (синглтоны DI): Range /s3/ → парсер →
/// клэйм → классификация → процесс. Ошибки кластера не роняют тик;
/// параллелизм — SemaphoreSlim MaxClusters (внутри кластера — последовательно).
/// </summary>
internal sealed class S3ClusterProcesses(
    IEtcdGateway etcd,
    IOptionsMonitor<S3WorkerOptions> options,
    ClaimStore claims,
    WorkJournal journal,
    ProvisioningProcess provision,
    DeprovisioningProcess deprovision,
    NodeSupervisor supervisor,
    AddNodeProcess addNode,
    RemoveNodeProcess removeNode,
    MeshGovernanceProcess mesh,
    SyncProbeProcess syncProbe,
    SecretRotator rotator,
    Meter meter,
    Shared.Core.Hosting.ILoopProgress progress,
    ILogger<S3ClusterProcesses> logger) : IS3ClusterProcesses
{
    // Счётчик unknownKeys (spec §9): домен развивается — читатель не падает
    // (arch/23 §2); инкремент — размер списка за тик.
    private readonly Counter<long> _unknownKeys = meter.CreateCounter<long>("s3worker_unknown_keys_total");

    public async Task<int> TickAsync(CancellationToken ct)
    {
        var endpoints = options.CurrentValue.Etcd.Endpoints.ToArray();
        if (endpoints.Length == 0)
            throw new ApplicationException("S3Worker:Etcd:Endpoints are not configured");

        // Снапшот контроль-плейна: Range /s3/ c failover.
        var clustersKvs = await RangeWithFailoverAsync(endpoints, "/s3/", ct);
        if (!clustersKvs.IsSuccess)
            throw clustersKvs.Error!;

        var parsed = S3SnapshotParser.Parse(clustersKvs.Value);
        foreach (var cluster in parsed.Value.Clusters)
        {
            if (cluster.ParseError is not null)
                logger.LogWarning("skipped malformed key: {Error}", cluster.ParseError);
            foreach (var node in cluster.Nodes.Where(n => n.ParseError is not null))
                logger.LogWarning("skipped malformed node key: {Error}", node.ParseError);
        }

        foreach (var unknown in parsed.Value.UnknownKeys)
            logger.LogWarning("unknown key in /s3/: {Key}", unknown);
        if (parsed.Value.UnknownKeys.Count > 0)
            _unknownKeys.Add(parsed.Value.UnknownKeys.Count);

        // Параллельная обработка кластеров с лимитом; ошибка кластера не роняет тик
        // (journal процесса несёт last_error — следующий тик продолжит).
        var gate = new SemaphoreSlim(Math.Max(1, options.CurrentValue.Parallelism.MaxClusters));
        try
        {
            var tasks = parsed.Value.Clusters
                .Select(snap => ProcessClusterAsync(snap, gate, ct))
                .ToArray();
            await Task.WhenAll(tasks);
        }
        finally
        {
            gate.Dispose();
        }

        // Фактическое число СВОИХ клэймов (health-секция claims).
        return parsed.Value.Clusters.Count(c => claims.IsMine(c.Cluster));
    }

    // Обработка одного кластера под семафором: клэйм → классификация → процесс.
    // Исключение любого кластера не роняет тик: catch-all → лог + journal.
    private async Task ProcessClusterAsync(S3ClusterSnapshot snap, SemaphoreSlim gate, CancellationToken ct)
    {
        await gate.WaitAsync(ct);
        CancellationTokenSource? lease = null; // linked-токен клэйма — виден catch'ам
        try
        {
            var cluster = snap.Cluster;
            var kind = S3ClusterClassifier.Classify(snap);

            // Клэйм ДО процесса (S5, arch/17): exclusivity; занят другим — не ошибка.
            var claimed = await claims.TryClaimClusterAsync(cluster, ct);
            if (!claimed.IsSuccess)
            {
                logger.LogError(claimed.Error, "claim {Cluster} failed: {Message}", cluster, claimed.Error!.Message);
                return;
            }

            if (!claimed.Value)
                return; // обрабатывает другой инстанс

            // Cancellation-гвард клэйма (arch/17 S5): потеря lease прерывает
            // исполняющийся тик кластера — linked-токен течёт во все операции.
            lease = CancellationTokenSource.CreateLinkedTokenSource(ct, claims.LeaseToken(cluster));
            var tick = lease.Token;

            switch (kind)
            {
                case S3ClusterKind.Provision:
                    await RunClusterOpAsync(cluster, "provision",
                        () => provision.RunAsync(snap, tick), ct, tick);
                    break;

                case S3ClusterKind.Deprovision:
                    await RunClusterOpAsync(cluster, "deprovision",
                        () => deprovision.TickAsync(snap, tick), ct, tick);
                    break;

                case S3ClusterKind.Active:
                    // Active-ветка (arch/24 §5, C → D → E → G → F): живая заявка
                    // ротации — ЭКСКЛЮЗИВНАЯ ветка тика (надзор/D/E не идут —
                    // дисциплина «одно пересоздание за тик»); иначе надзор (C) →
                    // add-node (D) → remove-node (E).
                    await RunClusterOpAsync(cluster, "active", async () =>
                    {
                        // Живая заявка ротации (failover по endpoints: read-only
                        // проверка — первый живой endpoint выигрывает).
                        var rotating = await GetWithFailoverAsync($"/s3worker/rotations/{cluster}", tick);
                        if (rotating.IsSuccess && rotating.Value is not null)
                            return await rotator.TickAsync(snap, tick);

                        // Прогресс-отметки ветки (ILoopProgress): каждая стадия
                        // с молчащей нодой легитимно длится до ~15-22 с (последо-
                        // вательные 7.5-с таймауты граней, без внутренних ретраев —
                        // поллинг-инвариант); без отметок watchdog примет живой
                        // тик за зависание (порог 30 c) и уронит воркер.
                        progress.Mark();
                        var supervised = await supervisor.TickAsync(snap, tick);
                        if (!supervised.IsSuccess)
                            return supervised;
                        progress.Mark();
                        var added = await addNode.TickAsync(snap, tick);
                        if (!added.IsSuccess)
                            return added;
                        progress.Mark();
                        var removed = await removeNode.TickAsync(snap, tick);
                        if (!removed.IsSuccess)
                            return removed;
                        // Доводка состава (arch/24 §5 G) — до F: зонд видит
                        // состав mesh доведённым тика; ошибка G прерывает
                        // ветку до F (ранний return — заявка зонда живёт).
                        progress.Mark();
                        var meshed = await mesh.TickAsync(snap, tick);
                        if (!meshed.IsSuccess)
                            return meshed;
                        // Зонд F — последний в Active-ветке (arch/24 §5):
                        // пишет connectivity/sync (arch/23 §3); долгие фазы
                        // отмечает сам (перед каждым вызовом грани).
                        progress.Mark();
                        return await syncProbe.TickAsync(snap, tick);
                    }, ct, tick);
                    break;

                case S3ClusterKind.Skip:
                    logger.LogWarning("cluster {Cluster}: malformed config — skipping tick", cluster);
                    break;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // остановка host'а посреди тика — не ошибка цикла
        }
        catch (OperationCanceledException oce) when (lease?.IsCancellationRequested == true)
        {
            // Потеря клэйма прервала операцию кластера (гвард arch/17 S5):
            // внятная фаза в журнале; кластер подхватит следующий тик/инстанс.
            logger.LogWarning("cluster {Cluster}: lease lost — tick aborted ({Message})",
                snap.Cluster, oce.Message);
            await WriteLeaseLostAsync(snap.Cluster, "tick");
        }
        catch (Exception ex)
        {
            // Страховка контура кластера: даже исключение вне вызова процесса
            // не должно уходить в Task.WhenAll → StopHost.
            logger.LogError(ex, "cluster {Cluster}: unhandled exception (tick continues)", snap.Cluster);
        }
        finally
        {
            lease?.Dispose();
            gate.Release();
        }
    }

    // Вызов процесса под catch-all: исключение процесса → лог + журнал
    // (phase=crashed, last_error), штатный Result — в обычный лог цикла.
    // host — токен остановки хоста (дискриминатор: OCE по нему = остановка,
    // OCE по ct = потеря клэйма — гварда arch/17 S5).
    internal async Task RunClusterOpAsync(
        string cluster, string op, Func<Task<Result>> call, CancellationToken host, CancellationToken ct)
    {
        try
        {
            var outcome = await call();
            if (outcome.IsSuccess)
                logger.LogInformation("{Op} {Cluster}: ok", op, cluster);
            else
                logger.LogError(outcome.Error, "{Op} {Cluster} failed: {Message}", op, cluster, outcome.Error!.Message);
        }
        catch (OperationCanceledException) when (host.IsCancellationRequested)
        {
            throw; // остановка host'а — пробрасываем (обработана уровнем выше)
        }
        catch (OperationCanceledException)
        {
            await WriteLeaseLostAsync(cluster, op); // потеря клэйма — гвард arch/17 S5
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "process {Op} {Cluster} threw an exception: {Message}", op, cluster, ex.Message);
            try
            {
                // CancellationToken.None: запись должна доехать даже при остановке host'а.
                await journal.WritePhaseAsync(
                    cluster, op, "crashed", claims.InstanceId, ex.Message, CancellationToken.None);
            }
            catch (Exception journalEx)
            {
                logger.LogWarning(journalEx, "work journal for {Cluster} not written after exception", cluster);
            }
        }
    }

    // Фаза lease-lost в журнале воркера (arch/17 S5); CancellationToken.None —
    // токен тика отменён, запись обязана доехать.
    private async Task WriteLeaseLostAsync(string cluster, string op)
    {
        try
        {
            await journal.WritePhaseAsync(
                cluster, op, "lease-lost", claims.InstanceId, null, CancellationToken.None);
        }
        catch (Exception journalEx)
        {
            logger.LogWarning(journalEx, "work journal for {Cluster} not written after lease loss", cluster);
        }
    }

    // Чтение одного ключа с failover: первый успешный endpoint выигрывает.
    private async Task<Result<Kv?>> GetWithFailoverAsync(string key, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in options.CurrentValue.Etcd.Endpoints)
        {
            var result = await etcd.GetAsync(endpoint, key, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not configured");
    }

    private async Task<Result<IReadOnlyList<Kv>>> RangeWithFailoverAsync(
        string[] endpoints, string prefix, CancellationToken ct)
    {
        Result<IReadOnlyList<Kv>> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.RangeAsync(endpoint, prefix, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not configured");
    }
}
