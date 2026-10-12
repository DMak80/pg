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
// Процессы подключаются задачами 10–13 (A — готово).

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
    SecretRotator rotator,
    Meter meter,
    ILogger<S3ClusterProcesses> logger) : IS3ClusterProcesses
{
    // Счётчик unknownKeys (spec §9): домен развивается — читатель не падает
    // (arch/23 §2); инкремент — размер списка за тик.
    private readonly Counter<long> _unknownKeys = meter.CreateCounter<long>("s3worker_unknown_keys_total");

    public async Task<int> TickAsync(CancellationToken ct)
    {
        var endpoints = options.CurrentValue.Etcd.Endpoints.ToArray();
        if (endpoints.Length == 0)
            throw new ApplicationException("S3Worker:Etcd:Endpoints не заданы");

        // Снапшот контроль-плейна: Range /s3/ c failover.
        var clustersKvs = await RangeWithFailoverAsync(endpoints, "/s3/", ct);
        if (!clustersKvs.IsSuccess)
            throw clustersKvs.Error!;

        var parsed = S3SnapshotParser.Parse(clustersKvs.Value);
        foreach (var cluster in parsed.Value.Clusters)
        {
            if (cluster.ParseError is not null)
                logger.LogWarning("пропущен битый ключ: {Error}", cluster.ParseError);
            foreach (var node in cluster.Nodes.Where(n => n.ParseError is not null))
                logger.LogWarning("пропущен битый ключ ноды: {Error}", node.ParseError);
        }

        foreach (var unknown in parsed.Value.UnknownKeys)
            logger.LogWarning("неизвестный ключ в /s3/: {Key}", unknown);
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
        try
        {
            var cluster = snap.Cluster;
            var kind = S3ClusterClassifier.Classify(snap);

            // Клэйм ДО процесса (S5, arch/17): exclusivity; занят другим — не ошибка.
            var claimed = await claims.TryClaimClusterAsync(cluster, ct);
            if (!claimed.IsSuccess)
            {
                logger.LogError(claimed.Error, "клэйм {Cluster} не удался: {Message}", cluster, claimed.Error!.Message);
                return;
            }

            if (!claimed.Value)
                return; // обрабатывает другой инстанс

            switch (kind)
            {
                case S3ClusterKind.Provision:
                    await RunClusterOpAsync(cluster, "provision",
                        () => provision.RunAsync(snap, ct), ct);
                    break;

                case S3ClusterKind.Deprovision:
                    await RunClusterOpAsync(cluster, "deprovision",
                        () => deprovision.TickAsync(snap, ct), ct);
                    break;

                case S3ClusterKind.Active:
                    // Active-ветка (arch/24 §5): живая заявка ротации —
                    // ЭКСКЛЮЗИВНАЯ ветка тика (надзор/D/E не идут — дисциплина
                    // «одно пересоздание за тик»); иначе надзор (C) →
                    // add-node (D) → remove-node (E).
                    await RunClusterOpAsync(cluster, "active", async () =>
                    {
                        // Живая заявка ротации (любой endpoint: read-only проверка).
                        var rotating = await etcd.GetAsync(
                            options.CurrentValue.Etcd.Endpoints[0],
                            $"/s3worker/rotations/{cluster}", ct);
                        if (rotating.IsSuccess && rotating.Value is not null)
                            return await rotator.TickAsync(snap, ct);

                        var supervised = await supervisor.TickAsync(snap, ct);
                        if (!supervised.IsSuccess)
                            return supervised;
                        var added = await addNode.TickAsync(snap, ct);
                        if (!added.IsSuccess)
                            return added;
                        return await removeNode.TickAsync(snap, ct);
                    }, ct);
                    break;

                case S3ClusterKind.Skip:
                    logger.LogWarning("кластер {Cluster}: битый config — пропуск тика", cluster);
                    break;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // остановка host'а посреди тика — не ошибка цикла
        }
        catch (Exception ex)
        {
            // Страховка контура кластера: даже исключение вне вызова процесса
            // не должно уходить в Task.WhenAll → StopHost.
            logger.LogError(ex, "кластер {Cluster}: необработанное исключение (тик продолжается)", snap.Cluster);
        }
        finally
        {
            gate.Release();
        }
    }

    // Вызов процесса под catch-all: исключение процесса → лог + журнал
    // (phase=crashed, last_error), штатный Result — в обычный лог цикла.
    // (Задействуется процессами Tasks 10–13.)
    internal async Task RunClusterOpAsync(
        string cluster, string op, Func<Task<Result>> call, CancellationToken ct)
    {
        try
        {
            var outcome = await call();
            if (outcome.IsSuccess)
                logger.LogInformation("{Op} {Cluster}: ok", op, cluster);
            else
                logger.LogError(outcome.Error, "{Op} {Cluster} не прошёл: {Message}", op, cluster, outcome.Error!.Message);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // остановка host'а — пробрасываем (обработана уровнем выше)
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "процесс {Op} {Cluster} бросил исключение: {Message}", op, cluster, ex.Message);
            try
            {
                // CancellationToken.None: запись должна доехать даже при остановке host'а.
                await journal.WritePhaseAsync(
                    cluster, op, "crashed", claims.InstanceId, ex.Message, CancellationToken.None);
            }
            catch (Exception journalEx)
            {
                logger.LogWarning(journalEx, "журнал работы {Cluster} не записан после исключения", cluster);
            }
        }
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

        return last.Error ?? new EtcdUnreachableException("etcd endpoints не заданы");
    }
}
