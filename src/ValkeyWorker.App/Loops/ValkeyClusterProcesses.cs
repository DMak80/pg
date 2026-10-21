using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Shared.Core;
using Shared.Etcd.Client;
using ValkeyWorker.App;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.Provisioning.Processes;

namespace ValkeyWorker.App.Loops;

// Агрегатор процессов для ReconcileLoop (порт ClusterProcesses PgWorker/KafkaWorker):
// цикл не знает конкретных машин состояний — только эту грань (мокабельно в
// unit-тестах цикла). Валю-туннель Active-ветки: надзор (C) → converge (D) →
// ротация (E) — без kafka-специфики (нет reassign/topic-sync/security/backoff).

/// <summary>Один reconcile-тик над всеми кластерами (процессы arch/21 §5).</summary>
public interface IValkeyClusterProcesses
{
    /// <summary>Число клэймов, держимых этим инстансом (health-секция claims).</summary>
    Task<int> TickAsync(CancellationToken ct);
}

/// <summary>
/// Реализация поверх процессов (синглтоны DI): Range /valkey/clusters/ →
/// парсер → клэйм → классификация → процесс. Ошибки кластера не роняют тик;
/// параллелизм — SemaphoreSlim MaxClusters (внутри кластера — последовательно).
/// </summary>
internal sealed class ValkeyClusterProcesses(
    IEtcdGateway etcd,
    IOptionsMonitor<ValkeyWorkerOptions> options,
    ClaimStore claims,
    WorkJournal journal,
    ProvisioningProcess provision,
    DeprovisioningProcess deprovision,
    NodeSupervisor supervisor,
    ConfigConverger converger,
    PasswordRotator rotator,
    TlsMigrator tlsMigrator,
    CaRotator caRotator,
    ILogger<ValkeyClusterProcesses> logger) : IValkeyClusterProcesses
{
    public async Task<int> TickAsync(CancellationToken ct)
    {
        var endpoints = options.CurrentValue.Etcd.Endpoints.ToArray();
        if (endpoints.Length == 0)
            throw new ApplicationException("ValkeyWorker:Etcd:Endpoints are not set");

        // Снапшот контроль-плейна: Range /valkey/clusters/ c failover.
        var clustersKvs = await RangeWithFailoverAsync(endpoints, "/valkey/clusters/", ct);
        if (!clustersKvs.IsSuccess)
            throw clustersKvs.Error!;

        var parsed = ValkeySnapshotParser.Parse(clustersKvs.Value);
        foreach (var cluster in parsed.Value.Clusters)
        foreach (var error in cluster.ParseErrors)
            logger.LogWarning("skipped malformed key: {Error}", error);
        foreach (var unknown in parsed.Value.UnknownKeys)
            logger.LogWarning("unknown key in /valkey/: {Key}", unknown);

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

        // Фактическое число СВОИХ клэймов (health-секция claims, spec §4.8).
        return parsed.Value.Clusters.Count(c => claims.IsMine(c.Cluster));
    }

    // Обработка одного кластера под семафором: клэйм → классификация → процесс.
    // Исключение любого кластера не роняет тик: catch-all → лог + journal.
    private async Task ProcessClusterAsync(ValkeyClusterSnapshot snap, SemaphoreSlim gate, CancellationToken ct)
    {
        await gate.WaitAsync(ct);
        CancellationTokenSource? lease = null; // linked-токен клэйма — виден catch'ам
        try
        {
            var cluster = snap.Cluster;
            var kind = ValkeyClusterClassifier.Classify(snap);

            // Клэйм ДО процесса (Д2): exclusivity; занят другим — не ошибка.
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
                case ValkeyClusterKind.Provision:
                    await RunClusterOpAsync(cluster, "provision",
                        () => provision.TickAsync(snap, tick), ct, tick);
                    break;

                case ValkeyClusterKind.Deprovision:
                    await RunClusterOpAsync(cluster, "deprovision",
                        () => deprovision.TickAsync(snap, tick), ct, tick);
                    break;

                case ValkeyClusterKind.Active:
                    // t06: миграция TLS — ПЕРВЫМ шагом Active-ветки (arch/21 §5):
                    // InProgress ⇒ надзор/converge/ротация в этом тике не идут
                    // (миграция доиграет тиками; узкое окно plain→TLS).
                    await RunClusterOpAsync(cluster, "active", async () =>
                    {
                        var migration = await tlsMigrator.RunAsync(snap, tick);
                        if (!migration.IsSuccess)
                            return migration.Error!;
                        if (migration.Value == TlsMigrator.MigrationOutcome.InProgress)
                            return Result.Success();

                        // t07: ротация CA — ВТОРОЙ шаг Active-ветки (arch/21 §5 K):
                        // окно открыто (InProgress) ⇒ надзор/converge/ротация в
                        // этом тике не идут (узкое окно двойного доверия);
                        // Waiting/NotNeeded — ветка продолжается (ждущие исходы
                        // ничего не мутировали; E доиграет ниже по ветке).
                        var rotation = await caRotator.RunAsync(snap, tick);
                        if (!rotation.IsSuccess)
                            return rotation.Error!;
                        if (rotation.Value == CaRotator.RotationOutcome.InProgress)
                            return Result.Success();

                        // Валю-туннель Active-ветки (arch/21 §5): надзор (C) →
                        // converge (D) → ротация (E). Конвергер требует
                        // endpoints+admin-кред — Active без дискавери пропускает D.
                        var supervised = await supervisor.TickAsync(snap, tick);
                        if (!supervised.IsSuccess)
                            return supervised;
                        if (snap.Endpoints is not null && snap.AdminPassword is not null)
                        {
                            var converged = await converger.TickAsync(snap, tick);
                            if (!converged.IsSuccess)
                                return converged;
                        }

                        return await rotator.TickAsync(snap, tick);
                    }, ct, tick);
                    break;

                case ValkeyClusterKind.Skip:
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
    private async Task RunClusterOpAsync(
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
                logger.LogWarning(journalEx, "work journal {Cluster} not written after exception", cluster);
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
            logger.LogWarning(journalEx, "work journal {Cluster} not written after lease loss", cluster);
        }
    }

    // Failover: первый успешный endpoint выигрывает (общий хелпер).
    private Task<Result<IReadOnlyList<Kv>>> RangeWithFailoverAsync(
        string[] endpoints, string prefix, CancellationToken ct)
        => EtcdEndpointFailover.CallAsync(endpoints, (endpoint, token) => etcd.RangeAsync(endpoint, prefix, token), ct);
}
