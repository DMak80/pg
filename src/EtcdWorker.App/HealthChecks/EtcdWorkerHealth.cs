using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace EtcdWorker.App.HealthChecks;

/// <summary>
/// Агрегированный health EtcdWorker (arch/25 §1.1/§7): секции
/// etcd-reachable, docker-hosts, loops-alive (keepalive), snapshot-export.
/// Пассивные данные — из HealthState (тики циклов), активные пробы —
/// ServiceProbes. Недоступный etcd / docker-хост / зависший цикл → Degraded
/// (сервис жив, но требует внимания); snapshot-export — информационная секция
/// (алерт панели). Секции отдаются в Data.
/// </summary>
public sealed class EtcdWorkerHealth(
    ServiceProbes probes,
    HealthState health,
    ClaimStore claims,
    EtcdWorker.Provisioning.SnapshotExporter exporter,
    IOptionsMonitor<EtcdWorkerOptions> options,
    TimeProvider clock) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        try
        {
            return await CheckCoreAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Catch-all: чек ВСЕГДА отдаёт структуру — неожиданный отказ тела
            // превращается в Degraded с данными, а не исключение чека.
            return HealthCheckResult.Degraded(
                $"health check failed with an error: {ex.Message}",
                data: new Dictionary<string, object> { ["error"] = ex.Message });
        }
    }

    private async Task<HealthCheckResult> CheckCoreAsync(CancellationToken ct)
    {
        var data = new Dictionary<string, object>();
        var degraded = new List<string>();

        // Метка инстанса: кто отвечает за этот healthz (id из ClaimStore +
        // лидерство координации).
        data["instance"] = $"{claims.InstanceId}; leader={claims.IsLeader}";

        // etcd-reachable: активная проба (range по endpoints, ServiceProbes).
        var etcd = await probes.EtcdReachableAsync(ct);
        data["etcd"] = etcd.IsSuccess ? "reachable" : etcd.Error!.Message;
        if (!etcd.IsSuccess)
            degraded.Add("etcd unreachable");

        // docker-hosts: ping каждого хоста конфигурации (Plain — таблица
        // Hosts; Swarm — manager).
        var hosts = await probes.PingDockerHostsAsync(ct);
        data["docker-hosts"] = hosts.Count == 0
            ? "no configured hosts"
            : string.Join("; ", hosts.Select(p => $"{p.Key}={(p.Value.IsSuccess ? "ok" : p.Value.Error!.Message)}"));
        foreach (var failed in hosts.Where(p => !p.Value.IsSuccess))
            degraded.Add($"docker host {failed.Key} unreachable");

        // loops-alive: возраст последнего keepalive-тика (пассивно, HealthState);
        // порог — общий хелпер LoopStaleness (keepalive — быстрый цикл).
        var loops = options.CurrentValue.Loops;
        var staleAfter = Shared.Core.HealthChecks.LoopStaleness.FastLoops(loops.ScanIntervalSec, loops.KeepaliveSec);
        var snapshot = health.Snapshot();
        data["keepalive"] = LoopEntry("keepalive", snapshot.LastKeepaliveTick);
        if (snapshot.LastKeepaliveTick is null)
            degraded.Add("loop keepalive has not ticked yet");
        else if (clock.GetUtcNow() - snapshot.LastKeepaliveTick.Value > staleAfter)
            degraded.Add(
                $"loop keepalive has not ticked for {(clock.GetUtcNow() - snapshot.LastKeepaliveTick.Value).TotalSeconds:F0} s");

        // snapshot-export (arch/25 §1.1/§5): факт последней выгрузки DR-слепка
        // в S3 — секция ИНФОРМАЦИОННАЯ (OK/FAILED/возраст): FAILED-выгрузка не
        // деградирует health (алерт etcd-snapshot-export-failed — панель;
        // установка может жить без etcd-backup S3-кластера — R6, §7.3-стенд);
        // «ключа ещё нет» — стартовое окно первого слепка.
        var export = await exporter.ReadStatusAsync(ct);
        if (export.IsSuccess && export.Value is { } exportStatus)
        {
            data["snapshot-export"] =
                $"{exportStatus.State}; last_uploaded_unix={exportStatus.LastUploadedUnix}; object={exportStatus.LastObject}";
        }
        else
        {
            data["snapshot-export"] = "no status key yet";
        }

        return degraded.Count == 0
            ? HealthCheckResult.Healthy("EtcdWorker is alive", data)
            : HealthCheckResult.Degraded(string.Join("; ", degraded), data: data);
    }

    private string LoopEntry(string name, DateTimeOffset? at)
        => at is null ? $"{name}=no ticks" : $"{name}={(clock.GetUtcNow() - at.Value).TotalSeconds:F0}s ago";
}
