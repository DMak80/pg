using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace EtcdWorker.App.HealthChecks;

/// <summary>
/// Агрегированный health EtcdWorker — каркас по образцу S3WorkerHealth:
/// пассивные данные — из HealthState (тики циклов), активные пробы и
/// доменные секции добавят фазы 4–5. Минимальный чек etcdworker: метка
/// инстанса (ClaimStore) и живость keepalive-цикла; нет тика / тише порога →
/// Degraded (сервис жив, но требует внимания), секции отдаются в Data.
/// </summary>
public sealed class EtcdWorkerHealth(
    HealthState health,
    ClaimStore claims,
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

    private Task<HealthCheckResult> CheckCoreAsync(CancellationToken ct)
    {
        var data = new Dictionary<string, object>();
        var degraded = new List<string>();

        // Метка инстанса: кто отвечает за этот healthz (id из ClaimStore +
        // лидерство координации).
        data["instance"] = $"{claims.InstanceId}; leader={claims.IsLeader}";

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

        return Task.FromResult(degraded.Count == 0
            ? HealthCheckResult.Healthy("EtcdWorker is alive", data)
            : HealthCheckResult.Degraded(string.Join("; ", degraded), data: data));
    }

    private string LoopEntry(string name, DateTimeOffset? at)
        => at is null ? $"{name}=no ticks" : $"{name}={(clock.GetUtcNow() - at.Value).TotalSeconds:F0}s ago";
}
