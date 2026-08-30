using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Coordination;

namespace KafkaWorker.App.HealthChecks;

/// <summary>
/// Агрегированный health PgWorker (задача 24; spec §8): секции etcd-reachable,
/// docker-hosts, loops-alive, claims, snapshot-freshness. Пассивные данные — из
/// HealthState (тики циклов), активные — пробы ServiceProbes. Недоступный etcd /
/// docker-хост / зависший цикл / протухший снапшот → Degraded (сервис жив, но
/// требует внимания); секции отдаются в Data для оператора.
/// </summary>
public sealed class KafkaWorkerHealth(
    ServiceProbes probes,
    HealthState health,
    ClaimStore claims,
    IOptionsMonitor<KafkaWorkerOptions> options,
    TimeProvider clock) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        var data = new Dictionary<string, object>();
        var degraded = new List<string>();

        // etcd-reachable: активная проба (последний Range-тик — в loops-alive).
        var etcd = await probes.EtcdReachableAsync(ct);
        data["etcd"] = etcd.IsSuccess ? "reachable" : etcd.Error!.Message;
        if (!etcd.IsSuccess)
            degraded.Add("etcd недоступен");

        // docker-hosts: ping каждого хоста конфигурации.
        var hosts = await probes.PingDockerHostsAsync(ct);
        data["docker-hosts"] = hosts.Count == 0
            ? "нет настроенных хостов"
            : string.Join("; ", hosts.Select(p => $"{p.Key}={(p.Value.IsSuccess ? "ok" : p.Value.Error!.Message)}"));
        foreach (var failed in hosts.Where(p => !p.Value.IsSuccess))
            degraded.Add($"docker-хост {failed.Key} недоступен");

        // loops-alive: возраст последнего тика каждого цикла (пассивно, HealthState).
        var loops = options.CurrentValue.Loops;
        var staleAfter = TimeSpan.FromSeconds(3 * Math.Max(loops.ScanIntervalSec, loops.KeepaliveSec) + 15);
        // snapshot-цикл между тиками спит SnapshotIntervalMin (лидер) — порог
        // свежести у него свой, а не как у scan-циклов.
        var snapshotStaleAfter = TimeSpan.FromSeconds(
            3 * Math.Max(loops.ScanIntervalSec, 60 * loops.SnapshotIntervalMin) + 15);
        var snapshot = health.Snapshot();
        data["loops"] = string.Join("; ", new[]
        {
            LoopEntry("reconcile", snapshot.LastReconcileTick),
            LoopEntry("keepalive", snapshot.LastKeepaliveTick),
            LoopEntry("snapshot", snapshot.LastSnapshotTick),
        });
        foreach (var (name, at, threshold) in new[]
                 {
                     ("reconcile", snapshot.LastReconcileTick, staleAfter),
                     ("keepalive", snapshot.LastKeepaliveTick, staleAfter),
                     ("snapshot", snapshot.LastSnapshotTick, snapshotStaleAfter),
                 })
        {
            if (at is null)
                degraded.Add($"цикл {name} ещё не тикал");
            else if (clock.GetUtcNow() - at.Value > threshold)
                degraded.Add($"цикл {name} не тикал {(clock.GetUtcNow() - at.Value).TotalSeconds:F0} с");
        }

        // claims: сколько клэймов удерживает инстанс + лидерство снапшотов (Д2).
        data["claims"] = $"held={snapshot.ClaimsHeld}; leader={claims.IsLeader}; instance={claims.InstanceId}";

        // snapshot-freshness: возраст последнего снапшота (только лидер снимает).
        if (snapshot.LastSnapshotTaken is { } taken)
        {
            var age = clock.GetUtcNow() - taken;
            data["snapshot"] = $"возраст {age.TotalMinutes:F0} мин";
            var limit = TimeSpan.FromMinutes(options.CurrentValue.Loops.SnapshotIntervalMin + 10);
            if (claims.IsLeader && age > limit)
                degraded.Add($"снапшот старше лимита ({age.TotalMinutes:F0} мин)");
        }
        else
        {
            data["snapshot"] = claims.IsLeader ? "ещё не снят (лидер)" : "снимает другой инстанс (лидер)";
        }

        return degraded.Count == 0
            ? HealthCheckResult.Healthy("KafkaWorker жив", data)
            : HealthCheckResult.Degraded(string.Join("; ", degraded), data: data);
    }

    private string LoopEntry(string name, DateTimeOffset? at)
        => at is null ? $"{name}=нет тиков" : $"{name}={(clock.GetUtcNow() - at.Value).TotalSeconds:F0}с назад";
}
