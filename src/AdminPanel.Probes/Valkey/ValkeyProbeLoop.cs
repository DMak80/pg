using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using AdminPanel.Etcd;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Probes.Valkey;

// Стор результатов valkey-проб: писатель один — ValkeyProbeLoop; читают
// ValkeySnapshotRefresher (мердж Live/ProbeError) и инспекция.
public interface IValkeyProbeStore
{
    IReadOnlyList<ValkeyProbeResult>? Current { get; }

    void Replace(IReadOnlyList<ValkeyProbeResult> results);
}

public sealed class ValkeyProbeStore : IValkeyProbeStore
{
    private volatile IReadOnlyList<ValkeyProbeResult>? _current;

    public IReadOnlyList<ValkeyProbeResult>? Current => _current;

    public void Replace(IReadOnlyList<ValkeyProbeResult> results) => _current = results;
}

// Фоновый тик valkey-проб (spec §4.6): для каждого Active-кластера с endpoints
// и полным набором admin-кредов — PING (AUTH admin). Ошибка/нет кредов →
// Live=false+Error / кластер без пробы. Пробы не блокируют KV-тик (переносит
// успешный тик refresher'а — симметрия kafka). Креды в результаты не попадают.
public sealed class ValkeyProbeLoop(
    IValkeySnapshotReader snapshotReader,
    IValkeySecretsStore secrets,
    IValkeyProbeClient client,
    IValkeyProbeStore store,
    IOptions<ProbesOptions> probesOptions,
    TimeProvider time,
    ILogger<ValkeyProbeLoop> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var valkey = probesOptions.Value.Valkey;
        if (!valkey.Enabled)
        {
            logger.LogInformation("AdminPanel:Probes:Valkey: проба выключена — тик не запускается");
            return;
        }

        var seconds = valkey.IntervalSec;
        if (seconds <= 0)
        {
            logger.LogWarning("AdminPanel:Probes:Valkey:IntervalSec <= 0 — использую 15 c");
            seconds = 15;
        }

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(seconds));
        do
        {
            try
            {
                await RunOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    // Ядро тика — публично для unit-тестов без хоста.
    public async Task RunOnceAsync(CancellationToken ct)
    {
        var at = time.GetUtcNow();
        var snapshot = snapshotReader.Current;
        if (snapshot is null)
            return; // valkey-снапшота ещё нет — пробать нечего

        var timeout = TimeSpan.FromSeconds(
            probesOptions.Value.Valkey.TimeoutSec > 0 ? probesOptions.Value.Valkey.TimeoutSec : 3);
        var hostMap = probesOptions.Value.HostMap;
        var results = new List<ValkeyProbeResult>();

        foreach (var cluster in snapshot.Clusters.Where(c =>
                     c.State == ValkeyClusterState.Active && !string.IsNullOrEmpty(c.Endpoints)))
        {
            // nodes=1: единственный адрес endpoints (spec §4.4); HostMapResolver —
            // порядок arch/02 §6 (адрес из etcd → override HostMap → прямое).
            var address = cluster.Endpoints!.Split(',')[0].Trim();
            var parts = address.Split(':');
            if (parts.Length != 2 || !int.TryParse(parts[1], out var port))
                continue;

            var resolved = HostMapResolver.Resolve(hostMap, parts[0], port);

            if (!secrets.Current.TryGetValue(cluster.Name, out var creds))
                continue; // неполные креды — кластер без пробы (live=null в DTO)

            var target = new ValkeyProbeTarget(
                resolved[..resolved.LastIndexOf(':')],
                int.Parse(resolved[(resolved.LastIndexOf(':') + 1)..]),
                creds.AdminUser,
                creds.AdminPassword,
                creds.CaPem); // TLS-доверие пробы (t06, arch/02 §11.1)
            var probe = await client.PingAsync(target, timeout, ct);
            var node = cluster.NodesList.FirstOrDefault()?.Name ?? "node1";
            results.Add(new ValkeyProbeResult(
                cluster.Name, node, probe.IsSuccess,
                at.ToUnixTimeSeconds(),
                probe.IsSuccess ? null : probe.Error!.Message));
        }

        results.Sort((a, b) => string.CompareOrdinal(a.Cluster, b.Cluster));
        store.Replace(results);
    }
}
