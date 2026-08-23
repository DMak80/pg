using Microsoft.Extensions.Options;
using PgWorker.Core;
using PgWorker.Docker.Engine;
using PgWorker.Etcd.Client;

namespace PgWorker.App.HealthChecks;

/// <summary>
/// Активные пробы внешних зависимостей для /healthz (задача 24; spec §8):
/// etcd-reachable (range по всем endpoints) и docker-hosts (ping каждого хоста
/// plain-таблицы / manager swarm). Пробы с коротким таймаутом — health не висит.
/// </summary>
public sealed class ServiceProbes(
    IEtcdGateway etcd,
    IOptionsMonitor<PgWorkerOptions> options,
    DockerEngineFactory factory)
{
    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(2);

    private readonly object _sync = new();
    private readonly Dictionary<string, IDockerEngine> _engines = []; // кэш клиентов

    /// <summary>etcd жив: хотя бы один endpoint отвечает на range по /pgworker/.</summary>
    public async Task<Result> EtcdReachableAsync(CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(ProbeTimeout);

        Result? last = null;
        foreach (var endpoint in options.CurrentValue.Etcd.Endpoints)
        {
            var range = await etcd.RangeAsync(endpoint, "/pgworker/", timeout.Token);
            if (range.IsSuccess)
                return Result.Success();
            last = Result.Failed(range.Error!);
        }

        return last ?? Result.Failed(new ApplicationException("PgWorker:Etcd:Endpoints не заданы"));
    }

    /// <summary>docker-хосты: ping каждого (plain: таблица Hosts; swarm: manager).</summary>
    public async Task<IReadOnlyDictionary<string, Result>> PingDockerHostsAsync(CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(ProbeTimeout);

        var docker = options.CurrentValue.Docker;
        var targets = string.Equals(docker.Mode, "Swarm", StringComparison.OrdinalIgnoreCase)
            ? docker.SwarmManager is { Length: > 0 } manager
                ? [("swarm-manager", manager)]
                : []
            : docker.Hosts.Select(h => (h.Name, h.Endpoint)).ToArray();

        var results = new Dictionary<string, Result>();
        foreach (var (name, endpoint) in targets)
            results[name] = await PingAsync(name, endpoint, timeout.Token);

        return results;
    }

    private async Task<Result> PingAsync(string name, string endpoint, CancellationToken ct)
    {
        try
        {
            IDockerEngine engine;
            lock (_sync)
            {
                if (!_engines.TryGetValue(name, out engine!))
                {
                    engine = factory.Create(endpoint);
                    _engines[name] = engine;
                }
            }

            return await engine.PingAsync(ct);
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"docker {name}: {e.Message}", e));
        }
    }
}
