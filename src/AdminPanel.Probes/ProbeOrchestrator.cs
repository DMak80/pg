using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Probes;

// Фоновый тик проб (arch/02 §4 «отдельный тик Probes.Interval»): цели из текущего
// снапшота, все пробы параллельно, состояние — в IProbeStateStore (spec §4.8).
// Пробы не блокируют тик KV refresher'а — тот берёт состояние готовым (§3.1).
[InjectAsSingleton(typeof(IHostedService))]
public sealed class ProbeOrchestrator(
    ISnapshotReader snapshotReader,
    IProbeStateStore stateStore,
    IPatroniRestProbe patroniProbe,
    ISqlProbe sqlProbe,
    IOptions<ProbesOptions> options,
    TimeProvider time,
    ILogger<ProbeOrchestrator> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var value = options.Value;
        if (!value.PatroniEnabled && !value.SqlEnabled)
        {
            // Обе пробы выключены — цикл не нужен (spec §3.15); hosted-регистрация остаётся.
            logger.LogInformation("AdminPanel:Probes: обе пробы выключены — тик проб не запускается");
            return;
        }

        var seconds = value.IntervalSeconds;
        if (seconds <= 0)
        {
            logger.LogWarning("AdminPanel:Probes:IntervalSeconds <= 0 — использую 15 c");
            seconds = 15;
        }

        // Первый тик сразу (прецедент t03 §7.2), далее по периоду.
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(seconds));
        do
        {
            await RunOnceAsync(stoppingToken);
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    // Ядро одного тика — публично для unit/integration-тестов без хоста (прецедент RefreshOnceAsync).
    public async Task RunOnceAsync(CancellationToken ct)
    {
        var value = options.Value;
        var at = time.GetUtcNow();
        var snapshot = snapshotReader.Current;
        var members = new Dictionary<string, HaMemberProbe>();
        var runtimes = new Dictionary<string, ShardRuntime>();
        var results = new List<ProbeResult>();
        var tasks = new List<Task>();

        // Цели — matched-скопы и шарды с DSN (spec §3.3); обе пробы — параллельно (§3.15).
        if (value.PatroniEnabled && snapshot is not null)
        {
            foreach (var scope in snapshot.HaScopes.Where(s => s.Matched))
            foreach (var member in scope.Members)
            {
                var key = $"{scope.Scope}/{member.Name}";
                tasks.Add(Patroni(scope, member, key, at, members, results, ct));
            }
        }

        if (value.SqlEnabled && snapshot is not null)
        {
            foreach (var cluster in snapshot.Clusters)
            foreach (var shard in cluster.Shards.Where(s => s.DsnHosts.Count > 0))
            {
                tasks.Add(Sql(cluster, shard, at, runtimes, results, ct));
            }
        }

        await Task.WhenAll(tasks);

        // Одна атомарная замена состояния; порядок проб стабилен (kind, затем target).
        stateStore.Replace(new ProbeState(
            at,
            [.. results.OrderBy(r => r.Kind, StringComparer.Ordinal).ThenBy(r => r.Target, StringComparer.Ordinal)],
            members,
            runtimes));
        return;

        // Локальная обёртка Patroni-цели: реализация пробы не бросает, но контракт
        // не гарантирует — ошибка цели ловится в failed-результат (spec §3.15).
        async Task Patroni(
            HaScope scope, HaMember member, string key, DateTimeOffset atUtc,
            Dictionary<string, HaMemberProbe> sink, List<ProbeResult> probeSink, CancellationToken token)
        {
            PatroniMemberResult result;
            try
            {
                result = await patroniProbe.ProbeAsync(scope, member, token);
            }
            catch (Exception e)
            {
                result = new PatroniMemberResult(
                    new HaMemberProbe(null, null, null, null, atUtc, e.Message),
                    new ProbeResult(key, "patroni", false, null, e.Message, atUtc));
            }

            lock (sink)
            {
                sink[key] = result.Enrichment;
            }

            lock (probeSink)
            {
                probeSink.Add(result.Result);
            }
        }

        // Локальная обёртка SQL-цели — та же защита от броска реализации.
        async Task Sql(
            ClusterInfo cluster, ShardInfo shard, DateTimeOffset atUtc,
            Dictionary<string, ShardRuntime> sink, List<ProbeResult> probeSink, CancellationToken token)
        {
            SqlShardResult result;
            try
            {
                result = await sqlProbe.ProbeAsync(cluster, shard, token);
            }
            catch (Exception e)
            {
                result = new SqlShardResult(
                    new ShardRuntime(shard.Name, [], [], [], [], null, e.Message),
                    new ProbeResult($"{cluster.Name}/{shard.Name}", "sql", false, null, e.Message, atUtc));
            }

            lock (sink)
            {
                sink[$"{cluster.Name}/{shard.Name}"] = result.Runtime;
            }

            lock (probeSink)
            {
                probeSink.Add(result.Result);
            }
        }
    }
}
