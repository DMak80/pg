namespace AdminPanel.Core;

// Внесение результатов проб в свежий снапшот (arch/02 §4 п.3; spec §4.2): члены HA
// обогащаются REST-полями, шардам ставится Runtime, Probes — последним тиком проб.
// Чистая функция; лишние ключи состояния (цель исчезла из etcd) игнорируются (spec §3.5).
public static class ProbeEnricher
{
    public static EtcdSnapshot Apply(EtcdSnapshot snapshot, ProbeState? state)
    {
        if (state is null)
            return snapshot; // тиков не было — снапшот уже собран с пустыми Probes/Runtime

        var scopes = state.Members.Count == 0
            ? snapshot.HaScopes
            : [.. snapshot.HaScopes.Select(scope => scope with
            {
                Members = [.. scope.Members.Select(member => MergeMember(scope.Scope, member, state))],
            })];

        var clusters = state.Runtimes.Count == 0
            ? snapshot.Clusters
            : [.. snapshot.Clusters.Select(cluster => cluster with
            {
                Shards = [.. cluster.Shards.Select(shard => MergeRuntime(cluster.Name, shard, state))],
            })];

        return snapshot with { HaScopes = scopes, Clusters = clusters, Probes = state.Probes };
    }

    // Успех: REST перекрывает role/state/timeline/lag, ошибка снята; отказ: DCS-часть
    // остаётся, лаги не показываем, фиксируем время и текст ошибки (spec §3.5).
    private static HaMember MergeMember(string scope, HaMember member, ProbeState state)
        => state.Members.TryGetValue($"{scope}/{member.Name}", out var probe)
            ? member with
            {
                Role = probe.Role ?? member.Role,
                State = probe.State ?? member.State,
                Timeline = probe.Timeline,
                LagBytes = probe.LagBytes,
                ProbeAtUtc = probe.AtUtc,
                ProbeError = probe.Error,
            }
            : member;

    private static ShardInfo MergeRuntime(string cluster, ShardInfo shard, ProbeState state)
        => state.Runtimes.TryGetValue($"{cluster}/{shard.Name}", out var runtime)
            ? shard with { Runtime = runtime }
            : shard;
}
