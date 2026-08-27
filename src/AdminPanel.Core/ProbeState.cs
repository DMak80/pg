namespace AdminPanel.Core;

// Результат Patroni-пробы одного члена: обогащение HaMember + статус попытки (spec §4.1).
public sealed record HaMemberProbe(
    string? Role,
    string? State,
    long? Timeline,
    long? LagBytes,
    DateTimeOffset AtUtc,
    string? Error);

// Состояние одного тика проб (arch/02 §4): пишет ProbeOrchestrator, читает SnapshotRefresher.
public sealed record ProbeState(
    DateTimeOffset AtUtc,
    IReadOnlyList<ProbeResult> Probes,                    // все попытки тика, ok и error
    IReadOnlyDictionary<string, HaMemberProbe> Members,   // ключ "<scope>/<member>"
    IReadOnlyDictionary<string, ShardRuntime> Runtimes); // ключ "<cluster>/<shard>"

// Стор состояния проб: атомарная замена ссылки — зеркалит ISnapshotStore (spec §4.9).
public interface IProbeStateStore
{
    ProbeState? Current { get; }

    void Replace(ProbeState state);
}
