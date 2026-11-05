namespace PgWorker.Core.Model;

// Слепок префикса /clusters/ из опросного кэша (arch/synchronization/05-delta-application.md §S10): ReconcileLoop
// держит EtcdPrefixPollCache префикса, читатели тика (ClusterSecretEnsurer и
// др.) берут ключи отсюда — БЕЗ etcd-GET на каждый тик. Значение — пара
// (значение, mod_revision); отсутствующий ключ = пара отсутствует в словаре.
public interface IClustersCacheSnapshot
{
    /// <summary>Потокобезопасный слепок: ключ etcd → (значение, mod_revision).</summary>
    IReadOnlyDictionary<string, (string Value, long ModRevision)> Snapshot { get; }
}

// Простой неизменяемый слепок из словаря (тесты/инструменты; прод —
// PgWorker.App.EtcdPrefixSources поверх EtcdPrefixPollCache).
public sealed class ClustersCacheSnapshot(
    IReadOnlyDictionary<string, (string Value, long ModRevision)> snapshot) : IClustersCacheSnapshot
{
    public static readonly ClustersCacheSnapshot Empty = new(
        new Dictionary<string, (string Value, long ModRevision)>());

    public IReadOnlyDictionary<string, (string Value, long ModRevision)> Snapshot { get; } = snapshot;
}
