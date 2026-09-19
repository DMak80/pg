using AdminPanel.Core.Valkey;

namespace AdminPanel.Etcd;

// Читатели valkey-домена (инспекция API, проба) — без блокировок.
public interface IValkeySnapshotReader
{
    // До первого тика снапшота нет — потребители показывают «загрузку».
    ValkeySnapshot? Current { get; }
}

// Хранилище текущего снапшота valkey (порт KafkaSnapshotStore): писатель один —
// ValkeySnapshotRefresher; атомарная замена volatile-ссылки.
public interface IValkeySnapshotStore : IValkeySnapshotReader
{
    new ValkeySnapshot? Current { get; }

    void Replace(ValkeySnapshot snapshot);
}

// Регистрация — явно в ModuleExtensions.AddValkey() (симметрия AddKafka).
public sealed class ValkeySnapshotStore : IValkeySnapshotStore
{
    private volatile ValkeySnapshot? _current;

    public ValkeySnapshot? Current => _current;

    public void Replace(ValkeySnapshot snapshot) => _current = snapshot;
}
