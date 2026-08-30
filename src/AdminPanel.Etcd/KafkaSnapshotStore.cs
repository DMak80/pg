using AdminPanel.Core.Kafka;

namespace AdminPanel.Etcd;

// Читатели kafka-домена (инспекция API, проба B6) — без блокировок.
public interface IKafkaSnapshotReader
{
    // До первого тика снапшота нет — потребители показывают «загрузка».
    KafkaSnapshot? Current { get; }
}

// Хранилище текущего снапшота kafka (порт SnapshotStore): писатель один —
// KafkaSnapshotRefresher; атомарная замена volatile-ссылки.
public interface IKafkaSnapshotStore : IKafkaSnapshotReader
{
    new KafkaSnapshot? Current { get; }

    void Replace(KafkaSnapshot snapshot);
}

// Регистрация — явно в ModuleExtensions.AddKafka() (не attribute-DI: единственная
// точка композиции kafka-домена, симметрия AddEtcd).
public sealed class KafkaSnapshotStore : IKafkaSnapshotStore
{
    private volatile KafkaSnapshot? _current;

    public KafkaSnapshot? Current => _current;

    public void Replace(KafkaSnapshot snapshot) => _current = snapshot;
}
