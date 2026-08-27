using AdminPanel.Core;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Etcd;

// Хранилище текущего снапшота: читатели никогда не блокируются (arch/01 §1).
public interface ISnapshotStore : ISnapshotReader
{
    // До первого тика снапшота нет — потребители (t04) показывают «загрузка» (spec §3.13).
    // new: контракт уже задан ISnapshotReader, переобъявление — самодокументирование.
    new EtcdSnapshot? Current { get; }

    // Атомарная замена ссылки; писатель один — SnapshotRefresher (arch/01 §1).
    void Replace(EtcdSnapshot snapshot);
}

[InjectAsSingleton(typeof(ISnapshotStore), typeof(ISnapshotReader))]
public sealed class SnapshotStore : ISnapshotStore
{
    private volatile EtcdSnapshot? _current;

    public EtcdSnapshot? Current => _current;

    public void Replace(EtcdSnapshot snapshot) => _current = snapshot;
}
