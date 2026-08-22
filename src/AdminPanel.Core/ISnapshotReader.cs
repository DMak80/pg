namespace AdminPanel.Core;

// Доступ к текущему снапшоту для модулей вне Etcd (пробы t06): Probes → Core, не → Etcd
// (направление зависимостей arch/01 §1; spec §4.3). Реализует Etcd-стор (ISnapshotStore).
public interface ISnapshotReader
{
    EtcdSnapshot? Current { get; }
}
