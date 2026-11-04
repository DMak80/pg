using PgWorker.Etcd.S3;
using Shared.Etcd.Client;

namespace PgWorker.PgNode.Operations;

/// <summary>Прод-фабрика операции полного (DI Program нодового режима): все
/// внешние зависимости — процесса приёмника (etcd/пулы/проба роли). Staging
/// и child-процессов у операции больше нет. Тесты подменяют сам интерфейс
/// IFullBackupOperationFactory фейком — этот класс при подмене не
/// инстанцируется.</summary>
public sealed class FullBackupOperationFactory(
    PgNodeIdentity identity,
    IEtcdGateway etcd,
    string[] etcdEndpoints,
    IS3PoolStore pools,
    Func<CancellationToken, Task<WalReceiverRole>> roleProbe,
    OperationRegistry registry) : IFullBackupOperationFactory
{
    public IReceiverOperation Create(string id)
        => new FullBackupOperation(
            id, identity, etcd, etcdEndpoints, pools, roleProbe, registry);
}
