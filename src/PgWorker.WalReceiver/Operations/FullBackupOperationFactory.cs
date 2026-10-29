using PgWorker.Etcd.S3;
using Shared.Etcd.Client;

namespace PgWorker.WalReceiver.Operations;

/// <summary>Прод-фабрика операции полного (DI Program нодового режима): все
/// внешние зависимости — процесса приёмника (etcd/пулы/проба роли/runner/
/// staging из env). Тесты подменяют сам интерфейс IFullBackupOperationFactory
/// фейком (шаг 9) — этот класс при подмене не инстанцируется.</summary>
public sealed class FullBackupOperationFactory(
    WalReceiverIdentity identity,
    IEtcdGateway etcd,
    string[] etcdEndpoints,
    IS3PoolStore pools,
    Func<CancellationToken, Task<WalReceiverRole>> roleProbe,
    IProcessRunner runner,
    string stagingDir,
    OperationRegistry registry) : IFullBackupOperationFactory
{
    public IReceiverOperation Create(string id)
        => new FullBackupOperation(
            id, stagingDir, identity, etcd, etcdEndpoints, pools, roleProbe, runner, registry);
}
