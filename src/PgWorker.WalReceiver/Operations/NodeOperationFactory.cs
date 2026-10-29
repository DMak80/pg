namespace PgWorker.WalReceiver.Operations;

/// <summary>Прод-фабрика ops-операций ноды (DI Program): все операции — в
/// ОДНОМ процессе приёмника (arch/19 §5); реестр — ОБЩИЙ singleton
/// ops-операций (одна активная ops-операция на процесс, гвард TryBegin);
/// пути — на data-volume ноды (NodePaths). Тесты подменяют сам интерфейс
/// IOpsOperationFactory фейком — этот класс при подмене не инстанцируется.</summary>
public sealed class NodeOperationFactory(
    string stagingRoot,
    OpsOperationRegistry registry) : IOpsOperationFactory
{
    public IReceiverOperation CreateVerify(StartVerifyRequest request)
        => new VerifyOperation(request, new ProcessRunner(), registry, stagingRoot);

    public IReceiverOperation CreateRestore(StartRestoreRequest request)
        => new RestoreOperation(request, new ProcessRunner(), registry);
}
