namespace PgWorker.PgNode.Operations;

/// <summary>Прод-фабрика ops-операций ноды (DI Program): все операции — в
/// ОДНОМ процессе приёмника (arch/backups/06-s3-layout.md); реестр — ОБЩИЙ singleton
/// ops-операций (одна активная ops-операция на процесс, гвард TryBegin);
/// пути — на data-volume ноды (NodePaths); verify/restore работают потоково
/// (без staging, spec t01 §6.17). Тесты подменяют сам интерфейс
/// IOpsOperationFactory фейком — этот класс при подмене не инстанцируется.</summary>
public sealed class NodeOperationFactory(OpsOperationRegistry registry) : IOpsOperationFactory
{
    public IReceiverOperation CreateVerify(StartVerifyRequest request)
        => new VerifyOperation(request, registry);

    public IReceiverOperation CreateRestore(StartRestoreRequest request)
        => new RestoreOperation(request, new ProcessRunner(), registry);
}
