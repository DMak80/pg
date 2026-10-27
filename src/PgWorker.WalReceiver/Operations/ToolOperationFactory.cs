namespace PgWorker.WalReceiver.Operations;

/// <summary>Прод-фабрика tool-операций (DI Program tool-режима): одна операция
/// на контейнер — общий OperationRegistry; runner — общий ProcessRunner
/// процесса. Тесты подменяют сам интерфейс IToolOperationFactory фейком
/// (шаг 9) — этот класс при подмене не инстанцируется.</summary>
public sealed class ToolOperationFactory(OperationRegistry registry) : IToolOperationFactory
{
    public IReceiverOperation CreateVerify(StartVerifyRequest request)
        => new VerifyOperation(request, new ProcessRunner(), registry);

    public IReceiverOperation CreateRestore(StartRestoreRequest request)
        => new RestoreOperation(request, new ProcessRunner(), registry);
}
