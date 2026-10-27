namespace PgWorker.WalReceiver.Operations;

/// <summary>Общая грань операции бэкапов для маршрутов API (spec t26 §3.2):
/// RunAsync — весь пайплайн (маршрут запускает его фоновой Task); CancelAsync —
/// best-effort отмена (cancel API: гасит дерево текущего child-процесса, чистит
/// staging). Снимок статуса маршруты читают из OperationRegistry (прод-путь);
/// собственный Snapshot-член реализациям не нужен в интерфейсе.</summary>
public interface IReceiverOperation
{
    Task RunAsync(CancellationToken ct);

    Task CancelAsync();
}

/// <summary>Фабрика операции полного бэкапа (нодовый режим; DI-контракт Program —
/// реализации шага 5, тесты подменяют фейками, шаг 9).</summary>
public interface IFullBackupOperationFactory
{
    IReceiverOperation Create(string id);
}

/// <summary>Фабрика tool-операций (tool-режим: verify/restore по виду запроса;
/// DI-контракт Program — реализации шагов 6–7, тесты подменяют фейками, шаг 9).</summary>
public interface IToolOperationFactory
{
    IReceiverOperation CreateVerify(StartVerifyRequest request);

    IReceiverOperation CreateRestore(StartRestoreRequest request);
}
