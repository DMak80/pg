namespace PgWorker.PgNode.Operations;

/// <summary>Общая грань операции бэкапов для маршрутов API (spec §3.2):
/// RunAsync — весь пайплайн (маршрут запускает его фоновой Task); CancelAsync —
/// best-effort отмена (cancel API: гасит дерево текущего child-процесса, чистит
/// staging). Снимок статуса маршруты читают из OperationRegistry (прод-путь);
/// собственный Snapshot-член реализациям не нужен в интерфейсе.</summary>
public interface IReceiverOperation
{
    Task RunAsync(CancellationToken ct);

    Task CancelAsync();

    /// <summary>Гашение при остановке хоста приёмника (arch/pgnode/
    /// 06-restore-drill.md §2): внутренний канал, ОТЛИЧНЫЙ от операторской
    /// отмены — гасит задачу и postmaster операции, но снимок НЕ
    /// перезаписывается (после смерти процесса реестра нет — воркер идёт
    /// vanished-путём 404 → повтор POST).</summary>
    Task ShutdownAsync();
}

/// <summary>Фабрика операции полного бэкапа (нодовый режим; DI-контракт Program —
/// реализации шага 5, тесты подменяют фейками, шаг 9).</summary>
public interface IFullBackupOperationFactory
{
    IReceiverOperation Create(string id);
}

/// <summary>Фабрика ops-операций ноды (verify/restore по виду запроса;
/// DI-контракт Program — реализации шага 5, тесты подменяют фейками, шаг 9).</summary>
public interface IOpsOperationFactory
{
    IReceiverOperation CreateVerify(StartVerifyRequest request);

    IReceiverOperation CreateRestore(StartRestoreRequest request);
}
