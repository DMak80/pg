namespace PgWorker.App.Api.Operations;

// Исключения API воркера (task etcd-via-worker-api): перенос панельных
// (src/AdminPanel.Api/Operations/CreateClusterCommand.cs/DeleteClusterCommand.cs),
// тексты 1:1 — фронт-контракт панели не меняется.

// Валидация не прошла: 400 с errors по полям (arch/02 §9.3).
public sealed class CreateClusterValidationException(IReadOnlyList<PgWorker.Core.Writing.ValidationError> errors)
    : Exception("параметры создания кластера некорректны")
{
    public IReadOnlyList<PgWorker.Core.Writing.ValidationError> Errors { get; } = errors;
}

// Клэйм-txn не сошёлся: имя занято (arch/02 §9.2) — 409.
public sealed class ClusterAlreadyExistsException(string name)
    : Exception($"кластер {name} уже существует (config-ключ присутствует)");

// Все etcd-endpoint'ы недоступны — писать некуда — 503.
public sealed class EtcdWriteUnavailableException()
    : Exception("нет активного etcd-endpoint'а (снапшот пуст или etcd недоступен)");

// Кластера нет (config-ключ отсутствует) или имя неканоническое — 404 (arch/02 §9.4).
public sealed class ClusterNotFoundException(string name)
    : Exception($"кластер {name} не найден (config-ключ отсутствует)");

// Config-ключ есть, но не парсится/без обязательных полей — 503 (arch/02 §9.4).
public sealed class InvalidClusterConfigException(string name)
    : Exception($"config кластера {name} битый или без обязательных полей buckets/dbname");

// Валидация add-shard не прошла: 400 с errors по полям (arch/02 §9.5).
public sealed class AddShardValidationException(IReadOnlyList<PgWorker.Core.Writing.ValidationError> errors)
    : Exception("параметры добавления шарда некорректны")
{
    public IReadOnlyList<PgWorker.Core.Writing.ValidationError> Errors { get; } = errors;
}

// Кластер не Active: NOT_INITIALIZED («дождитесь инициализации») или TO_REMOVE
// («кластер удаляется») — подсказка оператору по state (§9.5/§9.6).
public sealed class ClusterNotActiveException(string name, string state)
    : Exception(state == "NOT_INITIALIZED"
        ? $"кластер {name} ещё инициализируется (NOT_INITIALIZED) — дождитесь инициализации"
        : $"кластер {name} удаляется (TO_REMOVE) — операция запрещена");

// Клэйм-txn имени не сошёлся: конкурентный POST занял имя (arch/02 §9.5).
public sealed class ShardNameTakenException(string cluster, string shard)
    : Exception($"имя шарда {cluster}/{shard} занято (replicas-ключ присутствует)");

// shard<max+1> превысил предел числа шардов (§9.3: ≤128).
public sealed class ShardLimitReachedException(string cluster)
    : Exception($"кластер {cluster} достиг предела числа шардов (128)");

// Нешардированная БД (1 бакет, ≤1 шард — arch/03 §2): шарды есть только в
// шардированной; добавить шард к нешардированной нельзя — это просто кластер.
public sealed class NonShardedClusterException(string cluster)
    : Exception($"БД {cluster} нешардированная — шарды только в шардированной (для полного демонтажа/расширения пересоздайте кластер нужного типа)");

// Шард не найден (replicas-ключ отсутствует) — 404 (arch/02 §9.6).
public sealed class ShardNotFoundException(string cluster, string shard)
    : Exception($"шард {cluster}/{shard} не найден (replicas-ключ отсутствует)");

// Быстрая серверная пред-проверка guard'ов (Д4): воркер перепроверит авторитетно.
public sealed class ShardRemoveBlockedException(string reason) : Exception(reason)
{
    public static ShardRemoveBlockedException Buckets(int count)
        => new($"на шарде {count} бакетов — сначала явно перевезите (UI переездов — t07)");

    public static ShardRemoveBlockedException UnfinishedMove()
        => new("незавершённый переезд бакета — завершите/отмените");

    public static ShardRemoveBlockedException LastShard()
        => new("нельзя снять последний шард — для полного демонтажа удалите кластер");

    public static ShardRemoveBlockedException Quarantine()
        => new("шард в карантине после эвакуации — сначала разбор данных");
}

// Пред-проверки не смогли прочитать данные (все endpoints недоступны) — 503.
public sealed class ShardPrecheckUnavailableException()
    : Exception("снапшот панели отстаёт — повторите запрос");

// Валидация тела заявок переездов не прошла: 400 (arch/02 §9.7).
public sealed class MoveBucketsValidationException(IReadOnlyList<PgWorker.Core.Writing.ValidationError> errors)
    : Exception("параметры переноса бакетов некорректны")
{
    public IReadOnlyList<PgWorker.Core.Writing.ValidationError> Errors { get; } = errors;
}

// Приёмник в демонтаже: на удаляемый шард везти нельзя (arch/02 §9.7 п.2; источник
// TO_REMOVE допустим — эвакуация перед демонтажем, spec Д9).
public sealed class MoveTargetRemovingException(string cluster, string shard)
    : Exception($"шард-приёмник {cluster}/{shard} удаляется (TO_REMOVE) — выберите другой приёмник");

// Бакет не годен для переезда с источника: не его владелец / не ACTIVE / вне диапазона.
public sealed class BucketNotOnSourceException(int bucket, string? owner, string state)
    : Exception($"бакет {bucket} не доступен для переезда (владелец: {owner ?? "—"}, состояние: {state})");

// На бакете уже стоит иная заявка — чужие не перезаписываем (arch/02 §9.7 п.3).
public sealed class MoveRequestConflictException(string bucket, string op, string? to)
    : Exception($"на {bucket} уже стоит заявка (op={op}, to={to ?? "—"}) — дождитесь её обработки или уберите ключ");

// Txn-клэйм не сошёлся: конкурентная заявка заняла ключ между чтением и записью.
public sealed class MoveClaimLostException(int bucket)
    : Exception($"конкурентная заявка заняла bucket_{bucket} между чтением и записью — повторите запрос");

// Валидация тел move-ops (rollback/finalize/abort) не прошла: 400 с errors
// по полям (arch/02 §9.7.2–§9.7.4) — тот же маппинг, что у MoveBucketsValidationException.
public sealed class MoveOpValidationException(IReadOnlyList<PgWorker.Core.Writing.ValidationError> errors)
    : Exception("параметры операции переездов некорректны")
{
    public IReadOnlyList<PgWorker.Core.Writing.ValidationError> Errors { get; } = errors;
}

// Бакет не в состоянии, требуемом операцией (02 §9.7.2–§9.7.4): тексты по op —
// как у процесса (rollback/finalize «только из ACTIVE»; abort — см. фабрики).
public sealed class BucketNotActiveForMoveOpException(string message) : Exception(message)
{
    public static BucketNotActiveForMoveOpException RollbackOrFinalize(string op, int bucket, string? owner, string state)
        => new($"{op} bucket_{bucket} возможен только из ACTIVE (владелец: {owner ?? "—"}, состояние: {state})");

    public static BucketNotActiveForMoveOpException AbortActive(int bucket)
        => new($"бакет bucket_{bucket} ACTIVE — отменять нечего; пост-flip артефакты убирает finalize");

    public static BucketNotActiveForMoveOpException AbortNotInitialized(int bucket)
        => new($"бакет bucket_{bucket} NOT_INITIALIZED — начальное состояние создаваемого кластера, не переезд");

    public static BucketNotActiveForMoveOpException OutOfRange(string op, int bucket)
        => new($"бакет {bucket} вне диапазона или без routing — операция {op} невозможна");
}

// Finalize: выбранный шард — текущий владелец, убирать нечего (02 §9.7.3) — 409.
public sealed class FinalizeTargetIsOwnerException(string cluster, int bucket, string shard)
    : Exception($"шард {cluster}/{shard} — текущий владелец bucket_{bucket}, убирать нечего");

// Abort: статус-ключ свежий, mover возможно жив (02 §9.7.4; порт текста AbortSequence) — 409.
public sealed class MoveStatusFreshException(long ageSec, long thresholdSec)
    : Exception($"статус обновлён {ageSec}с назад (< AbortMinAgeSec={thresholdSec}с) — переезд, возможно, ещё жив; если mover точно мёртв — force");

// Abort: routing уже указывает на target — доведение осознанно (02 §9.7.4) — 409.
public sealed class MoveAlreadyFlippedException(string target)
    : Exception($"routing уже указывает на target '{target}' — похоже, flip прошёл, а статус-ключ остался; такой abort станет уборкой СТАРОГО шарда (как finalize) — осознанно: force");

// Отмена заявки: ключа /pgworker/moves/<C>/<bucket> нет (02 §9.7.5) — 404.
public sealed class MoveTicketNotFoundException(string cluster, string bucket)
    : Exception($"заявки /pgworker/moves/{cluster}/{bucket} нет");

// Живая заявка ротации уже стоит: не перезаписываем (отмена — runbook/etcdctl).
public sealed class RotationAlreadyRequestedException(string cluster)
    : Exception($"ротация app-пароля {cluster} уже запрошена — дождитесь исполнения (ключ /pgworker/rotations/{cluster})");

// Неизвестный режим пересоздания (допустимы только soft|hard).
public sealed class InvalidRecreateModeException(string mode)
    : Exception($"режим пересоздания «{mode}» недопустим: только soft или hard");

// HA-скоп не найден — 404.
public sealed class ScopeNotFoundException(string scope)
    : Exception($"HA-скоп {scope} не найден");

// Нода не найдена в скопе — 404.
public sealed class NodeNotFoundException(string scope, string node)
    : Exception($"нода {node} не найдена в скопе {scope}");

// Последняя живая нода: пересоздание невозможно (нет источника для basebackup).
public sealed class LastNodeException(string scope, string node)
    : Exception($"нода {node} — последняя в скопе {scope}, пересоздание невозможно")
{
    public string Node { get; } = node;
}

// Все остальные ноды уже в процессе пересоздания (REBUILDING/TO_RECREATE).
public sealed class AllOthersRecreatingException(string scope, string node)
    : Exception($"все остальные ноды скопа {scope} уже пересоздаются — дождитесь завершения")
{
    public string Node { get; } = node;
}

// Ресурс/эндпоинт воркера не найден или выключен — 404 (напр., seed-эндпоинт
// за флагом EnableSeedEndpoint, arch/14 §1.1.1).
public sealed class WorkerApiNotFoundException(string message) : Exception(message);
