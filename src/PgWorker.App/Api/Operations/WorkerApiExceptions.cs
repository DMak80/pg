
using PgWorker.Core.Writing;
namespace PgWorker.App.Api.Operations;

// Исключения API воркера (task etcd-via-worker-api): перенос панельных
// (src/AdminPanel.Api/Operations/CreateClusterCommand.cs/DeleteClusterCommand.cs),
// тексты 1:1 — фронт-контракт панели не меняется.

// Валидация policy бэкапов не прошла: 400 с errors по полям (t06, arch/19 §4).
public sealed class BackupsPolicyValidationException(
    IReadOnlyList<ValidationError> errors)
    : Exception("backup policy is invalid")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// Валидация не прошла: 400 с errors по полям (arch/02 §9.3).
public sealed class CreateClusterValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("cluster creation parameters are invalid")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// Клэйм-txn не сошёлся: имя занято (arch/02 §9.2) — 409.
public sealed class ClusterAlreadyExistsException(string name)
    : Exception($"cluster {name} already exists (config key is present)");

// Все etcd-endpoint'ы недоступны — писать некуда — 503.
public sealed class EtcdWriteUnavailableException()
    : Exception("no active etcd endpoint (snapshot is empty or etcd is unavailable)");

// Кластера нет (config-ключ отсутствует) или имя неканоническое — 404 (arch/02 §9.4).
public sealed class ClusterNotFoundException(string name)
    : Exception($"cluster {name} not found (config key is missing)");

// Config-ключ есть, но не парсится/без обязательных полей — 503 (arch/02 §9.4).
public sealed class InvalidClusterConfigException(string name)
    : Exception($"cluster {name} config is malformed or missing required fields buckets/dbname");

// Валидация add-shard не прошла: 400 с errors по полям (arch/02 §9.5).
public sealed class AddShardValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("shard addition parameters are invalid")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// Валидация мутации config не прошла: 400 с errors по полям (t06, 02 §9.10).
public sealed class UpdateClusterConfigValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("config mutation parameters are invalid")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// RMW-txn проигран: config изменился между чтением и записью — 503, retry клиентом (t06).
public sealed class ClusterConcurrentWriteException(string key)
    : Exception($"concurrent write modified {key} between read and write — retry the request");

// Кластер не Active: NOT_INITIALIZED («дождитесь инициализации») или TO_REMOVE
// («кластер удаляется») — подсказка оператору по state (§9.5/§9.6).
public sealed class ClusterNotActiveException(string name, string state)
    : Exception(state == "NOT_INITIALIZED"
        ? $"cluster {name} is still initializing (NOT_INITIALIZED) — wait for initialization"
        : $"cluster {name} is being removed (TO_REMOVE) — operation is forbidden");

// Клэйм-txn имени не сошёлся: конкурентный POST занял имя (arch/02 §9.5).
public sealed class ShardNameTakenException(string cluster, string shard)
    : Exception($"shard name {cluster}/{shard} is taken (replicas key is present)");

// shard<max+1> превысил предел числа шардов (§9.3: ≤128).
public sealed class ShardLimitReachedException(string cluster)
    : Exception($"cluster {cluster} has reached the shard limit (128)");

// Нешардированная БД (1 бакет, ≤1 шард — arch/03 §2): шарды есть только в
// шардированной; добавить шард к нешардированной нельзя — это просто кластер.
public sealed class NonShardedClusterException(string cluster)
    : Exception($"database {cluster} is non-sharded — shards exist only in sharded clusters (to fully dismantle or extend, recreate the cluster of the desired type)");

// Шард не найден (replicas-ключ отсутствует) — 404 (arch/02 §9.6).
public sealed class ShardNotFoundException(string cluster, string shard)
    : Exception($"shard {cluster}/{shard} not found (replicas key is missing)");

// Быстрая серверная пред-проверка guard'ов (Д4): воркер перепроверит авторитетно.
public sealed class ShardRemoveBlockedException(string reason) : Exception(reason)
{
    public static ShardRemoveBlockedException Buckets(int count)
        => new($"на шарде {count} бакетов — сначала явно перевезите (UI переездов — t07)");

    public static ShardRemoveBlockedException UnfinishedMove()
        => new("unfinished bucket move — complete or cancel it");

    public static ShardRemoveBlockedException LastShard()
        => new("cannot remove the last shard — delete the cluster for a full teardown");

    public static ShardRemoveBlockedException Quarantine()
        => new("shard is quarantined after evacuation — investigate the data first");
}

// Пред-проверки не смогли прочитать данные (все endpoints недоступны) — 503.
public sealed class ShardPrecheckUnavailableException()
    : Exception("panel snapshot is stale — retry the request");

// Валидация тела заявок переездов не прошла: 400 (arch/02 §9.7).
public sealed class MoveBucketsValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("bucket move parameters are invalid")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// Приёмник в демонтаже: на удаляемый шард везти нельзя (arch/02 §9.7 п.2; источник
// TO_REMOVE допустим — эвакуация перед демонтажем, spec Д9).
public sealed class MoveTargetRemovingException(string cluster, string shard)
    : Exception($"target shard {cluster}/{shard} is being removed (TO_REMOVE) — choose another target");

// Бакет не годен для переезда с источника: не его владелец / не ACTIVE / вне диапазона.
public sealed class BucketNotOnSourceException(int bucket, string? owner, string state)
    : Exception($"бакет {bucket} не доступен для переезда (владелец: {owner ?? "—"}, состояние: {state})");

// На бакете уже стоит иная заявка — чужие не перезаписываем (arch/02 §9.7 п.3).
public sealed class MoveRequestConflictException(string bucket, string op, string? to)
    : Exception($"{bucket} already has a move request (op={op}, to={to ?? "—"}) — wait for it to be processed or remove the key");

// Txn-клэйм не сошёлся: конкурентная заявка заняла ключ между чтением и записью.
public sealed class MoveClaimLostException(int bucket)
    : Exception($"a concurrent request claimed bucket_{bucket} between read and write — retry the request");

// Валидация тел move-ops (rollback/finalize/abort) не прошла: 400 с errors
// по полям (arch/02 §9.7.2–§9.7.4) — тот же маппинг, что у MoveBucketsValidationException.
public sealed class MoveOpValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("move operation parameters are invalid")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// Бакет не в состоянии, требуемом операцией (02 §9.7.2–§9.7.4): тексты по op —
// как у процесса (rollback/finalize «только из ACTIVE»; abort — см. фабрики).
public sealed class BucketNotActiveForMoveOpException(string message) : Exception(message)
{
    public static BucketNotActiveForMoveOpException RollbackOrFinalize(string op, int bucket, string? owner, string state)
        => new($"{op} bucket_{bucket} возможен только из ACTIVE (владелец: {owner ?? "—"}, состояние: {state})");

    public static BucketNotActiveForMoveOpException AbortActive(int bucket)
        => new($"bucket bucket_{bucket} is ACTIVE — nothing to abort; post-flip artifacts are cleaned up by finalize");

    public static BucketNotActiveForMoveOpException AbortNotInitialized(int bucket)
        => new($"бакет bucket_{bucket} NOT_INITIALIZED — начальное состояние создаваемого кластера, не переезд");

    public static BucketNotActiveForMoveOpException OutOfRange(string op, int bucket)
        => new($"bucket {bucket} is out of range or has no routing — operation {op} is not possible");
}

// Finalize: выбранный шард — текущий владелец, убирать нечего (02 §9.7.3) — 409.
public sealed class FinalizeTargetIsOwnerException(string cluster, int bucket, string shard)
    : Exception($"шард {cluster}/{shard} — текущий владелец bucket_{bucket}, убирать нечего");

// Abort: статус-ключ свежий, mover возможно жив (02 §9.7.4; порт текста AbortSequence) — 409.
public sealed class MoveStatusFreshException(long ageSec, long thresholdSec)
    : Exception($"status was updated {ageSec}s ago (< AbortMinAgeSec={thresholdSec}s) — the move may still be alive; if the mover is definitely dead — force");

// Abort: routing уже указывает на target — доведение осознанно (02 §9.7.4) — 409.
public sealed class MoveAlreadyFlippedException(string target)
    : Exception($"routing уже указывает на target '{target}' — похоже, flip прошёл, а статус-ключ остался; такой abort станет уборкой СТАРОГО шарда (как finalize) — осознанно: force");

// Отмена заявки: ключа /pgworker/moves/<C>/<bucket> нет (02 §9.7.5) — 404.
public sealed class MoveTicketNotFoundException(string cluster, string bucket)
    : Exception($"заявки /pgworker/moves/{cluster}/{bucket} нет");

// Живая заявка ротации уже стоит: не перезаписываем (отмена — runbook/etcdctl).
public sealed class RotationAlreadyRequestedException(string cluster)
    : Exception($"rotation of the app password for {cluster} has already been requested — wait for it to complete (key /pgworker/rotations/{cluster})");

// Неизвестный режим пересоздания (допустимы только soft|hard).
public sealed class InvalidRecreateModeException(string mode)
    : Exception($"recreate mode '{mode}' is invalid: only soft or hard");

// HA-скоп не найден — 404.
public sealed class ScopeNotFoundException(string scope)
    : Exception($"HA scope {scope} not found");

// Нода не найдена в скопе — 404.
public sealed class NodeNotFoundException(string scope, string node)
    : Exception($"node {node} not found in scope {scope}");

// Последняя живая нода: пересоздание невозможно (нет источника для basebackup).
public sealed class LastNodeException(string scope, string node)
    : Exception($"нода {node} — последняя в скопе {scope}, пересоздание невозможно")
{
    public string Node { get; } = node;
}

// Все остальные ноды уже в процессе пересоздания (REBUILDING/TO_RECREATE).
public sealed class AllOthersRecreatingException(string scope, string node)
    : Exception($"all other nodes of scope {scope} are already being recreated — wait for completion")
{
    public string Node { get; } = node;
}

// Ресурс/эндпоинт воркера не найден или выключен — 404 (напр., seed-эндпоинт
// за флагом EnableSeedEndpoint, arch/14 §1.1.1).
public sealed class WorkerApiNotFoundException(string message) : Exception(message);

// ── Restore (t05, arch/19 §3.5) ──

// confirm != имя шарда — 400 (необратимая операция: защита от ошибки оператора).
public sealed class ConfirmMismatchException(string expected, string got)
    : Exception($"confirm must match the shard name: expected '{expected}', got '{got}'");

// target_time не RFC3339 — 400.
public sealed class InvalidTargetTimeException(string raw)
    : Exception($"target_time is not RFC3339: '{raw}'");

// Активный (PLANNED/RUNNING/REJOINING) restore шарда уже есть — 409 (§3.1:
// максимум один активный restore на шард).
public sealed class RestoreAlreadyActiveException(string cluster, string shard)
    : Exception($"restore of shard {cluster}/{shard} is already active — wait for completion (key restore/<id>)");

// Поля тела — 400 с errors по полям (паттерн CreateClusterValidationException).
public sealed class RestoreValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("restore request body failed validation")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// ── Сироты бэкапов (reliability t04, arch/19 §4) ──

// Префикса нет в реестре сирот / имена неканонические — 404.
public sealed class OrphanNotFoundException(string prefix)
    : Exception($"orphan {prefix} not found in the /pgworker/backups/orphans registry");

// Запись в DELETING — доводку начатого удаления не спасти — 409.
public sealed class OrphanDeletingException(string prefix)
    : Exception($"orphan {prefix} is already DELETING — deletion is in progress and cannot be stopped");

// confirm != "<C>/<X>" — 400 (осознанная потеря DR-источника).
public sealed class OrphanConfirmMismatchException(string expected, string got)
    : Exception($"confirm must match the orphan prefix: expected '{expected}', got '{got}'");

// Подсистема бэкапов выключена — 503 (заявки копятся? нет — API не принимает).
public sealed class BackupsDisabledException()
    : Exception("backups subsystem is disabled (PgWorker:Backups:Enabled=false) — orphan operations are unavailable");
