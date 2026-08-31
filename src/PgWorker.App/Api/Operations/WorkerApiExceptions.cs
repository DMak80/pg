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
