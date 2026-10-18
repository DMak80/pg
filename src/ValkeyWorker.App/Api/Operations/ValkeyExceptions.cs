namespace ValkeyWorker.App.Api.Operations;

// Исключения API воркера valkey-домена (arch/21 §1.1; порт KafkaExceptions):
// тексты ProblemDetails-маппинга ApiModule.

// Все etcd-endpoint'ы недоступны — писать некуда — 503.
public sealed class EtcdWriteUnavailableException()
    : Exception("no active etcd endpoint (snapshot is empty or etcd is unreachable)");

// Кластер не найден (config-ключа нет / имя неканоническое) — 404.
public sealed class ValkeyClusterNotFoundException(string cluster)
    : Exception($"valkey cluster {cluster} not found");

// Кластер не Active (NOT_INITIALIZED/TO_REMOVE) — 409.
public sealed class ValkeyClusterNotActiveException(string cluster, string state)
    : Exception($"valkey cluster {cluster} is not Active (state={state}) — operation rejected");

// Битый config в etcd — 503.
public sealed class InvalidValkeyConfigException(string cluster)
    : Exception($"config of valkey cluster {cluster} is unreadable (malformed JSON)");

// Валидация: 400 с errors по полям.
public sealed class ValkeyValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("invalid parameters")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// RMW-compare проигран (конкурентная запись) — повтор запроса клиентом.
public sealed class ValkeyConcurrentWriteException(string key)
    : Exception($"{key} changed since it was read — retry the request");

// Кластер с таким именем уже существует — 409.
public sealed class ValkeyClusterAlreadyExistsException(string name)
    : Exception($"valkey cluster {name} already exists");

// Нода кластера не найдена — 404.
public sealed class ValkeyNodeNotFoundException(string cluster, string node)
    : Exception($"node {node} of valkey cluster {cluster} not found");

// Живая заявка ротации — 409.
public sealed class ValkeyRotationAlreadyRequestedException(string cluster)
    : Exception($"password rotation of {cluster} already requested — wait for it to complete");

// Ресурс/эндпоинт воркера не найден или выключен — 404 (напр., seed за флагом).
public sealed class WorkerApiNotFoundException(string message) : Exception(message);
