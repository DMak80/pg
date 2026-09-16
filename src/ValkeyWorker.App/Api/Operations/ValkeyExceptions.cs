namespace ValkeyWorker.App.Api.Operations;

// Исключения API воркера valkey-домена (arch/21 §1.1; порт KafkaExceptions):
// тексты ProblemDetails-маппинга ApiModule.

// Все etcd-endpoint'ы недоступны — писать некуда — 503.
public sealed class EtcdWriteUnavailableException()
    : Exception("нет активного etcd-endpoint'а (снапшот пуст или etcd недоступен)");

// Кластер не найден (config-ключа нет / имя неканоническое) — 404.
public sealed class ValkeyClusterNotFoundException(string cluster)
    : Exception($"valkey-кластер {cluster} не найден");

// Кластер не Active (NOT_INITIALIZED/TO_REMOVE) — 409.
public sealed class ValkeyClusterNotActiveException(string cluster, string state)
    : Exception($"valkey-кластер {cluster} не Active (state={state}) — операция отклонена");

// Битый config в etcd — 503.
public sealed class InvalidValkeyConfigException(string cluster)
    : Exception($"config valkey-кластера {cluster} не читается (битый JSON)");

// Валидация: 400 с errors по полям.
public sealed class ValkeyValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("параметры некорректны")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// RMW-compare проигран (конкурентная запись) — повтор запроса клиентом.
public sealed class ValkeyConcurrentWriteException(string key)
    : Exception($"{key} изменился с момента чтения — повторите запрос");

// Кластер с таким именем уже существует — 409.
public sealed class ValkeyClusterAlreadyExistsException(string name)
    : Exception($"valkey-кластер {name} уже существует");

// Нода кластера не найдена — 404.
public sealed class ValkeyNodeNotFoundException(string cluster, string node)
    : Exception($"нода {node} valkey-кластера {cluster} не найдена");

// Живая заявка ротации — 409.
public sealed class ValkeyRotationAlreadyRequestedException(string cluster)
    : Exception($"ротация пароля {cluster} уже запрошена — дождитесь исполнения");

// Ресурс/эндпоинт воркера не найден или выключен — 404 (напр., seed за флагом).
public sealed class WorkerApiNotFoundException(string message) : Exception(message);
