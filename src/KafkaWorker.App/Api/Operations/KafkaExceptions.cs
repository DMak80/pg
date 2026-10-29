namespace KafkaWorker.App.Api.Operations;

// Исключения API воркера (task etcd-via-worker-api): перенос панельных
// (src/AdminPanel.Api/Operations/Kafka/KafkaCommands.cs + RebalanceCommands.cs),
// тексты 1:1 — фронт-контракт панели не меняется.

// Все etcd-endpoint'ы недоступны — писать некуда — 503 (текст панельного
// EtcdWriteUnavailableException из CreateClusterCommand.cs).
public sealed class EtcdWriteUnavailableException()
    : Exception("no active etcd endpoint (snapshot is empty or etcd is unavailable)");

// Кластер не найден (config-ключа нет / имя неканоническое) — 404.
public sealed class KafkaClusterNotFoundException(string cluster)
    : Exception($"kafka cluster {cluster} not found");

// Кластер не Active (NOT_INITIALIZED/TO_REMOVE) — 409.
public sealed class KafkaClusterNotActiveException(string cluster, string state)
    : Exception($"kafka cluster {cluster} is not Active (state={state}) — operation rejected");

// Битый config в etcd — 503.
public sealed class InvalidKafkaConfigException(string cluster)
    : Exception($"config of kafka cluster {cluster} is unreadable (broken JSON)");

// Валидация: 400 с errors по полям.
public sealed class KafkaValidationException(IReadOnlyList<ValidationError> errors)
    : Exception("invalid parameters")
{
    public IReadOnlyList<ValidationError> Errors { get; } = errors;
}

// RMW-compare проигран (конкурентная запись) — повтор запроса клиентом.
public sealed class KafkaConcurrentWriteException(string key)
    : Exception($"{key} changed since it was read — retry the request");

// kafka-кластер с таким именем уже существует — 409 (мутация 1).
public sealed class KafkaClusterAlreadyExistsException(string name)
    : Exception($"kafka cluster {name} already exists");

// Брокер уже заявлен (state-ключ присутствует) — 409 (мутация 4).
public sealed class KafkaBrokerNameTakenException(string name)
    : Exception($"broker {name} is already claimed (state key present)");

// Достигнут предел 9 брокеров — 409 (мутация 4).
public sealed class KafkaBrokerLimitException()
    : Exception("broker limit of 9 reached");

// Брокер не найден — 404 (мутация 5).
public sealed class KafkaBrokerNotFoundException(string cluster, string broker)
    : Exception($"broker {broker} of kafka cluster {cluster} not found");

// Брокер — controller-нода, демонтаж запрещён — 409 (мутация 5).
public sealed class KafkaBrokerIsControllerException(string cluster, string broker)
    : Exception($"broker {broker} is the controller node of cluster {cluster}, decommission is forbidden (the role is fixed forever)");

// Мутация №15: брокер в демонтаже — ресурсы менять незачем — 409 (02 §10.2-15).
public sealed class KafkaBrokerRemovalInProgressException(string cluster, string broker)
    : Exception($"broker {cluster}/{broker} is claimed for removal (TO_REMOVE/REMOVING) — resource update rejected");

// Нельзя снять последний брокер — 409 (мутация 5).
public sealed class KafkaLastBrokerException(string cluster)
    : Exception($"cannot remove the last broker of cluster {cluster}");

// Живая заявка ротации — 409 (мутация 8).
public sealed class KafkaRotationAlreadyRequestedException(string cluster)
    : Exception($"app password rotation for {cluster} is already requested — wait for completion");

// Живая заявка ребалансировки — 409 (мутация 13).
public sealed class KafkaRebalanceAlreadyRequestedException(string cluster)
    : Exception($"partition rebalance for {cluster} is already requested — wait for completion or cancel it");

// Заявка ребалансировки не найдена (отмена) — 404 (мутация 14).
public sealed class KafkaRebalanceNotFoundException(string cluster)
    : Exception($"rebalance request for {cluster} not found");

// Топик не найден — 404 (мутации 6–12; Task 9).
public sealed class KafkaTopicNotFoundException(string cluster, string topic, string? reason = null)
    : Exception($"topic {topic} of kafka cluster {cluster} not found" + (reason is null ? "" : $" ({reason})"));

// Битый ключ topics/<T> — 503 (факт реестра испорчен; Task 9).
public sealed class InvalidKafkaTopicKeyException(string cluster, string topic)
    : Exception($"key of topic {topic} of kafka cluster {cluster} is unreadable (broken JSON)");

// Конфиг-заявка топика не найдена (отмена) — 404 (мутация 7; Task 9).
public sealed class KafkaTopicDesiredNotFoundException(string cluster, string topic)
    : Exception($"config request for topic {topic} of kafka cluster {cluster} not found");

// Топик уже существует в реестре — 409 (create; Task 9).
public sealed class KafkaTopicExistsException(string cluster, string topic)
    : Exception($"topic {topic} of kafka cluster {cluster} already exists");

// Живая lifecycle-заявка на топик — 409 (Task 9).
public sealed class KafkaLifecyclePendingException(string cluster, string topic, string op)
    : Exception($"{op} request for topic {topic} of kafka cluster {cluster} is already pending — wait for completion or cancel it");

// Живая конфиг-заявка desired у топика — 409 (create/delete требуют отмены; Task 9).
public sealed class KafkaDesiredPendingException(string cluster, string topic)
    : Exception($"topic {topic} of cluster {cluster} has a pending desired config request — cancel it first");

// Lifecycle-заявка не найдена (отмена) — 404 (мутации 11/12; Task 9).
public sealed class KafkaLifecycleNotFoundException(string cluster, string topic, string op)
    : Exception($"{op} request for topic {topic} of kafka cluster {cluster} not found");

// Ресурс/эндпоинт воркера не найден или выключен — 404 (напр., seed-эндпоинт
// за флагом EnableSeedEndpoint, arch/16 §1.1; Task 10).
public sealed class WorkerApiNotFoundException(string message) : Exception(message);
