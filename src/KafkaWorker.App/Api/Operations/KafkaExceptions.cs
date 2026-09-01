namespace KafkaWorker.App.Api.Operations;

// Исключения API воркера (task etcd-via-worker-api): перенос панельных
// (src/AdminPanel.Api/Operations/Kafka/KafkaCommands.cs + RebalanceCommands.cs),
// тексты 1:1 — фронт-контракт панели не меняется.

// Все etcd-endpoint'ы недоступны — писать некуда — 503 (текст панельного
// EtcdWriteUnavailableException из CreateClusterCommand.cs).
public sealed class EtcdWriteUnavailableException()
    : Exception("нет активного etcd-endpoint'а (снапшот пуст или etcd недоступен)");

// Кластер не найден (config-ключа нет / имя неканоническое) — 404.
public sealed class KafkaClusterNotFoundException(string cluster)
    : Exception($"kafka-кластер {cluster} не найден");

// Кластер не Active (NOT_INITIALIZED/TO_REMOVE) — 409.
public sealed class KafkaClusterNotActiveException(string cluster, string state)
    : Exception($"kafka-кластер {cluster} не Active (state={state}) — операция отклонена");

// Битый config в etcd — 503.
public sealed class InvalidKafkaConfigException(string cluster)
    : Exception($"config kafka-кластера {cluster} не читается (битый JSON)");

// Валидация: 400 с errors по полям.
public sealed class KafkaValidationException(IReadOnlyList<KafkaWorker.Core.Writing.ValidationError> errors)
    : Exception("параметры некорректны")
{
    public IReadOnlyList<KafkaWorker.Core.Writing.ValidationError> Errors { get; } = errors;
}

// RMW-compare проигран (конкурентная запись) — повтор запроса клиентом.
public sealed class KafkaConcurrentWriteException(string key)
    : Exception($"{key} изменился с момента чтения — повторите запрос");

// kafka-кластер с таким именем уже существует — 409 (мутация 1).
public sealed class KafkaClusterAlreadyExistsException(string name)
    : Exception($"kafka-кластер {name} уже существует");

// Брокер уже заявлен (state-ключ присутствует) — 409 (мутация 4).
public sealed class KafkaBrokerNameTakenException(string name)
    : Exception($"брокер {name} уже заявлен (state-ключ присутствует)");

// Достигнут предел 9 брокеров — 409 (мутация 4).
public sealed class KafkaBrokerLimitException()
    : Exception("достигнут предел 9 брокеров");

// Брокер не найден — 404 (мутация 5).
public sealed class KafkaBrokerNotFoundException(string cluster, string broker)
    : Exception($"брокер {broker} kafka-кластера {cluster} не найден");

// Брокер — controller-нода, демонтаж запрещён — 409 (мутация 5).
public sealed class KafkaBrokerIsControllerException(string cluster, string broker)
    : Exception($"брокер {broker} — controller-нода кластера {cluster}, демонтаж запрещён (роль фиксируется навсегда)");

// Нельзя снять последний брокер — 409 (мутация 5).
public sealed class KafkaLastBrokerException(string cluster)
    : Exception($"нельзя снять последний брокер кластера {cluster}");

// Живая заявка ротации — 409 (мутация 8).
public sealed class KafkaRotationAlreadyRequestedException(string cluster)
    : Exception($"ротация app-пароля {cluster} уже запрошена — дождитесь исполнения");

// Живая заявка ребалансировки — 409 (мутация 13).
public sealed class KafkaRebalanceAlreadyRequestedException(string cluster)
    : Exception($"ребалансировка партиций {cluster} уже запрошена — дождитесь исполнения или отмените");

// Заявка ребалансировки не найдена (отмена) — 404 (мутация 14).
public sealed class KafkaRebalanceNotFoundException(string cluster)
    : Exception($"заявка ребалансировки {cluster} не найдена");

// Топик не найден — 404 (мутации 6–12; Task 9).
public sealed class KafkaTopicNotFoundException(string cluster, string topic, string? reason = null)
    : Exception($"топик {topic} kafka-кластера {cluster} не найден" + (reason is null ? "" : $" ({reason})"));

// Битый ключ topics/<T> — 503 (факт реестра испорчен; Task 9).
public sealed class InvalidKafkaTopicKeyException(string cluster, string topic)
    : Exception($"ключ топика {topic} kafka-кластера {cluster} не читается (битый JSON)");

// Конфиг-заявка топика не найдена (отмена) — 404 (мутация 7; Task 9).
public sealed class KafkaTopicDesiredNotFoundException(string cluster, string topic)
    : Exception($"конфиг-заявка топика {topic} kafka-кластера {cluster} не найдена");

// Топик уже существует в реестре — 409 (create; Task 9).
public sealed class KafkaTopicExistsException(string cluster, string topic)
    : Exception($"топик {topic} kafka-кластера {cluster} уже существует");

// Живая lifecycle-заявка на топик — 409 (Task 9).
public sealed class KafkaLifecyclePendingException(string cluster, string topic, string op)
    : Exception($"заявка {op} топика {topic} kafka-кластера {cluster} уже жива — дождитесь исполнения или отмените");

// Живая конфиг-заявка desired у топика — 409 (create/delete требуют отмены; Task 9).
public sealed class KafkaDesiredPendingException(string cluster, string topic)
    : Exception($"у топика {topic} кластера {cluster} живая конфиг-заявка desired — сначала отмените её");

// Lifecycle-заявка не найдена (отмена) — 404 (мутации 11/12; Task 9).
public sealed class KafkaLifecycleNotFoundException(string cluster, string topic, string op)
    : Exception($"заявка {op} топика {topic} kafka-кластера {cluster} не найдена");

// Ресурс/эндпоинт воркера не найден или выключен — 404 (напр., seed-эндпоинт
// за флагом EnableSeedEndpoint, arch/16 §1.1; Task 10).
public sealed class WorkerApiNotFoundException(string message) : Exception(message);
