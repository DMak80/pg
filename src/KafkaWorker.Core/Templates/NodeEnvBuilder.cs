using System.Security.Cryptography;
using System.Text;
using KafkaWorker.Core.Model;

namespace KafkaWorker.Core.Templates;

/// <summary>
/// Вход генератора env брокера (arch/16 §2.2): всё детерминировано от заявки,
/// плана размещения и кредов — повторная генерация даёт тот же набор (R3).
/// </summary>
/// <param name="Cluster">Имя кластера (из него — детерминированный CLUSTER_ID).</param>
/// <param name="NodeId">Числовой id ноды (k из broker&lt;k&gt;).</param>
/// <param name="NodeName">Имя ноды (docker-DNS alias, INTERNAL advertised).</param>
/// <param name="AdvertisedClient">advertised CLIENT host:port — вычислен по правилу
/// arch/16 §2.1 (AdvertisedClientHost ?? имя docker-хоста + клиентский порт).</param>
/// <param name="IsController">Нода — участник KRaft-кворума (combined broker,controller).</param>
/// <param name="QuorumVoters">Кворум "id@host:9093" (только controller-ноды).</param>
/// <param name="AppUser">SASL-пользователь приложений (обычно "app").</param>
/// <param name="AppPasswords">1 пароль (штатно) или 2 (окно ротации: OLD + NEW).</param>
/// <param name="Config">Заявка-конфиг кластера (default-конфиги → env).</param>
/// <param name="BrokerCount">Фактическое B — формулы RF служебных топиков min(3,B).</param>
/// <param name="DataDir">Точка монтирования тома данных (KAFKA_LOG_DIRS).</param>
public sealed record NodeEnvSpec(
    string Cluster,
    int NodeId,
    string NodeName,
    string AdvertisedClient,
    bool IsController,
    IReadOnlyList<string> QuorumVoters,
    string AppUser,
    IReadOnlyList<string> AppPasswords,
    KafkaClusterConfig Config,
    int BrokerCount,
    string DataDir);

/// <summary>
/// Генератор env брокера apache/kafka:4.0.0 (arch/16 §2.2, канон таблицы).
/// KRaft без ZooKeeper, SASL_PLAINTEXT на INTERNAL/CLIENT, PLAINTEXT CONTROLLER
/// внутри сети kfw-net-<C> кластера; служебные топики — RF min(3,B)/minISR min(2,B), чтобы
/// 1-брокерный стенд стартовал.
/// </summary>
public static class NodeEnvBuilder
{
    public static IReadOnlyDictionary<string, string> Build(NodeEnvSpec spec)
    {
        var users = BuildJaasUsers(spec.AppUser, spec.AppPasswords);

        // Inter-broker: INTERNAL требует SASL и у БРОКЕРА-КЛИЕНТА должны быть
        // креды — иначе фолловеры не подключаются и ISR проседает до лидера
        // (вскрыто 3-брокерным e2e волны C). Креды inter НЕ ротируются
        // (ротация app не должна ломать репликацию) — детерминированный
        // per-cluster пароль, живёт пересоздания контейнеров; listener доступен
        // только внутри закрытой сети kfw-net-<C> кластера (arch/16 §2.1).
        var interPassword = InterBrokerPassword(spec.Cluster);
        var env = new Dictionary<string, string>
        {
            // KRaft-идентичность: cluster-id детерминирован из имени кластера —
            // одинаков у всех нод и переживает пересоздание контейнера (том
            // хранит метаданные KRaft с этим id).
            ["CLUSTER_ID"] = ClusterId(spec.Cluster),
            ["KAFKA_NODE_ID"] = spec.NodeId.ToString(CultureInfoInvariant),
            ["KAFKA_PROCESS_ROLES"] = spec.IsController ? "broker,controller" : "broker",
            ["KAFKA_CONTROLLER_QUORUM_VOTERS"] = string.Join(",", spec.QuorumVoters),

            // Слушатели: CONTROLLER только у кворумных нод.
            ["KAFKA_LISTENERS"] = spec.IsController
                ? "CONTROLLER://:9093,INTERNAL://:9092,CLIENT://:9094"
                : "INTERNAL://:9092,CLIENT://:9094",
            ["KAFKA_ADVERTISED_LISTENERS"] =
                $"INTERNAL://{spec.NodeName}:9092,CLIENT://{spec.AdvertisedClient}",
            ["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] =
                "CONTROLLER:PLAINTEXT,INTERNAL:SASL_PLAINTEXT,CLIENT:SASL_PLAINTEXT",
            ["KAFKA_CONTROLLER_LISTENER_NAMES"] = "CONTROLLER",
            ["KAFKA_INTER_BROKER_LISTENER_NAME"] = "INTERNAL",
            ["KAFKA_SASL_ENABLED_MECHANISMS"] = "PLAIN",
            ["KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL"] = "PLAIN", // требует Kafka при SASL на INTERNAL
            // INTERNAL-JAAS: username/password (клиент inter-broker) + серверные
            // пользователи (inter и app-креды с окном ротации).
            ["KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG"] =
                $"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"inter\" password=\"{interPassword}\" user_inter=\"{interPassword}\" {users};",
            ["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"] = Jaas(users),

            // Служебные топики: формулы от фактического B (1-брокерный стенд стартует).
            ["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] = Min(spec.BrokerCount, 3).ToString(CultureInfoInvariant),
            ["KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"] = Min(spec.BrokerCount, 3).ToString(CultureInfoInvariant),
            ["KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"] = Min(spec.BrokerCount, 2).ToString(CultureInfoInvariant),

            // Default-конфиги из заявки кластера.
            ["KAFKA_DEFAULT_REPLICATION_FACTOR"] = spec.Config.ReplicationFactor.ToString(CultureInfoInvariant),
            ["KAFKA_MIN_INSYNC_REPLICAS"] = spec.Config.MinInSyncReplicas.ToString(CultureInfoInvariant),
            ["KAFKA_NUM_PARTITIONS"] = spec.Config.DefaultPartitions.ToString(CultureInfoInvariant),
            ["KAFKA_LOG_RETENTION_MS"] = spec.Config.DefaultRetentionMs.ToString(CultureInfoInvariant),

            // Состав топиков — только явное создание (CLI/клиентами, arch/15 §3).
            ["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] = "false",
            ["KAFKA_LOG_DIRS"] = spec.DataDir,
        };

        return env;
    }

    // Детерминированный inter-broker-пароль: 32 симв [A-Za-z0-9] из SHA-256
    // имени кластера (не хранится в etcd: не ротируется, нужен только нодам
    // кластера внутри kfw-net-<C>).
    public static string InterBrokerPassword(string cluster)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes("kafka-inter:" + cluster));
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var chars = new char[32];
        for (var i = 0; i < 32; i++)
            chars[i] = alphabet[hash[i] % alphabet.Length];
        return new string(chars);
    }

    // Детерминированный KRaft cluster-id: 16 байт SHA-256 имени кластера в
    // base64url без паддинга — 22 символа (формат Kafka uuid).
    public static string ClusterId(string cluster)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes("kafka:" + cluster));
        return Convert.ToBase64String(hash, 0, 16)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }

    // user_<name>="<password>" через пробел; в окне ротации второй кред —
    // user_<name>2. Пароли ТОЛЬКО в двойных кавычках: Java JAAS-парсер не
    // принимает незакавыченное значение, начинающееся с цифры («Value not
    // specified for key …» — брокер падает на старте; алфавит генератора
    // [A-Za-z0-9] даёт ~16% таких паролей).
    private static string BuildJaasUsers(string appUser, IReadOnlyList<string> passwords)
    {
        var users = $"user_{appUser}=\"{passwords[0]}\"";
        if (passwords.Count > 1)
            users += $" user_{appUser}2=\"{passwords[1]}\"";
        return users;
    }

    private static string Jaas(string users)
        => $"org.apache.kafka.common.security.plain.PlainLoginModule required {users};";

    private static int Min(int a, int b) => a < b ? a : b;

    private static System.Globalization.CultureInfo CultureInfoInvariant => System.Globalization.CultureInfo.InvariantCulture;
}
