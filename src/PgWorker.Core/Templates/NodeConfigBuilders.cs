using System.Globalization;
using System.Text;
using PgWorker.Core.Model;
using PgWorker.Core.Tuning;

namespace PgWorker.Core.Templates;

/// <summary>Топология шарда: scope = "&lt;C&gt;-&lt;X&gt;", адреса всех нод.</summary>
public sealed record ShardTopology(string Cluster, string Shard, string Scope,
    IReadOnlyDictionary<string, NodeAddress> Nodes);

/// <summary>
/// Секреты установки (Д7, spec §10): per-install из env PgWorker, в etcd не
/// пишутся. Прокидываются в контейнер ноды; НИКОГДА не попадают в конфиги
/// doorman/haproxy (текстовые файлы томов).
/// </summary>
public sealed record InstallSecrets(string SuPassword, string StandbyPassword,
    string BucketAdminPassword, string MoverPassword,
    string BucketAdminUser = "bucket_admin");

/// <summary>
/// ENV контейнера pgworker-node для Spilo/Patroni. SPILO_CONFIGURATION —
/// YAML-строка по эталону arch/configs/postgres/pg.env с правками PgWorker:
/// P11 (канонические тайминги PatroniTimings: ttl=20/loop_wait=1/
/// retry_timeout=3 — полы Patroni 4.x, t09; применяется при первой
/// инициализации DCS-кластера, далее гарантируется конвергенцией §5 C) +
/// callback on_role_change → lease-скрипт мастер-ключа
/// /clusters/&lt;C&gt;/shards/&lt;X&gt;/master,
/// P3 (wal_level=logical, sync_replication_slots, max_slot_wal_keep_size),
/// P15 (максимальные соединения и бюджет doorman — синхронизированы).
/// При tuning != null параметры PG — merge(PGTune ∪ канон), построение набора —
/// в PgParametersCanon.Desired (единый источник с конвергенцией t11, arch/14
/// §2.1/§5 C); SpiloEnvBuilder только цитирует значения для YAML (все — в
/// двойных кавычках, КРОМЕ wal_level — исторический raw-string; порядок §5.2,
/// канон поверх без дубликатов). tuning == null — прежний хардкод-набор (тесты
/// драйвера, изолированные пути). Per-нода PGW_NODE_HOST добавляет драйвер при
/// создании контейнера.
/// </summary>
public static class SpiloEnvBuilder
{
    public static IReadOnlyDictionary<string, string> Build(
        ShardTopology topology, EtcdEndpoints etcd, InstallSecrets secrets,
        PgTuneResult? tuning = null, IReadOnlySet<string>? excludeParams = null)
    {
        // Patroni DCS: Spilo строит его из env ETCD3_HOSTS (etcd v3 API; наш etcd
        // 3.5 без v2). Формат — "host:port" БЕЗ scheme (полный URL Patroni
        // оборачивает в "[...]:" и падает); lease-скрипту нужен полный URL — PGW_ETCD.
        var etcdHosts = string.Join(",", etcd.Http.Select(StripScheme));
        var etcdUrls = string.Join(",", etcd.Http);
        var masterKey = $"/clusters/{topology.Cluster}/shards/{topology.Shard}/master";

        return new Dictionary<string, string>
        {
            // Идентификация Patroni-кластера (scope глобально уникален, arch/11 §2).
            ["SCOPE"] = topology.Scope,
            ["ETCD3_HOSTS"] = etcdHosts,

            // Учётные данные PostgreSQL (bootstrap Spilo).
            ["PGUSER_SUPERUSER"] = "postgres",
            ["PGPASSWORD_SUPERUSER"] = secrets.SuPassword,
            ["PGUSER_STANDBY"] = "standby",
            ["PGPASSWORD_STANDBY"] = secrets.StandbyPassword,

            // Пароли ролей бакетного слоя (создаёт DatabaseProvisioner; здесь —
            // доступность внутри контейнера для админ-скриптов). App-пароль в env
            // НЕ прокидывается — per-cluster в etcd (spec §4.1). bucket_admin —
            // per-cluster credentials из config (переопределены в ProvisioningProcess).
            ["PGW_BUCKET_ADMIN_PASSWORD"] = secrets.BucketAdminPassword,
            ["PGW_BUCKET_ADMIN_USER"] = secrets.BucketAdminUser,
            ["PGW_BUCKET_MOVER_PASSWORD"] = secrets.MoverPassword,

            // ENV lease-скрипта мастер-ключа (callback on_role_change, P11).
            ["PGW_ETCD"] = etcdUrls,
            ["PGW_MASTER_KEY"] = masterKey,

            // Пути Spilo: PGROOT НЕ переопределяем — кастомный PGROOT ломает
            // bootstrap (data-каталог создаётся root'ом, patroni под postgres
            // падает); данные живут в дефолтном /home/postgres/pgdata (туда
            // монтируется volume, ClusterDriver).
            ["USE_DATA_DIR_FOR_WAL"] = "true",

            // Patroni-конфигурация: эталон pg.env; параметры PG — merge(PGTune ∪
            // канон) при tuning != null, иначе прежний хардкод-набор; тайминги —
            // из канона PatroniTimings (полы Patroni 4.x, t09).
            ["SPILO_CONFIGURATION"] = $$"""
                ---
                bootstrap:
                  dcs:
                    ttl: {{PatroniTimings.Ttl}}
                    loop_wait: {{PatroniTimings.LoopWait}}
                    retry_timeout: {{PatroniTimings.RetryTimeout}}
                    synchronous_mode: true
                    synchronous_mode_strict: false
                    postgresql:
                      use_pg_rewind: true
                      callbacks:
                        # P11: ключ /clusters/<C>/shards/<X>/master под etcd-lease.
                        # on_start — старт ноды (роль может быть уже master);
                        # on_role_change — промоушен/демоут.
                        on_start: /home/postgres/master-lease.py
                        on_role_change: /home/postgres/master-lease.py
                      parameters:
                {{ParametersBlock(tuning, excludeParams)}}
                """,
        };
    }

    // Блок parameters YAML (отступ 8): tuning == null → прежний хардкод-набор
    // (тесты драйвера, изолированные пути); иначе merge(PGTune ∪ канон).
    private static string ParametersBlock(PgTuneResult? tuning, IReadOnlySet<string>? excludeParams) =>
        tuning is null ? CanonicalParametersBlock() : TunedParametersBlock(tuning, excludeParams);

    // Прежний хардкод-набор (без PGTune): значения и комментарии — 1:1 как до
    // внедрения PGTune (справка: это поведение изолированных путей/старых тестов).
    private static string CanonicalParametersBlock() => string.Join("\n",
    [
        "        # P15: 55 pg_doorman + 2 админ/mover + 3 reserved",
        "        max_connections: \"60\"",
        "        shared_buffers: \"2GB\"",
        "        effective_cache_size: \"6GB\"",
        "        # P3: логическое декодирование + failover slots",
        "        wal_level: logical",
        "        hot_standby: \"on\"",
        "        sync_replication_slots: \"on\"",
        "        max_slot_wal_keep_size: \"16GB\"",
        "        max_wal_senders: \"10\"",
        "        max_replication_slots: \"10\"",
        "        wal_keep_size: \"2048MB\"",
        "        checkpoint_timeout: \"15min\"",
        "        checkpoint_completion_target: \"0.9\"",
        "        random_page_cost: \"1.1\"",
        "        logging_collector: \"on\"",
        "        log_directory: \"log\"",
        "        log_filename: \"postgresql-%Y-%m-%d.log\"",
        "        log_rotation_age: \"1d\"",
        "        log_rotation_size: \"100MB\"",
    ]);

    // Канон PgWorker поверх PGTune (P3 + лог-блок) переехал в
    // PgParametersCanon.CanonParameters (t11) — единый источник с конвергенцией.

    // Merge PGTune ∪ канон — единый источник PgParametersCanon.Desired (t11);
    // YAML-цитирование — деталь ЭТОГО сериализатора: все значения в двойных
    // кавычках, КРОМЕ wal_level (исторический стиль raw-string, инвариант
    // байт-в-байт SPILO_CONFIGURATION).
    private static string TunedParametersBlock(PgTuneResult tuning, IReadOnlySet<string>? excludeParams)
    {
        var desired = PgParametersCanon.Desired(tuning, excludeParams);
        return string.Join("\n", desired.Select(p =>
            $"        {p.Name}: {(p.Name == "wal_level" ? p.RawValue : $"\"{p.RawValue}\"")}"));
    }

    // Примечание: секию postgresql (bin_dir/use_unix_socket) НЕ задаём —
    // дефолты Spilo корректны; переопределение use_unix_socket ломает
    // bootstrap (patroni не может создать data_dir от пользователя postgres).

    // "http://host:port" → "host:port" (Patroni etcd3.hosts без scheme).
    private static string StripScheme(string url)
        => url.StartsWith("http://", StringComparison.Ordinal)
            ? url["http://".Length..]
            : url.StartsWith("https://", StringComparison.Ordinal)
                ? url["https://".Length..]
                : url;
}

/// <summary>
/// Конфиг pg_doorman ноды (arch/11 §4): ЕДИНСТВЕННЫЙ пул &lt;dbname&gt; (P14) с
/// бэкендом 127.0.0.1:5432 этой ноды, transaction-режим (P13), бюджет
/// серверных соединений — синхронизирован от рассчитанного PGTune
/// max_connections (P15: serverConnections = max(10, max_connections − 5);
/// 60 = 55 + 2 админ/mover + 3 reserved). Клиентский вход — :6432 c TLS
/// (sslmode=require, P17 — требование на стороне клиентских DSN); SCRAM
/// passthrough к PG. Финальная сверка полей с релизом doorman — при сборке
/// образа (задача 25).
/// </summary>
public static class DoormanConfigBuilder
{
    public static string Build(string dbname, int serverConnections) =>
        $"""
        # pg_doorman: единственный пул {dbname} на ноду (P13/P14/P15/P17).
        # Клиенты подключаются на :6432 с sslmode=require (P17).

        [pg_doorman]
        listen = "0.0.0.0:6432"
        pool_mode = "transaction"
        max_client_connections = 1000
        max_db_connections = {serverConnections}
        default_pool_size = {serverConnections}
        tls_mode = "require"

        [databases]
        # P14: dbname = имя кластера, бакеты — схемы; один пул на всю БД ноды.
        {dbname} = host=127.0.0.1 port=5432 dbname={dbname}
        """;

    /// <summary>
    /// Бюджет серверных соединений doorman от рассчитанного тюнинга (P15,
    /// решение пользователя): max(10, max_connections − 5). Параметр
    /// max_connections выводится всегда (§4.0 спецификации алгоритма), поэтому
    /// отсутствие значения — фейл сборки спеки, не тихий дефолт.
    /// </summary>
    public static int ServerConnections(PgTuneResult tuning)
    {
        var raw = tuning["max_connections"];
        if (raw is null || !int.TryParse(raw, CultureInfo.InvariantCulture, out var maxConnections))
            throw new ApplicationException(
                "pgtune: рассчитанный тюнинг не содержит числового max_connections " +
                "(параметр выводится всегда — §4.0 спецификации алгоритма)");
        return Math.Max(10, maxConnections - 5);
    }
}

/// <summary>
/// Конфиг HAProxy ноды (arch/11 §4): только write-фронтенд :5432 — вход
/// репликационного трафика переездов (P2). Бэкенды — Patroni-REST всех нод
/// шарда (httpchk GET /primary); read-фронтенд и stats не нужны (арх/14 §2.1).
/// </summary>
public static class HaproxyConfigBuilder
{
    public static string Build(ShardTopology topology)
    {
        var sb = new StringBuilder();
        sb.AppendLine("""
            # haproxy: write-фронтенд :5432 к текущему лидеру шарда (P2).
            # Health-check — Patroni REST /primary каждой ноды (port patroni).

            global
                log stdout format raw local0
                maxconn 4096

            defaults
                log     global
                mode    tcp
                option  tcplog
                timeout connect 5s
                timeout client  1m
                timeout server  1m
                retries 2

            frontend ft_pg_write
                bind *:5432
                default_backend bk_pg_master

            backend bk_pg_master
                balance first
                option httpchk GET /primary
                http-check expect status 200
                default-server inter 3s fall 3 rise 2
            """);

        // Бэкенды: все ноды шарда — host:pgPort с check-port Patroni.
        foreach (var node in topology.Nodes.OrderBy(n => n.Key))
        {
            var addr = node.Value;
            sb.AppendLine(
                $"    server {node.Key} {addr.Host}:{addr.Ports.Pg} check port {addr.Ports.Patroni}");
        }

        return sb.ToString();
    }
}
