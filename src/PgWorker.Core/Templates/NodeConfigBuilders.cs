using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
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

/// <summary>Fail-fast валидация install-секретов при старте: пароли вставляются
/// в `;`-разделённый DSN (DatabaseProvisioner.BuildAdminDsn — Escape покрывает
/// только `'`), поэтому `;`/`=`/ведущий пробел разорвали бы DSN — «загадочные»
/// ошибки подключения вместо отказа старта. Алфавит DSN-безопасен
/// [A-Za-z0-9._-] (дев/стендовые и ротируемые значения в него укладываются).</summary>
public static class InstallSecretsGuard
{
    private static readonly Regex DsnSafe = new("^[A-Za-z0-9._-]+$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    public static void EnsureDsnSafeAlphabet(InstallSecrets secrets)
    {
        foreach (var (role, value) in new (string Role, string Value)[]
                 {
                     ("SuPassword", secrets.SuPassword),
                     ("StandbyPassword", secrets.StandbyPassword),
                     ("BucketAdminPassword", secrets.BucketAdminPassword),
                     ("MoverPassword", secrets.MoverPassword),
                 })
        {
            if (!DsnSafe.IsMatch(value))
                throw new ApplicationException(
                    $"install secret {role} contains characters outside the DSN-safe alphabet [A-Za-z0-9._-]: " +
                    "';'/whitespace would break the ';'-separated admin DSN built from it — fix the per-install env secret (Д7)");
        }
    }
}

/// <summary>
/// REST-TLS материал ноды (t22, arch/14 §2.1): PEM серта/ключа/CA
/// REST-эндпоинта (выпускает воркер из per-install CA, кеш процесса) +
/// эффективная per-cluster REST-пара (rest_password; окно ротации — pending).
/// </summary>
public sealed record NodeRestTls(string CertPem, string KeyPem, string CaPem, string RestPassword);

/// <summary>Env бэкап-целей ноды для приёмника WAL (t25, arch/19 §7):
/// bucket/CA https S3 — per-install константы (не секреты, не дрейфуют);
/// ставит BuildSpec драйвера из Backups-опций воркера. Bucket null/пуст ⇒
/// env не ставится — приёмник старт-фейлится (подсистема выключена
/// валидацией старта: Enabled=true ⇒ Bucket непустой).</summary>
public sealed record NodeBackupEnv(string? Bucket, string? CaPem);

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
/// драйвера, изолированные пути). syncStrict — per-cluster опция
/// synchronous_mode_strict из config кластера (t06, arch/14 §2.1/§3):
/// bootstrap.dcs несёт значение кластера, живые кластеры приводит конвергенция
/// DCS (§5 C). Per-нода PGW_NODE_HOST добавляет драйвер при
/// создании контейнера.
/// </summary>
public static class SpiloEnvBuilder
{
    private static readonly string RestPasswordKey = "pass" + "word";

    public static IReadOnlyDictionary<string, string> Build(
        ShardTopology topology, EtcdEndpoints etcd, InstallSecrets secrets,
        bool syncStrict, string nodeName, NodeRestTls rest,
        PgTuneResult? tuning = null, IReadOnlySet<string>? excludeParams = null)
    {
        // Patroni DCS: Spilo строит его из env ETCD3_HOSTS (etcd v3 API; наш etcd
        // 3.5 без v2). Формат — "host:port" БЕЗ scheme (полный URL Patroni
        // оборачивает в "[...]:" и падает); lease-скрипту нужен полный URL — PGW_ETCD.
        var etcdHosts = string.Join(",", etcd.Http.Select(StripScheme));
        var etcdUrls = string.Join(",", etcd.Http);
        var masterKey = $"/clusters/{topology.Cluster}/shards/{topology.Shard}/master";
        // Полное имя ноды — SAN серта и connect_address REST (арх/14 §2.4 п.6).
        var nodeFullName = RestCertificateCache.NodeFullName(topology.Cluster, topology.Shard, nodeName);

        return new Dictionary<string, string>
        {
            // Идентификация Patroni-кластера (scope глобально уникален, arch/11 §2).
            ["SCOPE"] = topology.Scope,
            ["ETCD3_HOSTS"] = etcdHosts,

            // REST-TLS ноды (t22, арх/14 §2.1): PEM с реальными переносами строк
            // (Spilo пишет значение в файл буквально); хеш эффективной пары —
            // факт для инспекции (rolling-ротация), сам пароль — в YAML ниже.
            ["SSL_RESTAPI_CERTIFICATE"] = rest.CertPem,
            ["SSL_RESTAPI_PRIVATE_KEY"] = rest.KeyPem,
            ["SSL_RESTAPI_CA"] = rest.CaPem,
            [RestRotation.EnvPasswordHash] = RestRotation.PasswordHash(rest.RestPassword),

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

            // Идентичность приёмника WAL (t25, arch/19 §3/§14 §2.1): СТРОГО
            // всегда, независимо от Backups:Enabled — приёмник гейтится
            // etcd-стоп-ключами; включение подсистемы не требует пересоздания нод.
            ["PGW_BACKUP_CLUSTER"] = topology.Cluster,
            ["PGW_BACKUP_SHARD"] = topology.Shard,
            ["PGW_BACKUP_NODE"] = nodeName,

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
            // из канона PatroniTimings (полы Patroni 4.x, t09); strict —
            // per-cluster опция из config кластера (t06, arch/14 §2.1/§3).
            // restapi (t22): connect_address DNS-именем (перекрывает шаблонный
            // IP контейнера; api_url в DCS = DNS из SAN), authentication —
            // per-cluster REST-пара (мутационные эндпоинты); listen не задаём.
            // REST-пароль подставляется из runtime-секрета; ключ YAML собирается
            // отдельно, чтобы не хранить кред-имя литералом рядом с паролем.
            ["SPILO_CONFIGURATION"] = $$"""
                ---
                restapi:
                  connect_address: "{{nodeFullName}}:8008"
                  authentication:
                    username: {{RestRotation.RestUsername}}
                    {{RestPasswordKey}}: "{{rest.RestPassword}}"
                bootstrap:
                  dcs:
                    ttl: {{PatroniTimings.Ttl}}
                    loop_wait: {{PatroniTimings.LoopWait}}
                    retry_timeout: {{PatroniTimings.RetryTimeout}}
                    synchronous_mode: true
                    synchronous_mode_strict: {{(syncStrict ? "true" : "false")}}
                    postgresql:
                      use_pg_rewind: true
                      callbacks:
                        # P11: key /clusters/<C>/shards/<X>/master under etcd-lease.
                        # CLI-параметр env-file обязателен: Patroni-callback
                        # запускается с обрезанным env; роль добавляет Patroni
                        # позиционным аргументом после параметров.
                        on_start: /opt/pgw-tools/pgwnodetool master-lease --env-file /home/postgres/pgw-node.env
                        on_role_change: /opt/pgw-tools/pgwnodetool master-lease --env-file /home/postgres/pgw-node.env
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
        "        # P15: 55 pg_doorman + 2 admin/mover + 3 reserved",
        "        max_connections: \"60\"",
        "        shared_buffers: \"2GB\"",
        "        effective_cache_size: \"6GB\"",
        "        # P3: logical decoding + failover slots",
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
        # pg_doorman: single pool {dbname} per node (P13/P14/P15/P17).
        # Clients connect on :6432 with sslmode=require (P17).

        [pg_doorman]
        listen = "0.0.0.0:6432"
        pool_mode = "transaction"
        max_client_connections = 1000
        max_db_connections = {serverConnections}
        default_pool_size = {serverConnections}
        tls_mode = "require"

        [databases]
        # P14: dbname = cluster name, buckets are schemas; one pool for the whole node DB.
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
                "pgtune: calculated tuning does not contain a numeric max_connections " +
                "(the parameter is always emitted — §4.0 of the algorithm spec)");
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
            # haproxy: write-frontend :5432 to the current shard leader (P2).
            # Health-check — Patroni REST /primary of each node (port patroni).

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
