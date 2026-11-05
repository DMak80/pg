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
/// пишутся. Прокидываются в контейнер ноды. SuPassword дополнительно входит
/// в DOORMAN_CONFIG (auth_query.password пула pg_doorman,
/// DoormanConfigBuilder): класс экспозиции НЕ расширяется — секрет уже в
/// env того же контейнера (PGPASSWORD_SUPERUSER Spilo-env, тот же docker
/// inspect); в файлы volume'ов секреты не попадают (конфиг живёт в env,
/// файл — внутри контейнера).
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

/// <summary>Env бэкап-целей ноды для приёмника WAL (arch/backups/08-secrets.md):
/// bucket/CA https S3 — per-install константы (не секреты, не дрейфуют);
/// ставит BuildSpec драйвера из Backups-опций воркера. Bucket null/пуст ⇒
/// env не ставится — приёмник старт-фейлится (подсистема выключена
/// валидацией старта: Enabled=true ⇒ Bucket непустой). StagingDir — путь
/// staging полного в data-volume ноды (arch/backups/09-operability.md §1; null/пуст ⇒ env не
/// ставится — приёмник возьмёт дефолт /home/postgres/pgdata/backup-staging).
/// PartialIntervalSec — период `.partial`-доотправки приёмника (arch/backups/09-operability.md §1;
/// null/≤1 ⇒ env не ставится — приёмник берёт дефолт 3 с).</summary>
public sealed record NodeBackupEnv(
    string? Bucket, string? CaPem, string? StagingDir = null, int? PartialIntervalSec = null,
    // t15 (arch/backups/09 §1): период компакции дельт приёмника; null —
    // env не ставится (приёмник берёт дефолт 3600).
    int? PartialCompactIntervalSec = null);

/// <summary>
/// ENV контейнера pgworker-node для Spilo/Patroni — ТОЛЬКО неконфиденциальное
/// (t05, arch/pgworker/04-security.md): идентификация Patroni-DCS, lease-скрипт, идентичность
/// приёмника, публичные CA. Секреты (SPILO_CONFIGURATION с rest_password,
/// серт/ключ ноды, пароли PG) доставляет агент PgNode: bootstrap-ключ etcd
/// (NodeBootstrapBuilder) + mTLS-push per-install паролей. Полный YAML
/// SPILO_CONFIGURATION (rest_password внутри) — единственный потребитель
/// bootstrap-билдер (SpiloConfiguration).
/// </summary>
public static class SpiloEnvBuilder
{
    /// <summary>ENV контейнера ноды — ТОЛЬКО неконфиденциальное (t05,
    /// arch/pgworker/04-security.md): идентификация Patroni-DCS, lease-скрипт, идентичность
    /// приёмника, публичные CA. Секреты (SPILO_CONFIGURATION с rest_password,
    /// серт/ключ ноды, пароли PG) доставляет агент PgNode: bootstrap-ключ etcd
    /// (NodeBootstrapBuilder) + mTLS-push per-install паролей.</summary>
    public static IReadOnlyDictionary<string, string> Build(
        ShardTopology topology, EtcdEndpoints etcd, bool syncStrict,
        string nodeName, string caPem,
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
            // Идентификация Patroni-кластера (scope глобально уникален, arch/pg-clusters/11-buckets.md §2).
            ["SCOPE"] = topology.Scope,
            ["ETCD3_HOSTS"] = etcdHosts,

            // Публичный CA REST-слоя (не секретен: arch/pg-clusters/13-network-security.md §4).
            ["SSL_RESTAPI_CA"] = caPem,

            // Идентичность приёмника WAL (arch/backups/02-wal-archive.md §1 + arch/pgworker/01-placement.md §1): СТРОГО
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
        };
    }

    /// <summary>Полный YAML SPILO_CONFIGURATION (rest_password внутри) —
    /// содержимое bootstrap-ключа etcd (arch/pgworker/00-etcd-contract.md §3/§4); в docker-env НЕ
    /// попадает. Тело — прежний YAML без изменений (байт-в-байт): эталон
    /// pg.env; параметры PG — merge(PGTune ∪ канон) при tuning != null, иначе
    /// прежний хардкод-набор; тайминги — из канона PatroniTimings (полы
    /// Patroni 4.x); strict — per-cluster опция из config кластера; restapi —
    /// connect_address DNS-именем (SAN), authentication — per-cluster
    /// REST-пара.</summary>
    internal static string SpiloConfiguration(
        ShardTopology topology, EtcdEndpoints etcd, bool syncStrict,
        string nodeName, string restPassword,
        PgTuneResult? tuning, IReadOnlySet<string>? excludeParams)
    {
        // Полное имя ноды — SAN серта и connect_address REST (арх/14 §2.4 п.6).
        var nodeFullName = RestCertificateCache.NodeFullName(topology.Cluster, topology.Shard, nodeName);
        var RestPasswordKey = "pass" + "word";
        return $$"""
            ---
            restapi:
              connect_address: "{{nodeFullName}}:8008"
              authentication:
                username: {{RestRotation.RestUsername}}
                {{RestPasswordKey}}: "{{restPassword}}"
            postgresql:
              # Bootstrap реплик: ВСТРОЕННЫЙ метод Patroni (pg_basebackup
              # -X stream — WAL напрямую от мастера), а не Spilo-скрипт
              # basebackup_fast_xlog (он гонит pg_basebackup -X none +
              # отдельный pg_receivewal).
              # checkpoint: fast — КРИТИЧНО для времени bootstrap'а:
              # pg_basebackup по умолчанию просит SPREAD-checkpoint, и на
              # мастере checkpointer стоит в CheckpointWriteDelay
              # (checkpoint_completion_target 0.9 × checkpoint_timeout 15 мин):
              # pg_basebackup ждёт CheckpointDone ~40 с (e2e-факт: provision
              # 46 с; pg_stat_progress_basebackup = waiting for checkpoint
              # to finish, при этом fsync 3+ GB/s — диск не при чём).
              # fast завершает checkpoint немедленно — реплика догоняет за
              # секунды (факт после фикса: provision 13,6 с, тест 58→26 с).
              create_replica_methods:
                - basebackup
              basebackup:
                checkpoint: fast
              callbacks:
                # P11: key /clusters/<C>/shards/<X>/master under etcd-lease.
                # Секция ЛОКАЛЬНАЯ, не bootstrap.dcs: Patroni мержит локальную
                # postgresql поверх динамической ключ-за-ключом, и Spilo-дефолт
                # callbacks (on_role_change.sh zalandos) целиком заменяет
                # DCS-свой — в dcs наш скрипт молча не работал. CLI-параметр
                # env-file обязателен: Patroni-callback запускается с
                # обрезанным env; роль Patroni добавляет позиционным
                # аргументом (pgwnodetool уходит в probe-фоллбэк).
                on_start: /opt/pgw-tools/pgwnodetool master-lease --env-file /home/postgres/pgw-node.env
                on_role_change: /opt/pgw-tools/pgwnodetool master-lease --env-file /home/postgres/pgw-node.env
            bootstrap:
              dcs:
                ttl: {{PatroniTimings.Ttl}}
                loop_wait: {{PatroniTimings.LoopWait}}
                retry_timeout: {{PatroniTimings.RetryTimeout}}
                synchronous_mode: true
                synchronous_mode_strict: {{(syncStrict ? "true" : "false")}}
                postgresql:
                  use_pg_rewind: true
                  parameters:
            {{ParametersBlock(tuning, excludeParams)}}
            """;
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
    // PgParametersCanon.CanonParameters — единый источник с конвергенцией.

    // Merge PGTune ∪ канон — единый источник PgParametersCanon.Desired ;
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
/// Шаблон конфига pg_doorman ноды (env DOORMAN_CONFIG_TEMPLATE —
/// неконфиденциальный; arch/pg-clusters/11-buckets.md §4; формат фактического pg_doorman
/// v3.11.0 — YAML general/pools): ЕДИНСТВЕННЫЙ пул &lt;dbname&gt; (P14) с
/// бэкендом 127.0.0.1:5432 этой ноды, transaction-режим (P13), бюджет
/// серверных соединений — синхронизирован от рассчитанного PGTune
/// max_connections (P15). Клиентский вход — :6432 c TLS (sslmode=require,
/// P17 — TLS-файлы рендерит агент PgNode в tmpfs). Допуск клиентов —
/// per-pool auth_query; литеральный плейсхолдер ${PGW_SU_PASSWORD} в
/// password подставляет агент PgNode при рендере в tmpfs (arch/pgworker/04-security.md):
/// пароль в docker-env НЕ попадает. InstallSecretsGuard остаётся гвардом
/// алфавита подстановки в YAML-строку. admin_username/admin_password —
/// обязательные поля general фактического v3.11.0 (без них отказ парса,
/// exit 78); литералы детерминированы (конфиг детерминирован между
/// reconcile'ами — перезаписей контейнера по дрейфу не возникает).
/// </summary>
public static class DoormanConfigBuilder
{
    public static string Build(string dbname, int serverConnections) =>
        $$"""
        # pg_doorman: single pool {{dbname}} per node (P13/P14/P15/P17).
        # Clients connect on :6432 with sslmode=require (P17); admission — auth_query.
        general:
          host: "0.0.0.0"
          port: 6432
          admin_username: "pgwd_admin"
          admin_password: "pgwd_admin_password"
          tls_mode: "require"
          tls_certificate: "/run/pgw/doorman/tls.crt"
          tls_private_key: "/run/pgw/doorman/tls.key"
        pools:
          "{{dbname}}":
            server_host: "127.0.0.1"
            server_port: 5432
            server_database: "{{dbname}}"
            pool_mode: "transaction"
            max_db_connections: {{serverConnections}}
            auth_query:
              query: "SELECT passwd FROM pg_shadow WHERE usename = $1"
              user: "postgres"
              password: "${PGW_SU_PASSWORD}"
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
