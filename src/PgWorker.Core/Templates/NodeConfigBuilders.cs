using System.Text;
using PgWorker.Core.Model;

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
    string AppPassword, string BucketAdminPassword, string MoverPassword);

/// <summary>
/// ENV контейнера pgworker-node для Spilo/Patroni. SPILO_CONFIGURATION —
/// YAML-строка по эталону arch/configs/postgres/pg.env с правками PgWorker:
/// P11 (ttl=5/loop_wait=2/retry_timeout=3 + callback on_role_change →
/// lease-скрипт мастер-ключа /clusters/&lt;C&gt;/shards/&lt;X&gt;/master),
/// P3 (wal_level=logical, sync_replication_slots, max_slot_wal_keep_size),
/// P15 (max_connections=60, walsenders/slots=10).
/// Per-нода PGW_NODE_HOST добавляет драйвер при создании контейнера.
/// </summary>
public static class SpiloEnvBuilder
{
    public static IReadOnlyDictionary<string, string> Build(
        ShardTopology topology, EtcdEndpoints etcd, InstallSecrets secrets)
    {
        var etcdHosts = string.Join(",", etcd.Http);
        var masterKey = $"/clusters/{topology.Cluster}/shards/{topology.Shard}/master";

        return new Dictionary<string, string>
        {
            // Идентификация Patroni-кластера (scope глобально уникален, arch/11 §2).
            ["SCOPE"] = topology.Scope,
            ["ETCD_HOSTS"] = etcdHosts,

            // Учётные данные PostgreSQL (bootstrap Spilo).
            ["PGUSER_SUPERUSER"] = "postgres",
            ["PGPASSWORD_SUPERUSER"] = secrets.SuPassword,
            ["PGUSER_STANDBY"] = "standby",
            ["PGPASSWORD_STANDBY"] = secrets.StandbyPassword,

            // Пароли ролей бакетного слоя (создаёт DatabaseProvisioner; здесь —
            // доступность внутри контейнера для админ-скриптов).
            ["PGW_APP_PASSWORD"] = secrets.AppPassword,
            ["PGW_BUCKET_ADMIN_PASSWORD"] = secrets.BucketAdminPassword,
            ["PGW_BUCKET_MOVER_PASSWORD"] = secrets.MoverPassword,

            // ENV lease-скрипта мастер-ключа (callback on_role_change, P11).
            ["PGW_ETCD"] = etcdHosts,
            ["PGW_MASTER_KEY"] = masterKey,

            // Пути Spilo (эталон pg.env).
            ["PGROOT"] = "/home/postgres/pgroot",
            ["USE_DATA_DIR_FOR_WAL"] = "true",

            // Patroni-конфигурация: эталон pg.env с wal_level: logical (P3).
            ["SPILO_CONFIGURATION"] = """
                ---
                bootstrap:
                  dcs:
                    ttl: 5
                    loop_wait: 2
                    retry_timeout: 3
                    synchronous_mode: true
                    synchronous_mode_strict: false
                    postgresql:
                      use_pg_rewind: true
                      callbacks:
                        on_role_change: /home/postgres/master-lease.py
                      parameters:
                        # P15: 55 pg_doorman + 2 админ/mover + 3 reserved
                        max_connections: "60"
                        shared_buffers: "2GB"
                        effective_cache_size: "6GB"
                        # P3: логическое декодирование + failover slots
                        wal_level: logical
                        hot_standby: "on"
                        sync_replication_slots: "on"
                        max_slot_wal_keep_size: "16GB"
                        max_wal_senders: "10"
                        max_replication_slots: "10"
                        wal_keep_size: "2048MB"
                        checkpoint_timeout: "15min"
                        checkpoint_completion_target: "0.9"
                        random_page_cost: "1.1"
                        logging_collector: "on"
                        log_directory: "log"
                        log_filename: "postgresql-%Y-%m-%d.log"
                        log_rotation_age: "1d"
                        log_rotation_size: "100MB"
                postgresql:
                  bin_dir: /usr/lib/postgresql/16/bin
                  use_unix_socket: true
                """,
        };
    }
}

/// <summary>
/// Конфиг pg_doorman ноды (arch/11 §4): ЕДИНСТВЕННЫЙ пул &lt;dbname&gt; (P14) с
/// бэкендом 127.0.0.1:5432 этой ноды, transaction-режим (P13), бюджет 55
/// серверных соединений (P15). Клиентский вход — :6432 c TLS (sslmode=require,
/// P17 — требование на стороне клиентских DSN); SCRAM passthrough к PG.
/// Финальная сверка полей с релизом doorman — при сборке образа (задача 25).
/// </summary>
public static class DoormanConfigBuilder
{
    public static string Build(string dbname) =>
        $"""
        # pg_doorman: единственный пул {dbname} на ноду (P13/P14/P15/P17).
        # Клиенты подключаются на :6432 с sslmode=require (P17).

        [pg_doorman]
        listen = "0.0.0.0:6432"
        pool_mode = "transaction"
        max_client_connections = 1000
        max_db_connections = 55
        default_pool_size = 55
        tls_mode = "require"

        [databases]
        # P14: dbname = имя кластера, бакеты — схемы; один пул на всю БД ноды.
        {dbname} = host=127.0.0.1 port=5432 dbname={dbname}
        """;
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
