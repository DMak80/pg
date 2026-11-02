using System.Text.Json;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;

namespace PgWorker.UnitTests.Templates;

// NodeConfigBuilders: детерминированные конфиги контейнера pgworker-node
// (Spilo env без секретов, шаблон pg_doorman с плейсхолдером, bootstrap-JSON)
// из arch/configs + параметров spec (P2/P3/P11/P13/P14/P15/P17, Д4); PGTune:
// merge(PGTune ∪ канон) в SPILO_CONFIGURATION, exclude, doorman-бюджет от
// рассчитанного max_connections. t05: env — только неконфиденциальное
// (spec §3.1); секреты — bootstrap-ключ (NodeBootstrapBuilder).

public class NodeConfigBuildersTests
{
    private static readonly ShardTopology Topology = new(
        "shop", "shard1", "shop-shard1",
        new Dictionary<string, NodeAddress>
        {
            ["shard1a"] = new("h1", new NodePorts(15432, 18008, 16432)),
            ["shard1b"] = new("h2", new NodePorts(15433, 18009, 16433)),
        });

    private static readonly EtcdEndpoints Etcd = new(["http://e1:2379", "http://e2:2379"]);

    // PGTune-вывод по умолчанию exclude (опции PgWorker:Pgtune по умолчанию).
    private static readonly HashSet<string> DefaultExclude =
        new(["io_method", "io_workers"], StringComparer.Ordinal);

    // Публичный CA REST-слоя (env SSL_RESTAPI_CA) — PEM-заглушка теста.
    private const string CaPem = "-----BEGIN CERTIFICATE-----\nMIIBtest-ca\n-----END CERTIFICATE-----\n";

    // Пароль генерируется в рантайме: секрет не зашивается в исходник;
    // переиспользуется всеми обращениями (bootstrap-рендер).
    private static readonly string RestPassword = GenerateTestPassword();

    private static string GenerateTestPassword()
    {
        // Криптостойкий генератор: 24 символа букв/цифр (безопасно для env/YAML).
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var bytes = System.Security.Cryptography.RandomNumberGenerator.GetBytes(24);
        return new string(bytes.Select(b => alphabet[b % alphabet.Length]).ToArray());
    }

    // SPILO_CONFIGURATION из bootstrap-JSON (env его больше не несёт).
    private static string BootstrapJson(
        PgTuneResult? tuning = null, IReadOnlySet<string>? excludeParams = null,
        bool syncStrict = false) =>
        NodeBootstrapBuilder.Build(Topology, Etcd, syncStrict, "shard1a",
            "-----BEGIN CERTIFICATE-----\nMIIBtest-node\n-----END CERTIFICATE-----\n",
            "-----BEGIN PRIVATE KEY-----\nMIIBtest-key\n-----END PRIVATE KEY-----\n",
            RestPassword, tuning, excludeParams);

    private static string SpiloFromBootstrap(
        PgTuneResult? tuning = null, IReadOnlySet<string>? excludeParams = null,
        bool syncStrict = false)
    {
        using var doc = JsonDocument.Parse(BootstrapJson(tuning, excludeParams, syncStrict));
        return doc.RootElement.GetProperty("spilo_configuration").GetString()!;
    }

    [Fact]
    public void SpiloEnv_CarriesReceiverIdentityEnv()
    {
        // Arrange: топология шарда из 2 нод и адреса etcd.

        // Act: генерируем env контейнера ноды.
        var env = SpiloEnvBuilder.Build(Topology, Etcd, syncStrict: false, "shard1a", CaPem);

        // Assert: идентичность приёмника WAL (arch/19 §3/§14 §2.1) — СТРОГО
        // всегда, независимо от Backups:Enabled (гейтится etcd-стоп-ключами):
        // координаты из topology/nodeName.
        env["PGW_BACKUP_CLUSTER"].Should().Be(Topology.Cluster);
        env["PGW_BACKUP_SHARD"].Should().Be(Topology.Shard);
        env["PGW_BACKUP_NODE"].Should().Be("shard1a");
    }

    [Fact]
    public void SpiloEnv_ContainsPatroniAndPgParameters()
    {
        // Arrange: топология шарда из 2 нод и адреса etcd.

        // Act: SPILO_CONFIGURATION из bootstrap-ключа (env его не несёт).
        var spilo = SpiloFromBootstrap();

        // Assert: SPILO_CONFIGURATION несёт канон таймингов (: полы Patroni
        // 4.x — ttl=20/loop_wait=1/retry_timeout=3), P3 (wal_level),
        // P15 (max_connections) и callback мастер-ключа.
        spilo.Should().Contain("ttl: 20");
        spilo.Should().Contain("loop_wait: 1");
        spilo.Should().Contain("retry_timeout: 3");
        spilo.Should().Contain("wal_level: logical");
        spilo.Should().Contain("max_connections: \"60\"");
        spilo.Should().Contain("sync_replication_slots: \"on\"");
        spilo.Should().Contain("on_role_change");
    }

    [Fact]
    public void SpiloEnv_SpiloConfigurationContainsRestapiSection()
    {
        // Arrange: bootstrap-ключ ноды shard1a (cluster=shop, shard=shard1).

        // Act: SPILO_CONFIGURATION из bootstrap-JSON.
        var spilo = SpiloFromBootstrap();

        // Assert: секция restapi верхнего уровня — connect_address DNS-именем
        // ноды (SAN серта; перекрывает шаблонный IP), authentication — пара
        // per-cluster (пароль в двойных кавычках — YAML-safe);
        // listen НЕ переопределён (шаблонный :8008 корректен).
        spilo.Should().Contain("restapi:");
        spilo.Should().Contain("connect_address: \"pgw-shop-shard1-shard1a:8008\"");
        spilo.Should().Contain("authentication:");
        spilo.Should().Contain("username: patroni");
        spilo.Should().Contain($"password: \"{RestPassword}\"");
        spilo.Should().NotContain("listen");
        // bootstrap-секция не изменена (restapi — дополнение, не замена)
        spilo.Should().Contain("bootstrap:");
        // callbacks — в ЛОКАЛЬНОЙ postgresql-секции (Patroni мержит локальную
        // postgresql поверх динамической ключ-за-ключом: Spilo-дефолт callbacks
        // из шаблона целиком заменяет DCS-свой — в bootstrap.dcs наш скрипт
        // молча не работает); локальная секция идёт ДО bootstrap.
        var callbacksIdx = spilo.IndexOf("postgresql:", StringComparison.Ordinal);
        var bootstrapIdx = spilo.IndexOf("bootstrap:", StringComparison.Ordinal);
        callbacksIdx.Should().BePositive("the local postgresql section must exist");
        callbacksIdx.Should().BeLessThan(bootstrapIdx, "callbacks live in the LOCAL postgresql section, not bootstrap.dcs");
        spilo.Should().Contain("on_start: /opt/pgw-tools/pgwnodetool master-lease --env-file /home/postgres/pgw-node.env");
        spilo.Should().Contain("on_role_change: /opt/pgw-tools/pgwnodetool master-lease --env-file /home/postgres/pgw-node.env");
        spilo.Should().NotContain("dcs.postgresql.callbacks",
            "callbacks must not be under bootstrap.dcs (Spilo default overrides it)");
    }

    [Fact]
    public void SpiloEnv_ContainsScopeAndEtcdHosts()
    {
        // Arrange: scope = "<C>-<X>" и список etcd-эндпоинтов.

        // Act: генерируем env контейнера ноды.
        var env = SpiloEnvBuilder.Build(Topology, Etcd, syncStrict: false, "shard1a", CaPem);

        // Assert: идентификация Patroni-кластера и адреса DCS на месте.
        env["SCOPE"].Should().Be("shop-shard1");
        env["ETCD3_HOSTS"].Should().Be("e1:2379,e2:2379"); // Patroni: host:port без scheme (etcd v3)
        env["PGW_ETCD"].Should().Be("http://e1:2379,http://e2:2379");
        env["PGW_MASTER_KEY"].Should().Be("/clusters/shop/shards/shard1/master");
        env["SSL_RESTAPI_CA"].Should().Be(CaPem);
    }

    // AAA (t05, spec §3.1): env ноды — только неконфиденциальное; секретные
    // ключи в docker-env не попадают вовсе (доставка — агенту PgNode).
    [Fact]
    public void SpiloEnv_SecretsNeverLeak()
    {
        // Arrange: топология/etcd фикстуры файла.
        string[] forbiddenKeys =
        [
            "PGUSER_SUPERUSER", "PGPASSWORD_SUPERUSER", "PGUSER_STANDBY",
            "PGPASSWORD_STANDBY", "PGW_BUCKET_ADMIN_USER", "PGW_BUCKET_ADMIN_PASSWORD",
            "PGW_BUCKET_MOVER_PASSWORD", "SSL_RESTAPI_CERTIFICATE",
            "SSL_RESTAPI_PRIVATE_KEY", "PGW_REST_PASSWORD_HASH",
            "SPILO_CONFIGURATION", "DOORMAN_CONFIG",
        ];

        // Act: сборка env по новому контракту.
        var env = SpiloEnvBuilder.Build(Topology, Etcd, syncStrict: false, "shard1a", CaPem);

        // Assert: ни одного запрещённого ключа; публичный CA на месте.
        env.Keys.Should().NotContain(forbiddenKeys, "node env carries no secrets (spec §3.1)");
        env.Keys.Should().Contain("SSL_RESTAPI_CA");
    }

    [Fact]
    public void Doorman_TemplateCarriesPlaceholder_NoRealPassword()
    {
        // Arrange: шаблон конфига пулера (неконфиденциальный).

        // Act: генерируем шаблон без пароля в сигнатуре.
        var doorman = DoormanConfigBuilder.Build("shop", 55);

        // Assert: литеральный плейсхолдер ${PGW_SU_PASSWORD} на месте
        // auth_query-пароля; никаких реальных секретов в шаблоне нет;
        // TLS-файлы — tmpfs-пути рендера агента (spec §3.5).
        doorman.Should().Contain("password: \"${PGW_SU_PASSWORD}\"");
        doorman.Should().NotContain("\"su\"");
        doorman.Should().Contain("tls_certificate: \"/run/pgw/doorman/tls.crt\"");
        doorman.Should().Contain("tls_private_key: \"/run/pgw/doorman/tls.key\"");
    }

    [Fact]
    public void SpiloEnv_NoAppPasswordLeak()
    {
        // Arrange
        var topology = new ShardTopology("shop", "shard1", "shop-shard1",
            new Dictionary<string, NodeAddress>
            {
                ["shard1a"] = new("h1", new NodePorts(15432, 18008, 16432)),
            });

        // Act
        var env = SpiloEnvBuilder.Build(topology, new EtcdEndpoints(["http://etcd:2379"]), syncStrict: false, "shard1a", CaPem);

        // Assert — app-пароль в env контейнера не попадает (spec §2.4, критерий 6);
        // t05: bucket-пароли из env удалены (доставка не нужна — потребителя нет).
        env.Keys.Should().NotContain("PGW_APP_PASSWORD");
        env.Keys.Should().NotContain("PGW_BUCKET_ADMIN_PASSWORD");
        env.Keys.Should().NotContain("PGW_BUCKET_ADMIN_USER");
    }

    [Fact]
    public void Doorman_SingleTransactionPoolWithBudget()
    {
        // Arrange: dbname = имя кластера, бюджет 55 серверных соединений.

        // Act: генерируем шаблон пулера.
        var config = DoormanConfigBuilder.Build("shop", 55);

        // Assert: единственный пул shop → 127.0.0.1:5432, transaction-режим,
        // бюджет 55 серверных соединений (P13/P14/P15); YAML фактического
        // pg_doorman (general/pools).
        config.Should().Contain("pool_mode: \"transaction\"");
        config.Should().Contain("max_db_connections: 55");
        config.Should().Contain("auth_query:");
        config.Should().Contain("user: \"postgres\"");
        config.Should().Contain("pools:");
        config.Should().Contain("  \"shop\":");
        config.Should().Contain("server_host: \"127.0.0.1\"");
        config.Should().Contain("server_port: 5432");
        config.Should().Contain("server_database: \"shop\"");
    }

    [Fact]
    public void SpiloEnv_Tuning_MergesPgTuneWithCanon()
    {
        // Arrange: tuning от входа oltp/8GiB/4cpu/60/ssd/mid_ram + dw-вариант
        // для проверки перезаписи каноном.

        // Act: сборка bootstrap-ключа с merge(PGTune ∪ канон) — exclude по умолчанию.
        var tuning = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
            4, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));
        var spilo = SpiloFromBootstrap(tuning, DefaultExclude);

        // Assert: PGTune-параметры в YAML (в кавычках, стиль текущего блока).
        spilo.Should().Contain("max_connections: \"60\"");
        spilo.Should().Contain("shared_buffers: \"2GB\"");
        spilo.Should().Contain("effective_cache_size: \"6GB\"");
        spilo.Should().Contain("checkpoint_completion_target: \"0.9\"");

        // Assert: канон PgWorker «поверх» — P3 и лог-блок на месте.
        spilo.Should().Contain("wal_level: logical");
        spilo.Should().Contain("max_wal_senders: \"10\"");
        spilo.Should().Contain("sync_replication_slots: \"on\"");
        spilo.Should().Contain("hot_standby: \"on\"");
        spilo.Should().Contain("wal_keep_size: \"2048MB\"");
        spilo.Should().Contain("checkpoint_timeout: \"15min\"");
        spilo.Should().Contain("logging_collector: \"on\"");

        // Assert: порядок PGTune-параметров = §5.2 (индексы возрастают),
        // канон-ключи после PGTune-блока (новые ключи — в конец). Зонды —
        // с отступом строки (\n + 8 пробелов), чтобы «work_mem:» не матчился
        // внутрь «maintenance_work_mem:».
        var order = new[]
        {
            "max_connections:", "shared_buffers:", "effective_cache_size:",
            "maintenance_work_mem:", "checkpoint_completion_target:", "wal_buffers:",
            "default_statistics_target:", "random_page_cost:", "effective_io_concurrency:",
            "work_mem:", "huge_pages:", "wal_compression:", "min_wal_size:",
            "max_wal_size:", "max_worker_processes:",
        };
        var indices = order.Select(k => spilo.IndexOf("\n        " + k, StringComparison.Ordinal)).ToList();
        indices.Should().OnlyContain(i => i >= 0);
        indices.Should().BeInAscendingOrder();
        spilo.IndexOf("\n        max_parallel_workers_per_gather:", StringComparison.Ordinal)
            .Should().BeGreaterThan(indices[^1]);
        spilo.IndexOf("\n        wal_level:", StringComparison.Ordinal)
            .Should().BeGreaterThan(spilo.IndexOf("\n        max_worker_processes:", StringComparison.Ordinal));

        // Assert: дубликатов ключей нет (канон перезаписывает, а не добавляет).
        spilo.Split('\n').Count(l => l.TrimStart().StartsWith("wal_level:", StringComparison.Ordinal))
            .Should().Be(1);

        // Act: dbType=dw — wal_level всё равно logical (канон перекрывает PGTune-ветвь).
        var dwTuning = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Dw, 8388608, PgTuneMemoryUnit.KB,
            4, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));
        var dwSpilo = SpiloFromBootstrap(dwTuning, DefaultExclude);

        // Assert: wal_level: logical сохранён при dw.
        dwSpilo.Should().Contain("wal_level: logical");
    }

    [Fact]
    public void SpiloEnv_Tuning_ExcludedParametersAreNotWritten()
    {
        // Arrange: не-io_uring-вход (Windows → io_method=worker, io_workers=18),
        // чтобы оба параметра были бы выведены без exclude.

        // Act: сборка bootstrap-ключа c exclude {io_method, io_workers}.
        var tuning = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Windows, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
            72, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));
        tuning["io_method"].Should().Be("worker");
        tuning["io_workers"].Should().Be("18");
        var spilo = SpiloFromBootstrap(tuning, DefaultExclude);

        // Assert: исключённые параметры в YAML отсутствуют вовсе (никаких
        // пустых значений), остальные PGTune-параметры на месте.
        spilo.Should().NotContain("io_method");
        spilo.Should().NotContain("io_workers");
        spilo.Should().Contain("shared_buffers: \"2GB\"");
        spilo.Should().Contain("max_connections: \"60\"");
    }

    [Fact]
    public void Doorman_BudgetSynchronizedWithMaxConnections()
    {
        // Arrange: tuning с max_connections 60/40 и синтетический с 14 (пол 10).

        // Act: бюджеты от рассчитанного результата.
        var at60 = DoormanConfigBuilder.ServerConnections(PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
            null, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam)));
        var at40 = DoormanConfigBuilder.ServerConnections(PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
            null, 40, PgTuneHdType.Ssd, PgTuneDbSize.MidRam)));
        var atFloor = DoormanConfigBuilder.ServerConnections(
            new PgTuneResult([new PgTuneParameter("max_connections", "14")], []));

        // Assert: max(10, max_connections − 5) — 55 при 60 (инвариант P15),
        // 35 при 40, пол 10 при 14.
        at60.Should().Be(55);
        at40.Should().Be(35);
        atFloor.Should().Be(10);

        // Assert: шаблон пулера несёт вычисленный бюджет.
        DoormanConfigBuilder.Build("shop", 35).Should().Contain("max_db_connections: 35");
    }

    [Fact]
    public void SpiloEnv_TuningNull_KeepsCanonicalHardcodedSet()
    {
        // Arrange/Act: tuning == null — прежний путь (изолированные пути/тесты).

        // Assert: хардкод-набор не изменён (константы канона остаются в нём).
        var spilo = SpiloFromBootstrap();
        spilo.Should().Contain("max_connections: \"60\"");
        spilo.Should().Contain("shared_buffers: \"2GB\"");
        spilo.Should().Contain("random_page_cost: \"1.1\"");
        spilo.Should().Contain("# P15: 55 pg_doorman + 2 admin/mover + 3 reserved");
    }

    // AAA (spec §3.4/§6.3): strict из параметра попадает в bootstrap.dcs
    // SPILO_CONFIGURATION рядом с synchronous_mode; false сохраняет прежний текст.
    [Theory]
    [InlineData(true, "synchronous_mode_strict: true")]
    [InlineData(false, "synchronous_mode_strict: false")]
    public void Build_SyncStrict_GoesToBootstrapDcs(bool syncStrict, string expected)
    {
        // Arrange — топология/etcd фикстуры файла.
        // Act
        var spilo = SpiloFromBootstrap(syncStrict: syncStrict);

        // Assert
        spilo.Should().Contain(expected).And.Contain("synchronous_mode: true");
    }

    // AAA (t05, spec §3.3/§3.7.1): bootstrap-JSON несёт SPILO_CONFIGURATION
    // (с rest_password), серт и ключ; per-install пароли PG в него НЕ входят
    // (Д7 — прямая mTLS-передача); сериализация симметрична (spec §3.7.2).
    [Fact]
    public void Bootstrap_ContainsSpiloConfigurationCertAndKey()
    {
        // Arrange: тестовые PEM-пары и per-install пароли PG (в ключ НЕ входят).
        const string certPem = "CERTPEM";
        const string keyPem = "KEYPEM";
        const string suPassword = "su-test-password";
        const string standbyPassword = "standby-test-password";

        // Act: построение JSON bootstrap-ключа.
        var json = NodeBootstrapBuilder.Build(Topology, Etcd, syncStrict: false,
            "shard1a", certPem, keyPem, RestPassword, null, null);

        // Assert: JSON парсится; корень несёт spilo_configuration (с restapi
        // и rest_password), cert_pem/key_pem байт-в-байт; пароли PG в JSON нет;
        // повторный вызов — идентичная строка (симметрия).
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        root.GetProperty("spilo_configuration").GetString().Should()
            .Contain("restapi:").And.Contain($"password: \"{RestPassword}\"");
        root.GetProperty("cert_pem").GetString().Should().Be(certPem);
        root.GetProperty("key_pem").GetString().Should().Be(keyPem);
        json.Should().NotContain(suPassword).And.NotContain(standbyPassword);
        var again = NodeBootstrapBuilder.Build(Topology, Etcd, syncStrict: false,
            "shard1a", certPem, keyPem, RestPassword, null, null);
        again.Should().Be(json, "serialization is symmetric — same input, byte-identical output");
    }
}
