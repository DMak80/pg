using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;

namespace PgWorker.UnitTests.Templates;

// NodeConfigBuilders: детерминированные конфиги контейнера pgworker-node
// (Spilo env, pg_doorman, haproxy) из arch/configs + параметров spec (P2/P3/P11/P13/P14/P15/P17, Д4);
// PGTune (spec.md §4.4): merge(PGTune ∪ канон) в SPILO_CONFIGURATION, exclude,
// doorman-бюджет от рассчитанного max_connections.

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

    private static readonly InstallSecrets Secrets = new(
        "su-secret", "standby-secret", "admin-secret", "mover-secret");

    // PGTune-вывод по умолчанию exclude (опции PgWorker:Pgtune по умолчанию).
    private static readonly HashSet<string> DefaultExclude =
        new(["io_method", "io_workers"], StringComparer.Ordinal);

    [Fact]
    public void SpiloEnv_ContainsPatroniAndPgParameters()
    {
        // Arrange: топология шарда из 2 нод и адреса etcd.

        // Act: генерируем env контейнера ноды.
        var env = SpiloEnvBuilder.Build(Topology, Etcd, Secrets);

        // Assert: SPILO_CONFIGURATION несёт канон таймингов (t09: полы Patroni
        // 4.x — ttl=20/loop_wait=1/retry_timeout=3), P3 (wal_level),
        // P15 (max_connections) и callback мастер-ключа.
        var spilo = env["SPILO_CONFIGURATION"];
        spilo.Should().Contain("ttl: 20");
        spilo.Should().Contain("loop_wait: 1");
        spilo.Should().Contain("retry_timeout: 3");
        spilo.Should().Contain("wal_level: logical");
        spilo.Should().Contain("max_connections: \"60\"");
        spilo.Should().Contain("sync_replication_slots: \"on\"");
        spilo.Should().Contain("on_role_change");
    }

    [Fact]
    public void SpiloEnv_ContainsScopeAndEtcdHosts()
    {
        // Arrange: scope = "<C>-<X>" и список etcd-эндпоинтов.

        // Act: генерируем env контейнера ноды.
        var env = SpiloEnvBuilder.Build(Topology, Etcd, Secrets);

        // Assert: идентификация Patroni-кластера и адреса DCS на месте.
        env["SCOPE"].Should().Be("shop-shard1");
        env["ETCD3_HOSTS"].Should().Be("e1:2379,e2:2379"); // Patroni: host:port без scheme (etcd v3)
        env["PGW_ETCD"].Should().Be("http://e1:2379,http://e2:2379");
        env["PGW_MASTER_KEY"].Should().Be("/clusters/shop/shards/shard1/master");
    }

    [Fact]
    public void Secrets_AreInEnv_ButNeverInDoormanOrHaproxy()
    {
        // Arrange: секреты установки (Д7).

        // Act: генерируем все три конфига.
        var env = SpiloEnvBuilder.Build(Topology, Etcd, Secrets);
        var doorman = DoormanConfigBuilder.Build("shop", 55);
        var haproxy = HaproxyConfigBuilder.Build(Topology);

        // Assert: секреты прокинуты в env контейнера (Spilo/bootstrap),
        // но не попадают в тексты doorman/haproxy (их читаютvolume-маунты).
        env.Values.Should().Contain(new[] { "su-secret", "standby-secret", "admin-secret", "mover-secret" });
        doorman.Should().NotContainAny("su-secret", "standby-secret", "admin-secret", "mover-secret");
        haproxy.Should().NotContainAny("su-secret", "standby-secret", "admin-secret", "mover-secret");
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
        var secrets = new InstallSecrets("su", "sb", "adm", "mov");

        // Act
        var env = SpiloEnvBuilder.Build(topology, new EtcdEndpoints(["http://etcd:2379"]), secrets);

        // Assert — app-пароль в env контейнера не попадает (spec §2.4, критерий 6);
        // bucket_admin-механизм env не тронут
        env.Keys.Should().NotContain("PGW_APP_PASSWORD");
        env.Keys.Should().Contain("PGW_BUCKET_ADMIN_PASSWORD");
        env.Keys.Should().Contain("PGW_BUCKET_ADMIN_USER");
    }

    [Fact]
    public void Doorman_SingleTransactionPoolWithBudget()
    {
        // Arrange: dbname = имя кластера, бюджет 55 серверных соединений.

        // Act: генерируем конфиг пулера.
        var config = DoormanConfigBuilder.Build("shop", 55);

        // Assert: единственный пул shop → 127.0.0.1:5432, transaction-режим,
        // бюджет 55 серверных соединений (P13/P14/P15).
        config.Should().Contain("pool_mode = \"transaction\"");
        config.Should().Contain("max_db_connections = 55");
        config.Should().Contain("shop = host=127.0.0.1 port=5432 dbname=shop");
    }

    [Fact]
    public void SpiloEnv_Tuning_MergesPgTuneWithCanon()
    {
        // Arrange: tuning от входа oltp/8GiB/4cpu/60/ssd/mid_ram + dw-вариант
        // для проверки перезаписи каноном.

        // Act: сборка env с merge(PGTune ∪ канон) — exclude по умолчанию.
        var tuning = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Linux, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
            4, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));
        var spilo = SpiloEnvBuilder.Build(Topology, Etcd, Secrets, tuning, DefaultExclude)["SPILO_CONFIGURATION"];

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
        var dwSpilo = SpiloEnvBuilder.Build(Topology, Etcd, Secrets, dwTuning, DefaultExclude)["SPILO_CONFIGURATION"];

        // Assert: wal_level: logical сохранён при dw.
        dwSpilo.Should().Contain("wal_level: logical");
    }

    [Fact]
    public void SpiloEnv_Tuning_ExcludedParametersAreNotWritten()
    {
        // Arrange: не-io_uring-вход (Windows → io_method=worker, io_workers=18),
        // чтобы оба параметра были бы выведены без exclude.

        // Act: сборка env c exclude {io_method, io_workers}.
        var tuning = PgTune.Calculate(new PgTuneInput(
            18, PgTuneOsType.Windows, PgTuneDbType.Oltp, 8388608, PgTuneMemoryUnit.KB,
            72, 60, PgTuneHdType.Ssd, PgTuneDbSize.MidRam));
        tuning["io_method"].Should().Be("worker");
        tuning["io_workers"].Should().Be("18");
        var spilo = SpiloEnvBuilder.Build(Topology, Etcd, Secrets, tuning, DefaultExclude)["SPILO_CONFIGURATION"];

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

        // Assert: конфиг пулера несёт вычисленный бюджет.
        DoormanConfigBuilder.Build("shop", 35).Should().Contain("max_db_connections = 35");
    }

    [Fact]
    public void SpiloEnv_TuningNull_KeepsCanonicalHardcodedSet()
    {
        // Arrange/Act: tuning == null — прежний путь (изолированные пути/тесты).

        // Assert: хардкод-набор не изменён (константы канона остаются в нём).
        var spilo = SpiloEnvBuilder.Build(Topology, Etcd, Secrets)["SPILO_CONFIGURATION"];
        spilo.Should().Contain("max_connections: \"60\"");
        spilo.Should().Contain("shared_buffers: \"2GB\"");
        spilo.Should().Contain("random_page_cost: \"1.1\"");
        spilo.Should().Contain("# P15: 55 pg_doorman + 2 админ/mover + 3 reserved");
    }

    [Fact]
    public void Haproxy_WriteFrontendWithAllNodesAndPatroniChecks()
    {
        // Arrange: шард из 2 нод на разных хостах с выделенными портами.

        // Act: генерируем конфиг HAProxy.
        var config = HaproxyConfigBuilder.Build(Topology);

        // Assert: write-фронтенд :5432, health-check Patroni /primary (P2),
        // все ноды шарда в бэкенде с check-port Patroni.
        config.Should().Contain("bind *:5432");
        config.Should().Contain("option httpchk GET /primary");
        config.Should().Contain("http-check expect status 200");
        config.Should().Contain("server shard1a h1:15432 check port 18008");
        config.Should().Contain("server shard1b h2:15433 check port 18009");
    }
}
