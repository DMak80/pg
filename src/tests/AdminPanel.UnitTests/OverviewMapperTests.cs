using AdminPanel.Core;
using AdminPanel.Api.Inspection;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// OverviewMapper (u06–u08, D6): ha/backups/reliability/operations/freshness —
// перенос TS-агрегации OverviewPage 1:1 («те же входы → те же числа»).
// AAA.
public class OverviewMapperTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.FromUnixTimeSeconds(1761900600);

    // ——— ha ———

    [Fact]
    public void MapHa_MatchedWithoutLeader_CountsOnlyMatched()
    {
        // Arrange — 4 scope: 2 matched с лидером, 1 matched без, 1 unmatched
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            HaScopes =
            [
                new HaScope("demo-s1", "demo", "s1", true, "n1", null, true, null, null, null, [], null),
                new HaScope("demo-s2", "demo", "s2", true, null, null, true, null, null, null, [], null),
                new HaScope("demo-s3", "demo", "s3", true, "n3", null, true, null, null, null, [], null),
                new HaScope("other-x", null, null, false, null, null, true, null, null, null, [], null),
            ],
        };

        // Act
        var ha = OverviewMapper.MapHa(snapshot);

        // Assert — TS: withoutLeader только matched; unmatched отдельно
        ha.ScopesTotal.Should().Be(4);
        ha.WithoutLeader.Should().Be(1);
        ha.Unmatched.Should().Be(1);
    }

    // ——— backups ———

    [Fact]
    public void MapBackups_NotConfigured_NullsHealth()
    {
        // Arrange — грань не настроена (S3Storage null), etcd-ключ места есть
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            BackupStorage = new BackupStorageInfo(1000, 2000, 50, BackupStorageState.Warn, 1761900000),
            EtcdSnapshots = new EtcdSnapshotExportInfo(true, "OK", 1761900000, null, null, null, null, null),
        };

        // Act
        var backups = OverviewMapper.MapBackups(snapshot);

        // Assert — configured=false (S3-инвентаря нет), health null, место/выгрузка с сервера
        backups.Configured.Should().BeFalse();
        backups.Health.Should().BeNull();
        backups.StorageState.Should().Be("WARN");
        backups.UsedBytes.Should().Be(1000);
        backups.QuotaBytes.Should().Be(2000);
        backups.EtcdSnapshots.Should().NotBeNull();
        backups.EtcdSnapshots!.Enabled.Should().BeTrue();
        backups.EtcdSnapshots.State.Should().Be("OK");
    }

    [Fact]
    public void MapBackups_Configured_CarriesHealthFromS3Inventory()
    {
        // Arrange — live-инвентарь с health (cluster n/a вне ownS3-контура)
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            S3Storage = TestS3Storage(health: new S3Health(true, null, true, false, null)),
            BackupStorage = new BackupStorageInfo(500, null, 25, BackupStorageState.Ok, 1761900000),
        };

        // Act
        var backups = OverviewMapper.MapBackups(snapshot);

        // Assert
        backups.Configured.Should().BeTrue();
        backups.Health!.ApiOk.Should().BeTrue();
        backups.Health.LiveOk.Should().BeTrue();
        backups.Health.ClusterOk.Should().BeFalse();
        backups.StorageState.Should().Be("OK");
        backups.QuotaBytes.Should().BeNull();
    }

    // ——— reliability ———

    [Fact]
    public void MapReliability_SameInputsAsTs_SameNumbers()
    {
        // Arrange — те же входы, что давала TS-агрегация: Active-кластер demo
        // (s1 — wal-поток свежий → Wal-потенциал = возраст хвоста 60;
        //  s2 — wal-ключа нет → Full-потенциал = возраст полного now−(now−200)=200);
        // Active-кластер keep без записей бэкапов → Off; NOT_INITIALIZED-кластер
        // nope (тоже Off по калькулятору) НЕ входит в счётчики и worst.
        var now = Now.ToUnixTimeSeconds();
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            Clusters =
            [
                TestSnapshots.FullCluster() with
                {
                    // s2 — шард с COMPLETED-полным, но БЕЗ wal-ключа → Full-режим
                    Shards =
                    [
                        .. TestSnapshots.FullCluster().Shards,
                        new ShardInfo("s2", "host=s2a port=5432 dbname=demo", ["s2a"], 5432, "demo",
                            "bucket_admin", 1, null, [], null),
                    ],
                },
                new ClusterInfo("keep", "keep", 1, now, ClusterState.Active,
                    [new ShardInfo("s1", "host=k1 port=5432 dbname=keep", ["k1"], 5432, "keep",
                        "bucket_admin", 1, "k1:5432", [], null)],
                    [], [], false),
                new ClusterInfo("nope", "nope", 1, now, ClusterState.NotInitialized,
                    [new ShardInfo("s1", "host=n1 port=5432 dbname=nope", ["n1"], 5432, "nope",
                        "bucket_admin", 1, "n1:5432", [], null)],
                    [], [], false),
            ],
            Backups =
            [
                new ClusterBackupsInfo("demo", 86400,
                    new Dictionary<string, long?> { ["s1"] = now - 100, ["s2"] = now - 200 },
                    new Dictionary<string, WalStreamInfo?>
                    {
                        ["s1"] = new WalStreamInfo("demo", "s1", WalStreamInfoState.Active, "slot1", "n1",
                            now - 60, 1, null, "seg", null),
                    }),
            ],
            WorkerHealth = [],
        };

        // Act
        var reliability = OverviewMapper.MapReliability(snapshot, Now);

        // Assert — числа совпадают с ручным пересчётом TS-логики:
        // worst = max(60, 200) = 200 (active-only), fullOnly = 1 (s2), off = 1 (keep)
        reliability.WorstRpoPotentialSec.Should().Be(200);
        reliability.FullOnlyCount.Should().Be(1);
        reliability.OffCount.Should().Be(1);
        reliability.LastFailover.Should().BeNull();
    }

    [Fact]
    public void MapReliability_LastFailover_MostRecentClosedOnly()
    {
        // Arrange — два закрытых failover + один открытый (без durationSec):
        // TS берёт max по resolvedUnix ?? detectedUnix среди закрытых.
        var now = Now.ToUnixTimeSeconds();
        var snapshot = TestSnapshots.Healthy(Now) with
        {
            PgWorkerWork =
            [
                new WorkJournalInfo("demo", "supervise", "supervising", "w1", now, null, null, null, null,
                    new HaSupervisionInfo("s1", "n1", "crash", now - 600, now - 500, 100),
                    new HaSupervisionInfo("s2", "n2", "crash", now - 3000, now - 2500, 500)),
                new WorkJournalInfo("demo2", "supervise", "supervising", "w1", now, null, null, null, null,
                    new HaSupervisionInfo("s1", "n1", "crash", now - 60, null, null), // открытый
                    null),
            ],
        };

        // Act
        var reliability = OverviewMapper.MapReliability(snapshot, Now);

        // Assert — самый недавний закрытый: s1 (resolved 500 с назад), не s2 (2500)
        reliability.LastFailover.Should().NotBeNull();
        reliability.LastFailover!.DurationSec.Should().Be(100);
        reliability.LastFailover.AtUnix.Should().Be(now - 500);
    }

    // ——— operations ———

    [Fact]
    public void MapOperations_CoversAllDomains_OnePerSource()
    {
        // Arrange — по одной операции на домен
        var pg = TestSnapshots.Healthy(Now) with
        {
            Clusters = [TestSnapshots.FullCluster() with
            {
                Buckets =
                [
                    new BucketInfo(0, "shard1", BucketState.Syncing,
                        new MoveInfo("shard1", "shard2", 1761900000, 1761900010, null, null)),
                    new BucketInfo(1, "shard1", BucketState.Active, null),
                ],
            }],
            MoveTickets = [new MoveTicket("demo", "bucket_2", 2, "move", "shard2", 1761900020, "ops")],
            PgRotations = [new PgRotationTicket("demo", 1761900030, "ops")],
        };
        var kafka = new AdminPanel.Core.Kafka.KafkaSnapshot(
            Now, true, 0,
            [new AdminPanel.Core.Kafka.KafkaClusterInfo("events",
                AdminPanel.Core.Kafka.KafkaClusterState.Active, 3, 3, 2, 12, 604800000, null, null, [], [],
                [new AdminPanel.Core.Kafka.KafkaTopicLifecycleTicket("t1", "create", 1, null, null, null, 1761900040, "ops")])],
            [new AdminPanel.Core.Kafka.KafkaRotationTicket("events", 1761900050, "ops")],
            [], [], [], [], [], [], [], [], 0);
        var valkey = new AdminPanel.Core.Valkey.ValkeySnapshot(
            Now, true, 0, [],
            [new AdminPanel.Core.Valkey.ValkeyRotationTicket("cache", "admin", 1761900060, "ops")],
            [], [], [], [], [], 0);
        var s3 = new AdminPanel.Core.S3.S3Snapshot(
            Now, true, 0, [],
            [new AdminPanel.Core.S3.S3RotationTicket("store", 1761900070, "ops")],
            [], [], [], [], 0);

        // Act
        var operations = OverviewMapper.MapOperations(pg, kafka, valkey, s3);

        // Assert — домен×kind-словарь: pg move/move-ticket/rotation, kafka
        // rotation/lifecycle, valkey rotation, s3 rotation
        operations.Should().Contain(o => o.Domain == "pg" && o.Kind == "move" && o.Entity == "0");
        operations.Should().Contain(o => o.Domain == "pg" && o.Kind == "move-ticket" && o.Detail == "move");
        operations.Should().Contain(o => o.Domain == "pg" && o.Kind == "rotation" && o.Cluster == "demo");
        operations.Should().Contain(o => o.Domain == "kafka" && o.Kind == "rotation");
        operations.Should().Contain(o => o.Domain == "kafka" && o.Kind == "lifecycle" && o.Entity == "t1");
        operations.Should().Contain(o => o.Domain == "valkey" && o.Kind == "rotation" && o.Detail == "admin");
        operations.Should().Contain(o => o.Domain == "s3" && o.Kind == "rotation" && o.Cluster == "store");
    }

    // ——— freshness ———

    [Fact]
    public void MapFreshness_NullDomains_NullsEtcdFromLastRefresh()
    {
        // Arrange — kafka/valkey/s3 не тикали
        var pg = TestSnapshots.Healthy(Now);

        // Act
        var freshness = OverviewMapper.MapFreshness(pg, null, null, null);

        // Assert — etcd из LastRefreshUtc, pg из BuiltAtUtc, остальные null
        freshness.Etcd!.BuiltAtUnix.Should().Be(pg.Etcd.LastRefreshUtc.ToUnixTimeSeconds());
        freshness.Pg!.BuiltAtUnix.Should().Be(pg.BuiltAtUtc.ToUnixTimeSeconds());
        freshness.Kafka.Should().BeNull();
        freshness.Valkey.Should().BeNull();
        freshness.S3.Should().BeNull();
    }

    // ——— вспомогательные ———

    private static S3StorageInfo TestS3Storage(S3Health? health) => new(
        true, ["http://s3:9000"], "backups", health, ["demo"], 0, 0, [], [], 1761900000, 0, null);
}
