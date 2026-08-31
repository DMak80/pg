using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Core.Kafka.KafkaAlerting;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Кластерные kinds каталога kafka-алертов (arch/03 §7.4; план B2): каждый kind,
// sinceUnix по стабильному id и fresh-PROVISIONING-окно брокера.
public class KafkaAlertRulesTests
{
    private static readonly DateTimeOffset Now = new(2026, 8, 30, 12, 0, 0, TimeSpan.Zero);
    private static readonly long NowUnix = Now.ToUnixTimeSeconds();

    private readonly KafkaAlertEngine _engine =
        new(Options.Create(new KafkaAlertsOptions()));

    // Оценка: снапшот + необязательный предыдущий (механика refresher).
    private IReadOnlyList<Alert> Evaluate(KafkaSnapshot next, KafkaSnapshot? prev = null)
        => [.. _engine.Evaluate(next, prev)];

    private static KafkaSnapshot Snapshot(params KafkaClusterInfo[] clusters) => new(
        Now, EtcdReachable: true, ConsecutiveFailures: 0,
        [.. clusters], Rotations: [], Rebalances: [], Reassignments: [],
        WorkerEndpoints: [], Probes: [], Alerts: [], ParseErrors: [], UnknownKeyCount: 0);

    // Active-кластер с брокерами (по умолчанию один RUNNING broker1).
    private static KafkaClusterInfo ActiveCluster(
        string name = "events",
        string? endpoints = "host.docker.internal:16001",
        KafkaBrokerInfo[]? brokers = null)
    {
        brokers ??= [Broker("broker1")];
        return new KafkaClusterInfo(
            name, KafkaClusterState.Active,
            Brokers: brokers.Length, ReplicationFactor: 3, MinInSyncReplicas: 2, DefaultPartitions: 12,
            DefaultRetentionMs: 604800000, CreatedUnix: 1756500000, Endpoints: endpoints,
            BrokersList: [.. brokers], Topics: []);
    }

    private static KafkaClusterInfo PendingCluster(string name = "pending")
        => new(
            name, KafkaClusterState.NotInitialized, 3, 3, 2, 12, 604800000,
            1756500000, null, [], []);

    private static KafkaBrokerInfo Broker(string name, string? state = "RUNNING", string? role = "controller")
        => new(name, state, role, 2m, 4, 40);

    private static Alert AlertOf(string id, long sinceUnix) => new(
        id, AlertSeverity.Critical, id[..id.IndexOf(':')], "", "", null, sinceUnix);

    // ===== Волна C (план C3): missing-desired / stale / under-replicated / lag-high =====

    private static KafkaTopicInfo Topic(
        string name = "orders",
        bool missing = false,
        TopicDesiredDto? desired = null,
        int? underReplicated = null) => new(
        name, 3, 1, 604800000, 1, desired, missing, 1756500900, underReplicated);

    private static KafkaClusterInfo ActiveWithTopics(
        KafkaTopicInfo[] topics, KafkaGroupInfo[]? groups = null)
        => new(
            "events", KafkaClusterState.Active, 1, 1, 1, 12, 604800000, 1756500000,
            "host.docker.internal:16001",
            [new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40)],
            [.. topics],
            Groups: groups);

    [Fact]
    public void TopicMissing_WithLiveDesired_WarningAlert()
    {
        // Arrange
        var snapshot = Snapshot(ActiveWithTopics(
        [
            Topic("ghost", missing: true, desired: new TopicDesiredDto(null, 86400000, null, NowUnix - 30, "admin")),
        ]));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().Contain(a =>
            a.Kind == "kafka-topic-missing-desired" && a.Target == "events/ghost"
            && a.Severity == AlertSeverity.Warning);
    }

    [Fact]
    public void DesiredStale_AfterThreshold_WarningAlert()
    {
        // Arrange: заявка висит дольше 600 c (дефолт StaleDesiredSeconds).
        var snapshot = Snapshot(ActiveWithTopics(
        [
            Topic("orders", desired: new TopicDesiredDto(6, null, null, NowUnix - 601, "admin")),
        ]));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().Contain(a => a.Kind == "kafka-desired-stale" && a.Target == "events/orders");
    }

    [Fact]
    public void DesiredFresh_NoStaleAlert()
    {
        // Arrange: заявка молодая (30 c).
        var snapshot = Snapshot(ActiveWithTopics(
        [
            Topic("orders", desired: new TopicDesiredDto(6, null, null, NowUnix - 30, "admin")),
        ]));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().NotContain(a => a.Kind == "kafka-desired-stale");
    }

    [Fact]
    public void TopicUnderReplicated_FromRuntime_WarningAlert()
    {
        // Arrange: runtime-USR из пробы (refresher мерджит в кластер).
        var snapshot = Snapshot(ActiveWithTopics([Topic("orders", underReplicated: 2)]));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().Contain(a =>
            a.Kind == "kafka-topic-under-replicated" && a.Target == "events/orders"
            && a.Details!["underReplicatedPartitions"] == "2");
    }

    [Fact]
    public void GroupLag_AboveThreshold_WarningAlert()
    {
        // Arrange: лаг 150000 > порога 100000 (дефолт GroupLagMessages).
        var snapshot = Snapshot(ActiveWithTopics(
            [],
            groups: [new KafkaGroupInfo("lag-test", "Stable", 2, 150_000)]));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().Contain(a =>
            a.Kind == "kafka-group-lag-high" && a.Target == "events/lag-test"
            && a.Details!["totalLag"] == "150000");
    }

    [Fact]
    public void GroupLag_BelowThreshold_NoAlert()
    {
        // Arrange
        var snapshot = Snapshot(ActiveWithTopics(
            [],
            groups: [new KafkaGroupInfo("ok-group", "Stable", 1, 99_999)]));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().NotContain(a => a.Kind == "kafka-group-lag-high");
    }

    [Fact]
    public void NotInitializedCluster_InfoAlert()
    {
        // Arrange: заявленный, но не поднятый кластер.
        var snapshot = Snapshot(PendingCluster());

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("kafka-cluster-not-initialized");
        alert.Severity.Should().Be(AlertSeverity.Info);
        alert.Id.Should().Be("kafka-cluster-not-initialized:pending");
        alert.Target.Should().Be("pending");
    }

    [Fact]
    public void ToRemoveCluster_InfoAlert()
    {
        // Arrange: кластер в удалении.
        var snapshot = Snapshot(PendingCluster() with
        {
            Name = "events",
            State = KafkaClusterState.ToRemove,
        });

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle().Subject;
        alert.Kind.Should().Be("kafka-cluster-to-remove");
        alert.Severity.Should().Be(AlertSeverity.Info);
        alert.Id.Should().Be("kafka-cluster-to-remove:events");
    }

    [Fact]
    public void NotRunningBroker_CriticalAlert()
    {
        // Arrange: Active-кластер, broker2 UNREACHABLE.
        var next = Snapshot(ActiveCluster(brokers:
            [Broker("broker1"), Broker("broker2", "UNREACHABLE")]));

        // Act
        var alerts = Evaluate(next);

        // Assert
        var alert = alerts.Should().ContainSingle(a => a.Kind == "kafka-broker-not-running").Subject;
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Id.Should().Be("kafka-broker-not-running:events/broker2");
        alert.Details!["state"].Should().Be("UNREACHABLE");
    }

    [Fact]
    public void FreshProvisioningBroker_NotAlerted()
    {
        // Arrange: broker2 только что стал PROVISIONING (в prev его не было).
        var prev = Snapshot(ActiveCluster(brokers: [Broker("broker1")]));
        var next = Snapshot(ActiveCluster(brokers:
            [Broker("broker1"), Broker("broker2", "PROVISIONING")]));

        // Act
        var alerts = Evaluate(next, prev);

        // Assert
        alerts.Should().NotContain(a => a.Kind == "kafka-broker-not-running");
    }

    [Fact]
    public void ProvisioningOlderThanWindow_Alerted()
    {
        // Arrange: broker2 PROVISIONING уже был в prev, тик назад = 61 c — окно сгорело.
        var prev = Snapshot(ActiveCluster(brokers:
            [Broker("broker1"), Broker("broker2", "PROVISIONING")]))
            with { BuiltAtUtc = Now.AddSeconds(-61) };
        var next = Snapshot(ActiveCluster(brokers:
            [Broker("broker1"), Broker("broker2", "PROVISIONING")]));

        // Act
        var alerts = Evaluate(next, prev);

        // Assert: PROVISIONING держится дольше окна 60 c — critical.
        var alert = alerts.Should().ContainSingle(a => a.Kind == "kafka-broker-not-running").Subject;
        alert.Details!["state"].Should().Be("PROVISIONING");
    }

    [Fact]
    public void ProvisioningWithinWindow_NotAlerted()
    {
        // Arrange: PROVISIONING наблюдался один тик назад (3 c) — fresh.
        var prev = Snapshot(ActiveCluster(brokers:
            [Broker("broker1"), Broker("broker2", "PROVISIONING")]))
            with { BuiltAtUtc = Now.AddSeconds(-3) };
        var next = Snapshot(ActiveCluster(brokers:
            [Broker("broker1"), Broker("broker2", "PROVISIONING")]));

        // Act
        var alerts = Evaluate(next, prev);

        // Assert
        alerts.Should().NotContain(a => a.Kind == "kafka-broker-not-running");
    }

    [Fact]
    public void NotRunningBroker_FirstEvaluation_NoSinceUnix()
    {
        // Arrange: prev нет — алерт новый.
        var next = Snapshot(ActiveCluster(brokers: [Broker("broker1", "UNREACHABLE")]));

        // Act
        var alerts = Evaluate(next);

        // Assert
        alerts.Single(a => a.Kind == "kafka-broker-not-running").SinceUnix.Should().BeNull();
    }

    [Fact]
    public void ExistingAlert_SinceUnixCarried()
    {
        // Arrange: тот же алерт был в prev с SinceUnix — переносится.
        var prev = Snapshot(ActiveCluster(brokers: [Broker("broker1", "UNREACHABLE")])) with
        {
            Alerts = [AlertOf("kafka-broker-not-running:events/broker1", 1750000000)],
        };
        var next = Snapshot(ActiveCluster(brokers: [Broker("broker1", "UNREACHABLE")]));

        // Act
        var alerts = Evaluate(next, prev);

        // Assert
        alerts.Single(a => a.Kind == "kafka-broker-not-running").SinceUnix.Should().Be(1750000000);
    }

    [Fact]
    public void NewAlert_GetsNowUnix()
    {
        // Arrange: в prev алерта не было (брокер был RUNNING).
        var prev = Snapshot(ActiveCluster(brokers: [Broker("broker1")]));
        var next = Snapshot(ActiveCluster(brokers: [Broker("broker1", "UNREACHABLE")]));

        // Act
        var alerts = Evaluate(next, prev);

        // Assert
        alerts.Single(a => a.Kind == "kafka-broker-not-running").SinceUnix.Should().Be(NowUnix);
    }

    [Fact]
    public void MissingEndpoints_CriticalAlert()
    {
        // Arrange: Active-кластер без endpoints.
        var snapshot = Snapshot(ActiveCluster(endpoints: null));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle(a => a.Kind == "kafka-endpoints-missing").Subject;
        alert.Severity.Should().Be(AlertSeverity.Critical);
        alert.Id.Should().Be("kafka-endpoints-missing:events");
    }

    [Fact]
    public void EndpointsMissing_SuppressedForNonActive()
    {
        // Arrange: NOT_INITIALIZED-кластер без endpoints — норма (воркер не дописал).
        var snapshot = Snapshot(PendingCluster());

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().NotContain(a => a.Kind == "kafka-endpoints-missing");
    }

    [Fact]
    public void RotationTicket_PendingInfoAlert()
    {
        // Arrange: живая заявка ротации живого кластера.
        var snapshot = Snapshot(ActiveCluster()) with
        {
            Rotations = [new KafkaRotationTicket("events", 1750000200, "admin")],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle(a => a.Kind == "kafka-rotation-pending").Subject;
        alert.Severity.Should().Be(AlertSeverity.Info);
        alert.Id.Should().Be("kafka-rotation-pending:events");
        alert.Details!["requestedBy"].Should().Be("admin");
    }

    [Fact]
    public void RotationTicketOfDeadCluster_NotAlerted()
    {
        // Arrange: заявка-сирота (кластера нет) — не алертится; живой кластер её
        // не переживёт (arch/16 X-фазы удаляют rotations вместе с кластером).
        var snapshot = Snapshot(ActiveCluster()) with
        {
            Rotations = [new KafkaRotationTicket("ghost", 1750000200, "admin")],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().NotContain(a => a.Kind == "kafka-rotation-pending");
    }

    [Fact]
    public void ParseError_KeyMalformedWarning()
    {
        // Arrange: битый kafka-ключ.
        var snapshot = Snapshot(ActiveCluster()) with
        {
            ParseErrors = [new KeyParseError("/kafka/clusters/x/config", "bad json")],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle(a => a.Kind == "kafka-key-malformed").Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Id.Should().Be("kafka-key-malformed:/kafka/clusters/x/config");
        alert.Details!["reason"].Should().Be("bad json");
    }

    [Fact]
    public void Alerts_SortedBySeverityThenKind()
    {
        // Arrange: critical + warning + info одновременно.
        var snapshot = Snapshot(
            ActiveCluster(endpoints: null, brokers: [Broker("broker1", "UNREACHABLE")]),
            PendingCluster()) with
        {
            ParseErrors = [new KeyParseError("/kafka/clusters/x/config", "bad json")],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert: Critical → Warning → Info (pg-механика AlertEngine).
        alerts.Select(a => a.Severity).Should().BeInDescendingOrder();
    }

    // ===== Lifecycle-заявки топиков (t01, arch/03 §7.4) =====

    private static KafkaClusterInfo ActiveWithTickets(params KafkaTopicLifecycleTicket[] tickets)
        => new(
            "events", KafkaClusterState.Active, 1, 1, 1, 12, 604800000, 1756500000,
            "host.docker.internal:16001",
            [new KafkaBrokerInfo("broker1", "RUNNING", "controller", 2m, 4, 40)],
            Topics: [],
            LifecycleTickets: [.. tickets]);

    [Fact]
    public void LifecycleCreateTicket_PendingInfoAlert()
    {
        // Arrange: живая create-заявка (свежая — не stale).
        var snapshot = Snapshot(ActiveWithTickets(
            new KafkaTopicLifecycleTicket("audit", "create", 12, 3, 86400000L, null, NowUnix - 10, "admin")));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle(a => a.Kind == "kafka-topic-create-pending").Subject;
        alert.Severity.Should().Be(AlertSeverity.Info);
        alert.Id.Should().Be("kafka-topic-create-pending:events/audit");
        alert.Details!["requestedBy"].Should().Be("admin");
    }

    [Fact]
    public void LifecycleDeleteTicket_PendingWarningAlert()
    {
        // Arrange: живая delete-заявка — деструктивная близка к исполнению.
        var snapshot = Snapshot(ActiveWithTickets(
            new KafkaTopicLifecycleTicket("orders", "delete", null, null, null, null, NowUnix - 5, "admin")));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle(a => a.Kind == "kafka-topic-delete-pending").Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Target.Should().Be("events/orders");
    }

    [Fact]
    public void LifecycleTicket_StaleWarningOverridesPending()
    {
        // Arrange: заявка висит дольше StaleDesiredSeconds (600) — воркер буксует.
        var snapshot = Snapshot(ActiveWithTickets(
            new KafkaTopicLifecycleTicket("orders", "delete", null, null, null, null, NowUnix - 700, "admin")));

        // Act
        var alerts = Evaluate(snapshot);

        // Assert: stale-warning; обычного pending-алерта у той же заявки нет.
        var alert = alerts.Should().ContainSingle(a => a.Kind == "kafka-lifecycle-stale").Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alerts.Should().NotContain(a => a.Kind == "kafka-topic-delete-pending");
    }

    [Fact]
    public void NoLifecycleTickets_NoLifecycleAlerts()
    {
        // Arrange: заявок нет — тишина.
        var snapshot = Snapshot(ActiveCluster());

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().NotContain(a => a.Kind == "kafka-topic-create-pending"
            || a.Kind == "kafka-topic-delete-pending" || a.Kind == "kafka-lifecycle-stale");
    }

    // ===== Ребалансировка (t02, arch/03 §7.4): pending + stale =====

    [Fact]
    public void RebalanceTicket_PendingInfoAlert()
    {
        // Arrange: живая заявка ребалансировки живого кластера.
        var snapshot = Snapshot(ActiveCluster()) with
        {
            Rebalances = [new KafkaRebalanceTicket("events", 1750000200, "admin")],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        var alert = alerts.Should().ContainSingle(a => a.Kind == "kafka-rebalance-pending").Subject;
        alert.Severity.Should().Be(AlertSeverity.Info);
        alert.Id.Should().Be("kafka-rebalance-pending:events");
        alert.Target.Should().Be("events");
        alert.Details!["requestedBy"].Should().Be("admin");
    }

    [Fact]
    public void RebalanceTicketOfDeadCluster_NotAlerted()
    {
        // Arrange: заявка-сирота (кластера нет) — как ротация.
        var snapshot = Snapshot(ActiveCluster()) with
        {
            Rebalances = [new KafkaRebalanceTicket("ghost", 1750000200, "admin")],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert
        alerts.Should().NotContain(a => a.Kind == "kafka-rebalance-pending");
    }

    [Fact]
    public void ReassignmentStale_RemainingNotMoving_Warning()
    {
        // Arrange: прогресс жив в обоих тиках, remaining не двигается дольше
        // ReassignStaleSec (prev обновлён давно, next — тем же остатком).
        var progress = new KafkaReassignmentProgress("events", "drain", "broker4", 12, 5, 1750000215, null);
        var prev = Snapshot(ActiveCluster()) with { Reassignments = [progress] };
        var next = prev with
        {
            BuiltAtUtc = prev.BuiltAtUtc.AddSeconds(901),
            Reassignments = [progress with { UpdatedUnix = 1750000215 }],
        };

        // Act
        var alerts = Evaluate(next, prev);

        // Assert
        var alert = alerts.Should().ContainSingle(a => a.Kind == "kafka-reassignment-stale").Subject;
        alert.Severity.Should().Be(AlertSeverity.Warning);
        alert.Target.Should().Be("events");
    }

    [Fact]
    public void ReassignmentStale_NoPrev_WarningByAge()
    {
        // Arrange: prev нет — алерт по возрасту updated_unix (ключ стоит).
        var snapshot = Snapshot(ActiveCluster()) with
        {
            Reassignments = [new KafkaReassignmentProgress("events", "balance", null, 8, 3, 1750000215, null)],
        };
        var aged = snapshot with
        {
            BuiltAtUtc = snapshot.BuiltAtUtc.AddSeconds(1000),
        };

        // Act
        var alerts = Evaluate(aged);

        // Assert: ключ обновлён 1000 c назад (порог 900) — стагнация.
        alerts.Should().ContainSingle(a => a.Kind == "kafka-reassignment-stale");
    }

    [Fact]
    public void ReassignmentFresh_RemainingMoving_NoAlert()
    {
        // Arrange: prev нет, ключ свежий (обновлён недавно относительно Now).
        var snapshot = Snapshot(ActiveCluster()) with
        {
            Reassignments = [new KafkaReassignmentProgress(
                "events", "drain", "broker4", 12, 5, Now.ToUnixTimeSeconds() - 10, null)],
        };

        // Act
        var alerts = Evaluate(snapshot);

        // Assert: свежий прогресс — не алерт.
        alerts.Should().NotContain(a => a.Kind == "kafka-reassignment-stale");
    }
}
