using FluentAssertions;
using KafkaWorker.App.Loops;
using KafkaWorker.Core.Model;

namespace KafkaWorker.UnitTests.App;

// Классификация кластеров тика (arch/16 §5): config.state → Provision/
// Deprovision/Active + кандидаты scale-прохода волны B по стейтам брокеров.

public class KafkaClusterClassifierTests
{
    private static KafkaClusterConfig Config(string? state) =>
        new(3, 3, 2, 12, 604800000L, 1756500000L, state);

    private static KafkaBrokerDecl Broker(string name, string? state) =>
        new(name, state, null, null);

    [Fact]
    public void Classify_NotInitialized_Provision()
    {
        // Arrange: заявка панели.
        var config = Config("NOT_INITIALIZED");

        // Act: классификация.
        var work = KafkaClusterClassifier.Classify(config);

        // Assert: provisioning (K0–K6).
        work.Should().Be(KafkaClusterWork.Provision);
    }

    [Fact]
    public void Classify_ToRemove_Deprovision()
    {
        // Arrange: маркер удаления.
        var config = Config("TO_REMOVE");

        // Act: классификация.
        var work = KafkaClusterClassifier.Classify(config);

        // Assert: deprovisioning (X0–X3).
        work.Should().Be(KafkaClusterWork.Deprovision);
    }

    [Fact]
    public void Classify_NoStateOrUnknown_Active()
    {
        // Arrange: Active-кластер (state снят) и незнакомое значение (толерантность).
        // Act + Assert: оба — Active-ветка (надзор + converge).
        KafkaClusterClassifier.Classify(Config(null)).Should().Be(KafkaClusterWork.Active);
        KafkaClusterClassifier.Classify(Config("WEIRD")).Should().Be(KafkaClusterWork.Active);
    }

    [Fact]
    public void Candidates_BrokerStates_AddAndRemove()
    {
        // Arrange: Active-кластер: broker2 заявлен добавлением, broker4 — демонтаж.
        var snap = new KafkaClusterSnapshot(
            "events", Config(null),
            [Broker("broker1", "RUNNING"), Broker("broker2", "NOT_INITIALIZED"),
             Broker("broker3", "RUNNING"), Broker("broker4", "TO_REMOVE")],
            [], [], 0, "h:16000", "app", "pw");

        // Act: кандидаты scale-прохода (волна B).
        var add = KafkaClusterClassifier.AddCandidates(snap);
        var remove = KafkaClusterClassifier.RemoveCandidates(snap);

        // Assert: NOT_INITIALIZED → add; TO_REMOVE → remove; RUNNING — мимо.
        add.Should().BeEquivalentTo(["broker2"]);
        remove.Should().BeEquivalentTo(["broker4"]);
    }
}
