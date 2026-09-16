using FluentAssertions;
using ValkeyWorker.App.Loops;
using ValkeyWorker.Core.Model;
using Xunit;

namespace ValkeyWorker.UnitTests.App;

// Классификация тика (arch/21 §5): 4 ветки + незнакомое state → Active (raw).
public class ValkeyClusterClassifierTests
{
    private static ValkeyClusterSnapshot Snap(string? state)
        => new(
            "demo",
            state is null && !HasConfig() ? null : new ValkeyClusterConfig(1, 1, "allkeys-lru", 1, state),
            new Dictionary<string, ValkeyNodeSnapshot>(),
            null, null, null, null, null, [], []);

    // Хелпер-флаг: без config (null) — отдельный кейс.
    private static bool HasConfig() => false;

    [Fact]
    public void NotInitialized_Provision()
    {
        // Arrange: заявка создания.
        // Act/Assert
        ValkeyClusterClassifier.Classify(Snap("NOT_INITIALIZED")).Should().Be(ValkeyClusterKind.Provision);
    }

    [Fact]
    public void ToRemove_Deprovision()
    {
        // Arrange: заявка демонтажа.
        // Act/Assert
        ValkeyClusterClassifier.Classify(Snap("TO_REMOVE")).Should().Be(ValkeyClusterKind.Deprovision);
    }

    [Fact]
    public void БезState_Active()
    {
        // Arrange: Active-кластер (state снят воркером).
        var snap = new ValkeyClusterSnapshot(
            "demo", new ValkeyClusterConfig(1, 1, "allkeys-lru", 1, null),
            new Dictionary<string, ValkeyNodeSnapshot>(),
            null, null, null, null, null, [], []);

        // Act/Assert
        ValkeyClusterClassifier.Classify(snap).Should().Be(ValkeyClusterKind.Active);
    }

    [Fact]
    public void БитыйConfig_Skip()
    {
        // Arrange: config-ключа нет/битый (Config == null).
        var snap = new ValkeyClusterSnapshot(
            "demo", null,
            new Dictionary<string, ValkeyNodeSnapshot>(),
            null, null, null, null, null, [], ["битый JSON"]);

        // Act/Assert
        ValkeyClusterClassifier.Classify(snap).Should().Be(ValkeyClusterKind.Skip);
    }

    [Fact]
    public void НезнакомоеСостояние_ActiveRaw()
    {
        // Arrange: state вне канона (система развивается, arch/20 §5).
        // Act/Assert
        ValkeyClusterClassifier.Classify(Snap("DRAFT")).Should().Be(ValkeyClusterKind.Active);
    }
}
