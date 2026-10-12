using S3Worker.App.Loops;
using S3Worker.Core.Model;

namespace S3Worker.UnitTests.App;

// Классификация тика (arch/24 §5): чистая функция Classify по config.state —
// NOT_INITIALIZED/TO_REMOVE/Active (state нет)/Skip (config нет)/незнакомое raw.
public class S3ClusterClassifierTests
{
    private static S3ClusterSnapshot Snap(S3ClusterConfig? config) =>
        new("c", config, [], null, null);

    [Fact]
    public void Classify_ConfigNull_Skip()
    {
        // Arrange — config-ключа нет (или битый)
        var snap = Snap(null);

        // Act
        var kind = S3ClusterClassifier.Classify(snap);

        // Assert
        kind.Should().Be(S3ClusterKind.Skip);
    }

    [Fact]
    public void Classify_StateNull_Active()
    {
        // Arrange — Active: state отсутствует (arch/23 §2)
        var snap = Snap(new S3ClusterConfig("owns3", 3, "p", 1, null));

        // Act
        var kind = S3ClusterClassifier.Classify(snap);

        // Assert
        kind.Should().Be(S3ClusterKind.Active);
    }

    [Fact]
    public void Classify_NotInitialized_Provision()
    {
        // Arrange — заявка панели
        var snap = Snap(new S3ClusterConfig("owns3", 3, "p", 1, S3NodeStates.NotInitialized));

        // Act
        var kind = S3ClusterClassifier.Classify(snap);

        // Assert
        kind.Should().Be(S3ClusterKind.Provision);
    }

    [Fact]
    public void Classify_ToRemove_Deprovision()
    {
        // Arrange — заявка удаления (one-way)
        var snap = Snap(new S3ClusterConfig("owns3", 3, "p", 1, S3NodeStates.ToRemove));

        // Act
        var kind = S3ClusterClassifier.Classify(snap);

        // Assert
        kind.Should().Be(S3ClusterKind.Deprovision);
    }

    [Fact]
    public void Classify_UnknownState_Active()
    {
        // Arrange — незнакомое state (толерантность arch/23 §2 — система развивается)
        var snap = Snap(new S3ClusterConfig("owns3", 3, "p", 1, "SOMETHING"));

        // Act
        var kind = S3ClusterClassifier.Classify(snap);

        // Assert — Active с raw-строкой
        kind.Should().Be(S3ClusterKind.Active);
    }
}
