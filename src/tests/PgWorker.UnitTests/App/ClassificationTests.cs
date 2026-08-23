using PgWorker.App.Loops;
using PgWorker.Core.Model;

namespace PgWorker.UnitTests.App;

// Классификация кластеров циклом ReconcileLoop (задача 23; spec §6.2):
// config.state NOT_INITIALIZED → ProvisioningProcess, TO_REMOVE →
// DeprovisioningProcess, отсутствует/иное → NodeSupervisor.
public class ClassificationTests
{
    [Theory]
    [InlineData(ClusterState.NotInitialized, ClusterWork.Provision)]
    [InlineData(ClusterState.ToRemove, ClusterWork.Deprovision)]
    [InlineData(ClusterState.Active, ClusterWork.Supervise)]
    public void Classify_ByConfigState_SelectsProcess(ClusterState state, ClusterWork expected)
    {
        // Arrange — config кластера с заданным state
        var config = new ClusterConfig("shop", 6, "shop", null, state);

        // Act
        var work = ClusterClassifier.Classify(config);

        // Assert — процесс выбран по таблице классификации
        work.Should().Be(expected);
    }

    [Fact]
    public void Classify_ActiveClusterWithoutState_Supervised()
    {
        // Arrange — кластер после provisioning: поле state отсутствует (Д1)
        var config = new ClusterConfig("shop", 6, "shop", 12345, ClusterState.Active);

        // Act
        var work = ClusterClassifier.Classify(config);

        // Assert — обычный надзор
        work.Should().Be(ClusterWork.Supervise);
    }
}
