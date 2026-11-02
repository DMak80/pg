using PgWorker.PgNode;
using Xunit;

// AAA: PgNodeStatus — JSON-контракт статусного ключа (arch/14 §3.3, spec §3.6):
// null-поля не сериализуются, компоненты — словарём {"pg":…,"doorman":…}.
public class PgNodeStatusTests
{
    [Fact]
    public void ToJson_CarriesCoreFields()
    {
        // Arrange: статус ready с применённой ревизией и компонентами.
        var status = new PgNodeStatus(PgNodeStatus.Ready, 42,
            new Dictionary<string, string> { ["pg"] = "ready", ["doorman"] = "ready" },
            UpdatedUnix: 1700000000);

        // Act: сериализация.
        var json = status.ToJson();

        // Assert: все ключевые поля контракта на месте.
        json.Should().Contain("\"state\":\"ready\"");
        json.Should().Contain("\"applied_revision\":42");
        json.Should().Contain("\"pg\":\"ready\"");
        json.Should().Contain("\"doorman\":\"ready\"");
        json.Should().Contain("\"updated_unix\":1700000000");
    }

    [Fact]
    public void ToJson_NullFields_NotSerialized()
    {
        // Arrange: not_initialized — applied_revision/error ещё нет.
        var status = new PgNodeStatus(PgNodeStatus.NotInitialized, null,
            new Dictionary<string, string>(), UpdatedUnix: 1, Error: null);

        // Act: сериализация.
        var json = status.ToJson();

        // Assert: null-поля (error, applied_revision) отсутствуют.
        json.Should().NotContain("applied_revision");
        json.Should().NotContain("error");
        json.Should().Contain("\"state\":\"not_initialized\"");
    }

    [Fact]
    public void ToJson_ComponentsDictionary_Serialized()
    {
        // Arrange: стартующие компоненты (фаза (а)).
        var status = new PgNodeStatus(PgNodeStatus.Initializing, null,
            new Dictionary<string, string> { ["pg"] = "starting", ["doorman"] = "starting" },
            UpdatedUnix: 2);

        // Act: сериализация.
        var json = status.ToJson();

        // Assert: компонентный словарь — {"pg":…,"doorman":…}.
        json.Should().Contain("\"components\":{\"pg\":\"starting\",\"doorman\":\"starting\"}");
    }
}
