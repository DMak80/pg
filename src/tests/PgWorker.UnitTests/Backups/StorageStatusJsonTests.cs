using PgWorker.Backups;
using FluentAssertions;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Сериализация ключа /pgworker/backups/storage (t06, spec §3.4/AC6):
// с квотой — все поля; без квоты — поля квоты опущены, state=OK;
// имена состояний OK/WARN/CRIT.
public class StorageStatusJsonTests
{
    // С квотой: used_bytes/quota_bytes/used_percent/state/updated_unix.
    [Fact]
    public void С_квотой_все_поля()
    {
        // Arrange — статус с квотой 100, занято 30 (30%), OK
        var status = new StorageStatus(30, 100, 30, StorageState.Ok, 1_760_000_000);

        // Act — сериализация
        var json = StorageStatusJson.Serialize(status);

        // Assert — ключи канона присутствуют со значениями
        json.Should().Contain("\"used_bytes\":30");
        json.Should().Contain("\"quota_bytes\":100");
        json.Should().Contain("\"used_percent\":30");
        json.Should().Contain("\"state\":\"OK\"");
        json.Should().Contain("\"updated_unix\":1760000000");
    }

    // Квота 0 → полей квоты/процентов нет, state=OK.
    [Fact]
    public void Без_квоты_полей_квоты_нет()
    {
        // Arrange — статус без квоты
        var status = new StorageStatus(123, 0, null, StorageState.Ok, 1_760_000_000);

        // Act — сериализация
        var json = StorageStatusJson.Serialize(status);

        // Assert — quota_bytes/used_percent отсутствуют
        json.Should().Contain("\"used_bytes\":123");
        json.Should().Contain("\"state\":\"OK\"");
        json.Should().NotContain("quota_bytes");
        json.Should().NotContain("used_percent");
    }

    // Имена состояний WARN/CRIT.
    [Theory]
    [InlineData(StorageState.Warn, "WARN")]
    [InlineData(StorageState.Crit, "CRIT")]
    [InlineData(StorageState.Ok, "OK")]
    public void Имена_состояний(StorageState state, string expected)
    {
        // Arrange — состояние задано параметром

        // Act — имя
        var name = StorageStatusJson.StateName(state);

        // Assert — каноническое имя
        name.Should().Be(expected);
    }
}
