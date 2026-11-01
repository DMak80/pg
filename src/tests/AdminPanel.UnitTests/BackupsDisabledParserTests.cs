using AdminPanel.Core;
using Shared.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Точечный парсер ключа /pgworker/backups-disabled (u10, arch/19 §4): валидный
// JSON; битый JSON → parseError; пустое значение → parseError; ключа нет → null
// без ошибки (бэкапы работают). AAA.
public class BackupsDisabledParserTests
{
    [Fact]
    public void Parse_ValidValue_InfoWithAuthor()
    {
        // Arrange — канон arch/19 §4: {"updated_unix","updated_by"?}
        var kv = new Kv("/pgworker/backups-disabled",
            """{"updated_unix":1761900000,"updated_by":"ops"}""", 1);

        // Act
        var (info, error) = BackupsDisabledParser.Parse(kv);

        // Assert
        error.Should().BeNull();
        info.Should().NotBeNull();
        info!.UpdatedUnix.Should().Be(1761900000);
        info.UpdatedBy.Should().Be("ops");
    }

    [Fact]
    public void Parse_ValidValueWithoutAuthor_ByIsNull()
    {
        // Arrange — updated_by опционален
        var kv = new Kv("/pgworker/backups-disabled",
            """{"updated_unix":1761900000}""", 1);

        // Act
        var (info, error) = BackupsDisabledParser.Parse(kv);

        // Assert
        error.Should().BeNull();
        info!.UpdatedBy.Should().BeNull();
    }

    [Fact]
    public void Parse_BrokenJson_ParseError()
    {
        // Arrange
        var kv = new Kv("/pgworker/backups-disabled", "{not-json", 1);

        // Act
        var (info, error) = BackupsDisabledParser.Parse(kv);

        // Assert — толерантность: parseError, модель не строится
        info.Should().BeNull();
        error.Should().NotBeNull();
        error!.Key.Should().Be("/pgworker/backups-disabled");
    }

    [Fact]
    public void Parse_EmptyValue_ParseError()
    {
        // Arrange — пустое тело под живым ключом (мусор воркера/операции)
        var kv = new Kv("/pgworker/backups-disabled", "", 1);

        // Act
        var (info, error) = BackupsDisabledParser.Parse(kv);

        // Assert
        info.Should().BeNull();
        error.Should().NotBeNull();
    }

    [Fact]
    public void Parse_MissingUpdatedUnix_ParseError()
    {
        // Arrange
        var kv = new Kv("/pgworker/backups-disabled",
            """{"updated_by":"ops"}""", 1);

        // Act
        var (info, error) = BackupsDisabledParser.Parse(kv);

        // Assert
        info.Should().BeNull();
        error.Should().NotBeNull();
    }

    [Fact]
    public void Parse_NoKey_NullWithoutError()
    {
        // Arrange / Act — ключа нет (точечный range вернул пусто)
        var (info, error) = BackupsDisabledParser.Parse(null);

        // Assert — бэкапы работают, алерта нет
        info.Should().BeNull();
        error.Should().BeNull();
    }
}
