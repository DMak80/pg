using AdminPanel.Probes.S3;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Валидация MinioOptions (t08, spec §4.1): fail-fast только при заданном
// Endpoint — пустой Endpoint = грань выключена (не ошибка, AC1).
public class MinioOptionsTests
{
    private static MinioOptions Valid() => new()
    {
        S3 =
        {
            Endpoint = "http://minio:9000",
            Bucket = "pgworker-backups",
            AccessKey = "minioadmin",
            SecretKey = "minioadmin",
        },
        IntervalSec = 60,
        TimeoutSec = 5,
    };

    // AAA: Endpoint задан, Bucket пуст → InvalidOperationException с именем поля
    [Fact]
    public void EnsureValid_EmptyBucket_Throws()
    {
        // Arrange — грань включена, bucket не задан
        var options = Valid();
        options.S3.Bucket = "";

        // Act
        var act = () => options.EnsureValid();

        // Assert
        act.Should().Throw<InvalidOperationException>().Which.Message.Should().Contain("Bucket");
    }

    // AAA: Endpoint задан, AccessKey/SecretKey пусты → бросок
    [Fact]
    public void EnsureValid_EmptyCredentials_Throws()
    {
        // Arrange — креды не заданы
        var options = Valid();
        options.S3.AccessKey = "";
        options.S3.SecretKey = "";

        // Act
        var act = () => options.EnsureValid();

        // Assert — сообщение называет поле кредов
        act.Should().Throw<InvalidOperationException>().Which.Message.Should().Contain("AccessKey");
    }

    // AAA: Endpoint задан, IntervalSec = 0 → бросок
    [Fact]
    public void EnsureValid_ZeroInterval_Throws()
    {
        // Arrange — период тика 0
        var options = Valid();
        options.IntervalSec = 0;

        // Act
        var act = () => options.EnsureValid();

        // Assert
        act.Should().Throw<InvalidOperationException>().Which.Message.Should().Contain("IntervalSec");
    }

    // AAA: Endpoint задан, TimeoutSec = -1 → бросок
    [Fact]
    public void EnsureValid_NegativeTimeout_Throws()
    {
        // Arrange — отрицательный таймаут
        var options = Valid();
        options.TimeoutSec = -1;

        // Act
        var act = () => options.EnsureValid();

        // Assert
        act.Should().Throw<InvalidOperationException>().Which.Message.Should().Contain("TimeoutSec");
    }

    // AAA: пустой Endpoint — грань выключена, валидации нет (не бросает)
    [Fact]
    public void EnsureValid_EmptyEndpoint_DoesNotThrow()
    {
        // Arrange — незаданная грань с прочими пустыми полями
        var options = new MinioOptions();

        // Act
        var act = () => options.EnsureValid();

        // Assert
        act.Should().NotThrow();
    }

    // AAA: полный валидный набор — не бросает
    [Fact]
    public void EnsureValid_FullValidSet_DoesNotThrow()
    {
        // Arrange — всё задано
        var options = Valid();

        // Act
        var act = () => options.EnsureValid();

        // Assert
        act.Should().NotThrow();
    }
}
