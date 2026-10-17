using AdminPanel.Probes.S3;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// BackupsS3Options.Endpoints (t36, 02 §2.5): запятая-список целей; пусто →
// [Endpoint]; оба пусты — грань выключена. Поведенческий перебор целей —
// S3StorageClientFailoverTests.
public class BackupsS3OptionsTests
{
    private static BackupsS3Options.S3Options S3(string endpoint = "", string endpoints = "")
        => new() { Endpoint = endpoint, Endpoints = endpoints, Bucket = "b", AccessKey = "k", SecretKey = "s" };

    // AAA: Endpoints="a,b" → 2 цели в порядке списка.
    [Fact]
    public void Endpoints_TwoTargets()
    {
        // Arrange
        var s3 = S3("http://a:9000", "http://a:9000/,http://b:9000");

        // Act
        var targets = s3.TargetList();

        // Assert: порядок сохранён, хвостовой слэш отсечён.
        targets.Should().Equal("http://a:9000", "http://b:9000");
    }

    // AAA: задан только Endpoint → валидный частный случай (один элемент).
    [Fact]
    public void EndpointOnly_SingleTarget()
    {
        // Arrange
        var s3 = S3("http://a:9000");

        // Act
        var targets = s3.TargetList();

        // Assert
        targets.Should().Equal("http://a:9000");
    }

    // AAA: оба пусты → грань выключена, список целей пуст.
    [Fact]
    public void BothEmpty_NoTargets_GrainDisabled()
    {
        // Arrange
        var options = new BackupsS3Options { S3 = S3() };

        // Act
        var targets = options.S3.TargetList();

        // Assert
        targets.Should().BeEmpty();
        options.IsConfigured.Should().BeFalse();
    }

    // AAA: EnsureValid не бросает при заданных Endpoints (валидация грани).
    [Fact]
    public void Endpoints_EnsureValid_Passes()
    {
        // Arrange
        var options = new BackupsS3Options
        {
            S3 = S3("http://a:9000", "http://a:9000,http://b:9000"),
        };

        // Act
        var act = () => options.EnsureValid();

        // Assert
        act.Should().NotThrow();
    }

    // AAA: дубли в Endpoints схлопываются (перебор не крутит одну цель дважды).
    [Fact]
    public void Endpoints_Duplicates_Collapsed()
    {
        // Arrange
        var s3 = S3("http://a:9000", "http://a:9000,http://a:9000,http://b:9000");

        // Act
        var targets = s3.TargetList();

        // Assert
        targets.Should().Equal("http://a:9000", "http://b:9000");
    }

    // AAA: пустой Endpoint при непустых Endpoints — грань ВКЛЮЧЕНА (t36, 02 §2.5),
    // EnsureValid при полном комплекте не бросает.
    [Fact]
    public void EmptyEndpoint_NonEmptyEndpoints_GrainEnabled()
    {
        // Arrange
        var options = new BackupsS3Options { S3 = S3("", "http://a:9000,http://b:9000") };

        // Act
        var configured = options.IsConfigured;
        var act = () => options.EnsureValid();

        // Assert
        configured.Should().BeTrue(
            "непустые Endpoints при пустом Endpoint — грань включена (02 §2.5)");
        act.Should().NotThrow();
    }

    // AAA: грань через Endpoints валидируется так же — пустой Bucket при заданных
    // целях → fail-fast с именем поля.
    [Fact]
    public void EndpointsOnly_EmptyBucket_EnsureValid_Throws()
    {
        // Arrange
        var options = new BackupsS3Options
        {
            S3 = new BackupsS3Options.S3Options
            {
                Endpoint = "", Endpoints = "http://a:9000",
                Bucket = "", AccessKey = "k", SecretKey = "s",
            },
        };

        // Act
        var act = () => options.EnsureValid();

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*AdminPanel:Backups:S3:Bucket*");
    }
}
