using System.Text.Json;
using AdminPanel.Probes;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер ответа Patroni GET /cluster (arch/02 §6.1, §8: реальные фрагменты +
// вырожденные — отсутствующие поля, null-лаг, строковые числа; spec §10.3).
public class PatroniClusterParserTests
{
    private static string LoadFixture()
        => File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "ProbesFixtures", "patroni-cluster.json"));

    [Fact]
    public void Parse_FullFixture_AllMembers()
    {
        // Arrange
        var json = LoadFixture();

        // Act
        var members = PatroniClusterParser.Parse(json);

        // Assert
        members.Should().HaveCount(3);
        var replica = members.Single(m => m.Name == "s1b");
        replica.Role.Should().Be("replica");
        replica.State.Should().Be("streaming");
        replica.Timeline.Should().Be(1L);
        replica.LagBytes.Should().Be(52428800L);
        members.Single(m => m.Name == "s1c").LagBytes.Should().BeNull(); // null-лаг толерантен
    }

    [Fact]
    public void Parse_Tolerant_MissingFieldsAndStringNumbers()
    {
        // Arrange — нет state/timeline/lag, числа строками (строгий Patroni их не шлёт,
        // но шлёт эмулятор стенда; толерантность — arch/02 §8).
        const string json = """
            {"members":[{"name":"x","role":"replica","timeline":"2","lag":"100"}]}
            """;

        // Act
        var members = PatroniClusterParser.Parse(json);

        // Assert
        var member = members.Should().ContainSingle().Subject;
        member.State.Should().BeNull();
        member.Timeline.Should().Be(2L);
        member.LagBytes.Should().Be(100L);
    }

    [Fact]
    public void Parse_TransitionalLagUnknown_DoesNotThrow()
    {
        // Arrange — Patroni 4.x в переходных состояниях члена (starting при
        // пересоздании ноды) отдаёт "lag": "unknown" (и lsn-поля строками):
        // строгое чтение валило ВЕСЬ парсер и роняло пробы всех членов скопа.
        const string json = """
            {"members":[
              {"name":"a","role":"leader","state":"running","timeline":3},
              {"name":"b","role":"replica","state":"starting",
               "receive_lsn":"unknown","lsn":"unknown","lag":"unknown"}]}
            """;

        // Act
        var members = PatroniClusterParser.Parse(json);

        // Assert — парсер не упал, лаг переходного члена — null (неизвестен)
        members.Should().HaveCount(2);
        members.Single(m => m.Name == "b").LagBytes.Should().BeNull();
        members.Single(m => m.Name == "a").Timeline.Should().Be(3L);
    }

    [Fact]
    public void Parse_BrokenJson_Throws()
    {
        // Arrange — мусор парсер не глотает: ошибку ловит проба (spec §10.3).
        const string json = "not json at all";

        // Act
        var act = () => PatroniClusterParser.Parse(json);

        // Assert
        act.Should().Throw<JsonException>();
    }
}
