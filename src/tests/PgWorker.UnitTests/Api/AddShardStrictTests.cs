using PgWorker.App.Api.Operations;

namespace PgWorker.UnitTests.Api;

// t06 (spec §3.6/§6.6, юнит-часть AC): чтение strict из config-JSON —
// отсутствие/не-bool = true (легаси-кластеры strict по умолчанию, решение №3).
public class AddShardStrictTests
{
    // AAA: семантика поля — тернарная форма, свёртка && запрещена (даёт false).
    [Theory]
    [InlineData("""{"buckets":4,"dbname":"shop"}""", true)]
    [InlineData("""{"buckets":4,"dbname":"shop","synchronous_mode_strict":"yes"}""", true)]
    [InlineData("""{"buckets":4,"dbname":"shop","synchronous_mode_strict":true}""", true)]
    [InlineData("""{"buckets":4,"dbname":"shop","synchronous_mode_strict":false}""", false)]
    public void ReadStrictField_AbsentOrNonBool_IsTrue(string raw, bool expected)
    {
        // Arrange / Act
        var strict = AddShardHandler.ReadStrictField(raw);

        // Assert
        strict.Should().Be(expected);
    }
}
