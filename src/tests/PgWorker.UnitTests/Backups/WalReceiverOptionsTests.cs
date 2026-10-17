using FluentAssertions;
using PgWorker.WalReceiver;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Env-контракт приёмника (t27, arch/19 §3.1): полный валидный набор парсится,
// каждый обязательный ключ проверяется, секреты не попадают в текст ошибок.
public class WalReceiverOptionsTests
{
    // Полный валидный набор env (секреты — фиктивные тестовые значения).
    private static Dictionary<string, string> Valid() => new()
    {
        ["PG_HOST"] = "s1a",
        ["PG_PORT"] = "5432",
        ["PG_USER"] = "backup_exec",
        ["PG_PASSWORD"] = "secret-password",
        ["PG_DBNAME"] = "postgres",
        ["SLOT"] = "pgw_bkp_c1_shard1",
        ["CLUSTER"] = "c1",
        ["SHARD"] = "shard1",
        ["S3_ENDPOINTS"] = "http://s3a:9000,http://s3b:9000",
        ["S3_BUCKET"] = "backups",
        ["S3_ACCESS_KEY"] = "access-key",
        ["S3_SECRET_KEY"] = "secret-key",
    };

    private static Dictionary<string, string> Without(string key)
    {
        var env = Valid();
        env.Remove(key);
        return env;
    }

    private static Dictionary<string, string> With(string key, string value)
    {
        var env = Valid();
        env[key] = value;
        return env;
    }

    private static WalReceiverOptions? Parse(Dictionary<string, string> env, out List<string> errors)
        => WalReceiverEnv.Parse(k => env.GetValueOrDefault(k), out errors);

    [Fact]
    public void Полный_валидный_набор_парсится_все_поля_на_месте()
    {
        // Arrange
        var env = Valid();

        // Act
        var options = Parse(env, out var errors);

        // Assert
        options.Should().NotBeNull();
        errors.Should().BeEmpty();
        options!.PgHost.Should().Be("s1a");
        options.PgPort.Should().Be(5432);
        options.PgUser.Should().Be("backup_exec");
        options.PgPassword.Should().Be("secret-password");
        options.PgDbname.Should().Be("postgres");
        options.Slot.Should().Be("pgw_bkp_c1_shard1");
        options.Cluster.Should().Be("c1");
        options.Shard.Should().Be("shard1");
        // t36: списки пула — порядок сохранён; без S3_READ_ENDPOINTS read=write.
        options.S3Endpoints.Should().Equal("http://s3a:9000", "http://s3b:9000");
        options.S3ReadEndpoints.Should().Equal("http://s3a:9000", "http://s3b:9000");
        options.S3Bucket.Should().Be("backups");
        options.S3AccessKey.Should().Be("access-key");
        options.S3SecretKey.Should().Be("secret-key");
    }

    [Theory]
    [InlineData("PG_HOST")]
    [InlineData("PG_PORT")]
    [InlineData("PG_USER")]
    [InlineData("PG_PASSWORD")]
    [InlineData("PG_DBNAME")]
    [InlineData("SLOT")]
    [InlineData("CLUSTER")]
    [InlineData("SHARD")]
    [InlineData("S3_ENDPOINTS")]
    [InlineData("S3_BUCKET")]
    [InlineData("S3_ACCESS_KEY")]
    [InlineData("S3_SECRET_KEY")]
    public void Отсутствие_обязательного_ключа_null_и_ошибка_с_именем_ключа(string key)
    {
        // Arrange
        var env = Without(key);

        // Act
        var options = Parse(env, out var errors);

        // Assert
        options.Should().BeNull();
        errors.Should().ContainSingle(e => e.Contains(key));
    }

    [Fact]
    public void Пустое_значение_обязательного_ключа_ошибка()
    {
        // Arrange
        var env = With("PG_HOST", "");

        // Act
        var options = Parse(env, out var errors);

        // Assert
        options.Should().BeNull();
        errors.Should().ContainSingle(e => e.Contains("PG_HOST"));
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("0")]
    [InlineData("70000")]
    [InlineData("-1")]
    public void PG_PORT_вне_диапазона_ошибка(string port)
    {
        // Arrange
        var env = With("PG_PORT", port);

        // Act
        var options = Parse(env, out var errors);

        // Assert
        options.Should().BeNull();
        errors.Should().ContainSingle(e => e.Contains("PG_PORT"));
    }

    // AAA (t36): S3_ENDPOINTS парсится в список с сохранением порядка.
    [Fact]
    public void S3_ENDPOINTS_порядок_сохранён()
    {
        // Arrange — три uri в заданном порядке + отдельный read-список.
        var env = With("S3_ENDPOINTS", "http://a:9000,http://b:9000,http://c:9000");
        env["S3_READ_ENDPOINTS"] = "http://b:9000,http://a:9000";

        // Act
        var options = Parse(env, out var errors);

        // Assert
        options.Should().NotBeNull();
        errors.Should().BeEmpty();
        options!.S3Endpoints.Should().Equal("http://a:9000", "http://b:9000", "http://c:9000");
        options.S3ReadEndpoints.Should().Equal("http://b:9000", "http://a:9000");
    }

    // AAA (t36): отсутствие S3_ENDPOINTS — ошибка permanent (код 5 в Program).
    [Fact]
    public void S3_ENDPOINTS_отсутствует_ошибка()
    {
        // Arrange
        var env = Without("S3_ENDPOINTS");

        // Act
        var options = Parse(env, out var errors);

        // Assert
        options.Should().BeNull();
        errors.Should().Contain(e => e.Contains("S3_ENDPOINTS"));
    }

    // AAA (t36): пустой S3_ENDPOINTS (только запятые) — тоже ошибка.
    [Fact]
    public void S3_ENDPOINTS_пустой_ошибка()
    {
        // Arrange
        var env = With("S3_ENDPOINTS", ",, ");

        // Act
        var options = Parse(env, out var errors);

        // Assert
        options.Should().BeNull();
        errors.Should().Contain(e => e.Contains("S3_ENDPOINTS"));
    }

    [Fact]
    public void Секреты_не_попадают_в_текст_ошибок()
    {
        // Arrange — порт невалиден, оба секрета в env (но текст ошибки должен
        // строиться без значений секретов при любом раскладе)
        var env = With("PG_PORT", "abc");

        // Act
        var options = Parse(env, out var errors);

        // Assert
        options.Should().BeNull();
        string all = string.Join("\n", errors);
        all.Should().NotContain("secret-password");
        all.Should().NotContain("secret-key");
    }

    // AAA: S3_CA_PEM опционален — без него S3CaPem null (http-endpoint, прежний контракт)
    [Fact]
    public void Parse_БезS3CaPem_ДаетNull()
    {
        WalReceiverEnv.Parse(k => Valid().TryGetValue(k, out var v) ? v : null, out _)
            !.S3CaPem.Should().BeNull();
    }

    // AAA: S3_CA_PEM передан — попадает в опции значением
    [Fact]
    public void Parse_S3CaPem_Значение()
    {
        var parsed = WalReceiverEnv.Parse(k => With("S3_CA_PEM", "PEM-BODY").TryGetValue(k, out var v) ? v : null, out _);
        parsed!.S3CaPem.Should().Be("PEM-BODY");
    }
}
