using PgWorker.PgNode;
using Xunit;

// AAA (П6.2): env регенерации configure_spilo (ХВОСТ t05probe3a — секция
// postgresql.authentication писалась дефолтами шаблона zalando/standby):
// per-install пароли PG передаются в env child-процесса ТЕМИ ЖЕ именами,
// что spilo-supervised.sh экспортирует launch'у из spilo-env; пустые пароли
// не прокидываются (путь без доставки — прежнее поведение дефолтов).
public class PatroniApplyCommandsTests
{
    [Fact]
    public void RegenerationEnvironment_WithPerInstallPasswords_CarriesThemAll()
    {
        // Arrange: применяемый YAML + per-install пароли PG.
        const string yaml = "restapi:\n  authentication:\n    password: \"new-rest\"";

        // Act
        var env = PatroniApplyCommands.RegenerationEnvironment(
            yaml, "per-install-su-123", "per-install-standby-456");

        // Assert: полный env регенерации — YAML, env-пути рест-серт ноды
        // (t05probe2) и per-install пароли с юзерами (t05probe3a-хвост).
        env["SPILO_CONFIGURATION"].Should().Be(yaml);
        env["SSL_RESTAPI_CERTIFICATE_FILE"].Should().Be("/run/certs/rest-api-server.crt");
        env["SSL_RESTAPI_PRIVATE_KEY_FILE"].Should().Be("/run/certs/restapi-api-server.key");
        env["PGUSER_SUPERUSER"].Should().Be("postgres");
        env["PGPASSWORD_SUPERUSER"].Should().Be("per-install-su-123");
        env["PGUSER_STANDBY"].Should().Be("standby");
        env["PGPASSWORD_STANDBY"].Should().Be("per-install-standby-456");
    }

    [Fact]
    public void RegenerationEnvironment_WithoutPasswords_OmitsPgSecrets()
    {
        // Arrange: путь без доставки per-install паролей (пустые значения).
        const string yaml = "restapi:\n  authentication:\n    password: \"new-rest\"";

        // Act
        var env = PatroniApplyCommands.RegenerationEnvironment(yaml, "", "");

        // Assert: секретные ключи НЕ прокинуты — configure_spilo подставит
        // дефолты шаблона (прежнее поведение); серт-пути и YAML на месте.
        env.Should().NotContainKey("PGPASSWORD_SUPERUSER");
        env.Should().NotContainKey("PGPASSWORD_STANDBY");
        env.Should().NotContainKey("PGUSER_SUPERUSER");
        env.Should().NotContainKey("PGUSER_STANDBY");
        env["SPILO_CONFIGURATION"].Should().Be(yaml);
        env["SSL_RESTAPI_CERTIFICATE_FILE"].Should().Be("/run/certs/rest-api-server.crt");
    }

    [Fact]
    public void RegenerationEnvironment_PartialPassword_CarriesOnlyThatPair()
    {
        // Arrange: доставлен только su-пароль (standby пуст — например,
        // частичная доставка не допускается, но env обязан быть предсказуем).
        // Act
        var env = PatroniApplyCommands.RegenerationEnvironment(
            "yaml", "su-only-999", "");

        // Assert: только su-пара прокинута.
        env["PGPASSWORD_SUPERUSER"].Should().Be("su-only-999");
        env.Should().NotContainKey("PGPASSWORD_STANDBY");
        env.Should().NotContainKey("PGUSER_STANDBY");
    }
}
