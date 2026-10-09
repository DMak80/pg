using FluentAssertions;
using Microsoft.Extensions.Configuration;
using PgWorker.App;
using Xunit;

// env-секреты docker-транспорта pg (t07): PGW_DOCKER_TLS_*/PGW_DOCKER_SSH_* →
// конфиг-дерево. Биндинги — pg-специфика в PgWorker.App (модели в Shared.Docker
// env-имён не знают).
public class DockerEnvBindingsTests
{
    [Fact]
    public void ApplyTlsEnvOverrides_DockerTlsKeysMapped()
    {
        // Arrange: env-словарь (inject, без окружения).
        var env = new Dictionary<string, string>
        {
            ["PGW_DOCKER_TLS_CA"] = "ca-pem",
            ["PGW_DOCKER_TLS_CERT_PATH"] = "/tls/pgworker-docker.crt",
        };
        var config = new ConfigurationManager();

        // Act
        DockerEnvBindings.ApplyTlsEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert: ключи легли в PgWorker:Docker:Tls:*; таблица — 6 записей (факт
        // полноты проверяется применением; таблица приватна — это контракт файла).
        config["PgWorker:Docker:Tls:CaPem"].Should().Be("ca-pem");
        config["PgWorker:Docker:Tls:ClientCertPath"].Should().Be("/tls/pgworker-docker.crt");
    }

    [Fact]
    public void ApplySshEnvOverrides_SshKeysMapped()
    {
        // Arrange
        var env = new Dictionary<string, string>
        {
            ["PGW_DOCKER_SSH_KEY_PATH"] = "/secrets/id_pgworker",
            ["PGW_DOCKER_SSH_FINGERPRINT"] = "SHA256:abcdef",
        };
        var config = new ConfigurationManager();

        // Act
        DockerEnvBindings.ApplySshEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert
        config["PgWorker:Docker:Ssh:KeyPath"].Should().Be("/secrets/id_pgworker");
        config["PgWorker:Docker:Ssh:FingerprintSha256"].Should().Be("SHA256:abcdef");
    }

    [Fact]
    public void ApplyRestTlsEnvOverrides_AllFourKeysMapped()
    {
        // Arrange: REST-TLS-пакет (t22) — PEM-значения и _PATH-файлы.
        var env = new Dictionary<string, string>
        {
            ["PGW_REST_TLS_CA"] = "ca-pem",
            ["PGW_REST_TLS_CA_PATH"] = "/tls/ca.pem",
            ["PGW_REST_TLS_CA_KEY"] = "ca-key-pem",
            ["PGW_REST_TLS_CA_KEY_PATH"] = "/tls/ca.key",
        };
        var config = new ConfigurationManager();

        // Act
        DockerEnvBindings.ApplyRestTlsEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert: каждая env попадает в свой конфиг-ключ (арх/14 §8).
        config["PgWorker:Docker:RestTls:CaPem"].Should().Be("ca-pem");
        config["PgWorker:Docker:RestTls:CaPath"].Should().Be("/tls/ca.pem");
        config["PgWorker:Docker:RestTls:CaKeyPem"].Should().Be("ca-key-pem");
        config["PgWorker:Docker:RestTls:CaKeyPath"].Should().Be("/tls/ca.key");
    }
}
