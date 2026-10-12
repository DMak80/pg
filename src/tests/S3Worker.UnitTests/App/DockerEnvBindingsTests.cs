using Microsoft.Extensions.Configuration;
using S3Worker.App;

namespace S3Worker.UnitTests.App;

// env-секреты docker-транспорта (arch/24 §2): S3W_DOCKER_TLS_*/S3W_DOCKER_* →
// конфиг-дерево S3Worker:Docker:{Tls,Ssh}; таблицы биндингов — единственный
// источник соответствий (Program + тест).
public class DockerEnvBindingsTests
{
    [Fact]
    public void ApplyTlsEnvOverrides_PemAndPathKeysMapped()
    {
        // Arrange: env-словарь без реального окружения (inject в чистую функцию).
        var env = new Dictionary<string, string>
        {
            ["S3W_DOCKER_TLS_CA"] = "CA-PEM",
            ["S3W_DOCKER_TLS_CERT_PATH"] = "/tls/docker.crt",
        };
        var config = new ConfigurationManager();

        // Act
        DockerEnvBindings.ApplyTlsEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert
        config["S3Worker:Docker:Tls:CaPem"].Should().Be("CA-PEM");
        config["S3Worker:Docker:Tls:ClientCertPath"].Should().Be("/tls/docker.crt");
        config["S3Worker:Docker:Tls:ClientKeyPem"].Should().BeNull();
    }

    [Fact]
    public void ApplySshEnvOverrides_KeyAndFingerprintMapped()
    {
        // Arrange
        var env = new Dictionary<string, string>
        {
            ["S3W_DOCKER_SSH_KEY_PATH"] = "/secrets/id_ed25519",
            ["S3W_DOCKER_SSH_FINGERPRINT"] = "SHA256:abcdef",
        };
        var config = new ConfigurationManager();

        // Act
        DockerEnvBindings.ApplySshEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert
        config["S3Worker:Docker:Ssh:KeyPath"].Should().Be("/secrets/id_ed25519");
        config["S3Worker:Docker:Ssh:FingerprintSha256"].Should().Be("SHA256:abcdef");
        config["S3Worker:Docker:Ssh:KeyPem"].Should().BeNull();
    }
}
