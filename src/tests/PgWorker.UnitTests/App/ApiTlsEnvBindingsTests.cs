using FluentAssertions;
using Microsoft.Extensions.Configuration;
using PgWorker.App.Api;
using Xunit;

namespace PgWorker.UnitTests.App;

// Env-секреты mTLS API (arch/14 §1.1, t03): PEM и _PATH; порт — из
// ASPNETCORE_URLS/urls (E2E поднимает хост-процесс на свободном порту), иначе 8080.
public class ApiTlsEnvBindingsTests
{
    [Fact]
    public void ApplyEnvOverrides_PgApiTlsKeysMapped()
    {
        // Arrange
        var env = new Dictionary<string, string>
        {
            ["PGW_API_TLS_CERT"] = "cert-pem",
            ["PGW_API_TLS_KEY_PATH"] = "/tls/pgserver.key",
            ["PGW_API_TLS_CLIENT_CA_PATH"] = "/tls/ca.pem",
        };
        var config = new ConfigurationManager();

        // Act
        ApiTlsEndpoints.ApplyEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert
        config["PgWorker:Api:Tls:ServerCertPem"].Should().Be("cert-pem");
        config["PgWorker:Api:Tls:ServerKeyPath"].Should().Be("/tls/pgserver.key");
        config["PgWorker:Api:Tls:ClientCaPath"].Should().Be("/tls/ca.pem");
        ApiTlsEndpoints.EnvBindings.Should().HaveCount(6);
    }

    [Theory]
    [InlineData("https://127.0.0.1:18443", 18443)]
    [InlineData("http://127.0.0.1:9000;https://127.0.0.1:19001", 19001)] // последний binding
    [InlineData("", 8080)]
    [InlineData(null, 8080)]
    public void ResolvePort_FromUrlsConfig_OrDefault(string? urls, int expected)
    {
        // Arrange
        var config = new ConfigurationManager();
        if (urls is not null) config["urls"] = urls;

        // Act / Assert
        ApiTlsEndpoints.ResolvePort(config).Should().Be(expected);
    }
}
