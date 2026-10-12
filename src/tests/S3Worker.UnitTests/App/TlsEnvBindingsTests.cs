using Microsoft.Extensions.Configuration;
using S3Worker.App.Api;

namespace S3Worker.UnitTests.App;

// Options-биндинг TLS (arch/24 §8): S3W_API_TLS_* → конфиг-дерево; таблица
// EnvBindings — единственный источник соответствий (Program + тест).
public class TlsEnvBindingsTests
{
    [Fact]
    public void ApplyEnvOverrides_PemAndPathKeysMapped()
    {
        // Arrange: env-словарь без реального окружения (inject в чистую функцию).
        var env = new Dictionary<string, string>
        {
            ["S3W_API_TLS_CERT"] = "PEM-CERT",
            ["S3W_API_TLS_CLIENT_CA_PATH"] = "/tls/ca.pem",
        };
        var config = new ConfigurationManager();

        // Act: перенос.
        TlsEndpoints.ApplyEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert: каждое имя env легло в свой конфиг-ключ; незаданные — нет.
        config["S3Worker:Api:Tls:ServerCertPem"].Should().Be("PEM-CERT");
        config["S3Worker:Api:Tls:ClientCaPath"].Should().Be("/tls/ca.pem");
        config["S3Worker:Api:Tls:ServerKeyPem"].Should().BeNull();

        // Таблица покрывает все 6 секретов (arch/24 §8).
        TlsEndpoints.EnvBindings.Should().HaveCount(6);
    }
}
