using FluentAssertions;
using ValkeyWorker.App.Api;
using Microsoft.Extensions.Configuration;

namespace ValkeyWorker.UnitTests.App;

// Options-биндинг TLS (arch/21 §8): VWK_API_TLS_* → конфиг-дерево; таблица
// EnvBindings — единственный источник соответствий (Program + тест).
public class TlsEnvBindingsTests
{
    [Fact]
    public void ApplyEnvOverrides_PemAndPathKeysMapped()
    {
        // Arrange: env-словарь без реального окружения (inject в чистую функцию).
        var env = new Dictionary<string, string>
        {
            ["VWK_API_TLS_CERT"] = "PEM-CERT",
            ["VWK_API_TLS_CLIENT_CA_PATH"] = "/tls/ca.pem",
        };
        var config = new ConfigurationManager();

        // Act: перенос.
        TlsEndpoints.ApplyEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert: каждое имя env легло в свой конфиг-ключ; незаданные — нет.
        config["ValkeyWorker:Api:Tls:ServerCertPem"].Should().Be("PEM-CERT");
        config["ValkeyWorker:Api:Tls:ClientCaPath"].Should().Be("/tls/ca.pem");
        config["ValkeyWorker:Api:Tls:ServerKeyPem"].Should().BeNull();

        // Таблица покрывает все 6 секретов (arch/21 §8).
        TlsEndpoints.EnvBindings.Should().HaveCount(6);
    }
}
