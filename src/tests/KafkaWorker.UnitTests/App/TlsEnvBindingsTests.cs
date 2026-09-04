using FluentAssertions;
using KafkaWorker.App.Api;
using Microsoft.Extensions.Configuration;

namespace KafkaWorker.UnitTests.App;

// Options-биндинг TLS (spec §8.1): KFW_API_TLS_* → конфиг-дерево; таблица
// EnvBindings — единственный источник соответствий (Program + тест).
public class TlsEnvBindingsTests
{
    [Fact]
    public void ApplyEnvOverrides_PemAndPathKeysMapped()
    {
        // Arrange: env-словарь без реального окружения (inject в чистую функцию).
        var env = new Dictionary<string, string>
        {
            ["KFW_API_TLS_CERT"] = "PEM-CERT",
            ["KFW_API_TLS_CLIENT_CA_PATH"] = "/tls/ca.pem",
        };
        var config = new ConfigurationManager();

        // Act: перенос.
        TlsEndpoints.ApplyEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert: каждое имя env легло в свой конфиг-ключ; незаданные — нет.
        config["KafkaWorker:Api:Tls:ServerCertPem"].Should().Be("PEM-CERT");
        config["KafkaWorker:Api:Tls:ClientCaPath"].Should().Be("/tls/ca.pem");
        config["KafkaWorker:Api:Tls:ServerKeyPem"].Should().BeNull();

        // Таблица покрывает все 6 секретов (spec §5.3 / arch/16 §8).
        TlsEndpoints.EnvBindings.Should().HaveCount(6);
    }
}
