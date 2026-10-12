using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using S3Worker.IntegrationTests.Etcd;

namespace S3Worker.IntegrationTests.Api;

// WAF-хост для метрик (порт MetricsApiFactory valkey): копия S3ApiFactory БЕЗ
// RemoveAll<IHostedService> — OTel-MeterProvider hosted-сервис, без него
// /metrics не экспортирует доменные инструменты. Циклы на пустом etcd тикают
// успешно и бесшумно.
public class S3MetricsApiFactory(S3EtcdFixture etcd) : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["S3Worker:Etcd:Endpoints:0"] = etcd.Endpoint,
            ["S3Worker:Bucket"] = "pgworker-backups",
            ["S3Worker:Docker:Hosts:0:Name"] = "local",
            ["S3Worker:Docker:Hosts:0:Endpoint"] = "unix:///var/run/does-not-exist.sock",
            ["S3Worker:Api:AdvertiseUrl"] = "https://localhost:9995",
            ["S3Worker:Api:EnableSeedEndpoint"] = "false",
            // WAF-транспорт in-memory — только без TLS (arch/24 §1.1).
            ["S3Worker:Api:Tls:AllowInsecureHttp"] = "true",
        }));
    }
}
