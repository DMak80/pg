using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using PgWorker.IntegrationTests.E2e;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// Entrypoint ноды: материализация CA REST-TLS в /home/postgres/pgw-node-ca.pem
// + строка-путь PGW_NODE_CA в pgw-node.env (t22, arch/14 §2.1) — PEM в
// KEY=VALUE-файл не переносится. Прогон скрипта ДО supervisord: заглушка
// supervisord bind-mount'ится ПОВЕРХ /usr/bin/supervisord (exec идёт по
// абсолютному пути — PATH-подмена не работает), adduser добавляет postgres
// (alpine образа ноды его не имеет). Гейт PGW_TEST_DOCKER=1.
[Collection(NonE2eCollection.Name)]
public class NodeEntrypointRestCaTests
{
    private const string PythonImage = "python:3.12-alpine";

    [Fact]
    public async Task Entrypoint_MaterializesRestCaFileAndPathLine()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var tag = Guid.NewGuid().ToString("N")[..8];

        // Arrange: tmp-каталог с заглушкой supervisord и тестовым CA;
        // серт ноды с SAN 127.0.0.1 не нужен — entrypoint работает с PEM-строкой.
        var tmp = Directory.CreateTempSubdirectory($"pgw-it-epca-{tag}");
        var caPem = E2eTestPki.GenerateCa("epca").CaPem;
        var stub = Path.Combine(tmp.FullName, "supervisord");
        await File.WriteAllTextAsync(stub, "#!/bin/sh\nexit 0\n", ct);
        await E2eFixture.RunProcessAsync("chmod", ["+x", stub], ct);

        var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
        var runner = new ContainerBuilder(PythonImage)
            .WithName($"pgw-it-epca-{tag}")
            .WithCommand("sh", "-c",
                "adduser -D postgres && sh /tmp/entrypoint.sh; sleep 300")
            .WithEnvironment("SSL_RESTAPI_CA", caPem)
            // Заглушки остальных env скрипта (пустые строки допустимы).
            .WithEnvironment("HAPROXY_CONFIG", "")
            .WithEnvironment("DOORMAN_CONFIG", "")
            .WithEnvironment("PGW_ETCD", "http://etcd:2379")
            .WithEnvironment("PGW_MASTER_KEY", "/k")
            .WithEnvironment("PGW_NODE_HOST", "h1")
            .WithEnvironment("PGW_NODE_NAME", "n1a")
            .WithEnvironment("PGW_DOORMAN_PORT", "6432")
            .WithBindMount(stub, "/usr/bin/supervisord", AccessMode.ReadOnly)
            .WithBindMount(Path.Combine(root, "docker", "node", "docker-entrypoint.sh"),
                "/tmp/entrypoint.sh", AccessMode.ReadOnly)
            .Build();

        try
        {
            await runner.StartAsync(ct);
            // Дождаться выхода entrypoint-скрипта (заглушка supervisord exit 0;
            // дальше контейнер жив в sleep — exec-ассерты до удаления).
            var ran = await E2eFixture.WaitForAsync(async () =>
            {
                var outp = System.Text.Encoding.UTF8.GetString(
                    await runner.ReadFileAsync("/home/postgres/pgw-node.env", ct));
                return outp.Contains("PGW_NODE_CA=", StringComparison.Ordinal);
            }, TimeSpan.FromSeconds(15), ct);
            ran.Should().BeTrue("entrypoint обязан дописать PGW_NODE_CA после материализации ca-файла");

            // Assert: ca-файл == PEM байт-в-байт, права 600, владелец postgres.
            var caFile = System.Text.Encoding.UTF8.GetString(
                await runner.ReadFileAsync("/home/postgres/pgw-node-ca.pem", ct));
            caFile.TrimEnd('\n').Should().Be(caPem.TrimEnd('\n'),
                "PEM материализуется в файл буквально (реальные переносы строк)");
            var ls = await E2eFixture.RunProcessAsync("docker",
                ["exec", runner.Name, "sh", "-c",
                    "ls -l /home/postgres/pgw-node-ca.pem | awk '{print $1, $3}'"], ct);
            ls.Trim().Should().Be("-rw------- postgres",
                "ca-файл 600 и принадлежит postgres (паттерн HAPROXY_CONFIG)");

            // pgw-node.env: строка-путь есть, PEM в файл НЕ попадает.
            var envFile = System.Text.Encoding.UTF8.GetString(
                await runner.ReadFileAsync("/home/postgres/pgw-node.env", ct));
            envFile.Should().Contain("PGW_NODE_CA=/home/postgres/pgw-node-ca.pem");
            envFile.Should().NotContain("BEGIN CERTIFICATE",
                "PEM не переносится в KEY=VALUE-файл");
        }
        finally
        {
            // Teardown: собственный контейнер серии (own-only, префикс pgw-it-).
            await runner.DisposeAsync();
            tmp.Delete(recursive: true);
        }
    }
}
