using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using PgWorker.IntegrationTests.E2e;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// Entrypoint ноды: материализация CA REST-TLS в /home/postgres/pgw-node-ca.pem
// + строка-путь PGW_NODE_CA в pgw-node.env (arch/14 §2.1) — PEM в
// KEY=VALUE-файл не переносится. Прогон скрипта ДО supervisord: заглушки
// supervisord и pg_doorman bind-mount'ятся ПОВЕРХ реальных путей
// (exec идёт по абсолютным путям; python-образ не несёт пулера),
// adduser добавляет postgres (alpine образа ноды его не имеет). Пулер
// обязателен (arch/14 §8): DOORMAN_CONFIG_TEMPLATE — неконфиденциальный
// шаблон с плейсхолдером (проверки бинарника/шаблона entrypoint проходят,
// до supervisord дело не доходит). t05: openssl-генерации нет — серт
// пулера доставляет агент PgNode в tmpfs.
[Collection(NonE2eCollection.Name)]
public class NodeEntrypointRestCaTests
{
    private const string PythonImage = "python:3.12-alpine";

    [Fact]
    public async Task Entrypoint_MaterializesRestCaFileAndPathLine()
    {
        var ct = TestContext.Current.CancellationToken;
        var tag = Guid.NewGuid().ToString("N")[..8];

        // Arrange: tmp-каталог с заглушками supervisord/pg_doorman и
        // тестовым CA; серт ноды с SAN 127.0.0.1 не нужен — entrypoint
        // работает с PEM-строкой.
        var tmp = Directory.CreateTempSubdirectory($"pgw-it-epca-{tag}");
        var caPem = E2eTestPki.GenerateCa("epca").CaPem;
        var stub = Path.Combine(tmp.FullName, "supervisord");
        await File.WriteAllTextAsync(stub, "#!/bin/sh\nexit 0\n", ct);
        await E2eFixture.RunProcessAsync("chmod", ["+x", stub], ct);
        var doormanStub = Path.Combine(tmp.FullName, "pg_doorman");
        await File.WriteAllTextAsync(doormanStub, "#!/bin/sh\nexit 0\n", ct);
        await E2eFixture.RunProcessAsync("chmod", ["+x", doormanStub], ct);

        var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
        var runner = new ContainerBuilder(PythonImage)
            .WithName($"pgw-it-epca-{tag}")
            .WithCommand("sh", "-c",
                "adduser -D postgres && sh /tmp/entrypoint.sh; sleep 300")
            .WithEnvironment("SSL_RESTAPI_CA", caPem)
            // Заглушки остальных env скрипта.
            .WithEnvironment("HAPROXY_CONFIG", "")
            .WithEnvironment("DOORMAN_CONFIG_TEMPLATE",
                "pooler template ${PGW_SU_PASSWORD}")
            .WithEnvironment("PGW_ETCD", "http://etcd:2379")
            .WithEnvironment("PGW_MASTER_KEY", "/k")
            .WithEnvironment("PGW_NODE_HOST", "h1")
            .WithEnvironment("PGW_NODE_NAME", "n1a")
            .WithEnvironment("PGW_DOORMAN_PORT", "6432")
            .WithBindMount(stub, "/usr/bin/supervisord", AccessMode.ReadOnly)
            .WithBindMount(doormanStub, "/usr/local/bin/pg_doorman", AccessMode.ReadOnly)
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
            ran.Should().BeTrue("the entrypoint must append PGW_NODE_CA after materializing the ca file");

            // Assert: ca-файл == PEM байт-в-байт, права 600, владелец postgres.
            var caFile = System.Text.Encoding.UTF8.GetString(
                await runner.ReadFileAsync("/home/postgres/pgw-node-ca.pem", ct));
            caFile.TrimEnd('\n').Should().Be(caPem.TrimEnd('\n'),
                "the PEM is materialized into the file verbatim (real line breaks)");
            var ls = await E2eFixture.RunProcessAsync("docker",
                ["exec", runner.Name, "sh", "-c",
                    "ls -l /home/postgres/pgw-node-ca.pem | awk '{print $1, $3}'"], ct);
            ls.Trim().Should().Be("-rw------- postgres",
                "the ca file is 600 and owned by postgres (the HAPROXY_CONFIG pattern)");

            // pgw-node.env: строка-путь есть, PEM в файл НЕ попадает.
            var envFile = System.Text.Encoding.UTF8.GetString(
                await runner.ReadFileAsync("/home/postgres/pgw-node.env", ct));
            envFile.Should().Contain("PGW_NODE_CA=/home/postgres/pgw-node-ca.pem");
            envFile.Should().NotContain("BEGIN CERTIFICATE",
                "the PEM is not carried into a KEY=VALUE file");
        }
        finally
        {
            // Teardown: собственный контейнер серии (own-only, префикс pgw-it-).
            await runner.DisposeAsync();
            tmp.Delete(recursive: true);
        }
    }

    // Негативный fail-fast (arch/14 §8): в образе (python-заглушке) НЕТ
    // бинарника pg_doorman — маунт заглушки не делается — entrypoint обязан
    // погасить контейнер с ненулевым exit и диагностикой про обязательный
    // пулер, ДО supervisord.
    [Fact]
    public async Task Entrypoint_MissingDoormanBinary_ContainerExitsWithDiagnostic()
    {
        var ct = TestContext.Current.CancellationToken;
        var tag = Guid.NewGuid().ToString("N")[..8];

        // Arrange: заглушка supervisord маунтится, заглушка pg_doorman —
        // НЕТ (путь /usr/local/bin/pg_doorman в образе отсутствует);
        // DOORMAN_CONFIG_TEMPLATE — непустой стаб: отказ обязан случиться ровно
        // на проверке бинарника.
        var tmp = Directory.CreateTempSubdirectory($"pgw-it-epnb-{tag}");
        var stub = Path.Combine(tmp.FullName, "supervisord");
        await File.WriteAllTextAsync(stub, "#!/bin/sh\nexit 0\n", ct);
        await E2eFixture.RunProcessAsync("chmod", ["+x", stub], ct);

        var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
        var runner = new ContainerBuilder(PythonImage)
            .WithName($"pgw-it-epnb-{tag}")
            // Без sleep: контейнер гаснет вместе с отказавшим entrypoint.
            .WithCommand("sh", "-c", "adduser -D postgres && sh /tmp/entrypoint.sh")
            .WithEnvironment("DOORMAN_CONFIG_TEMPLATE",
                "pooler template ${PGW_SU_PASSWORD}")
            .WithBindMount(stub, "/usr/bin/supervisord", AccessMode.ReadOnly)
            .WithBindMount(Path.Combine(root, "docker", "node", "docker-entrypoint.sh"),
                "/tmp/entrypoint.sh", AccessMode.ReadOnly)
            .Build();

        try
        {
            await runner.StartAsync(ct);

            // Act/Assert — контейнер погас с exit 1 и диагностикой пулера.
            var (code, logs) = await ExitedWithLogsAsync(runner, ct);
            code.Should().Be(1, "the missing mandatory pooler binary must fail the start");
            logs.Should().Contain("pg_doorman binary is missing",
                "the start-failure diagnostic names the required DOORMAN_IMAGE build-arg");
        }
        finally
        {
            // Teardown: собственный контейнер серии.
            await runner.DisposeAsync();
            tmp.Delete(recursive: true);
        }
    }

    // Негативный fail-fast (arch/14 §8): бинарник заглушкой есть, но
    // DOORMAN_CONFIG_TEMPLATE пуст — entrypoint обязан погасить контейнер с
    // ненулевым exit и диагностикой про пустой шаблон, ДО supervisord.
    [Fact]
    public async Task Entrypoint_EmptyDoormanConfig_ContainerExitsWithDiagnostic()
    {
        var ct = TestContext.Current.CancellationToken;
        var tag = Guid.NewGuid().ToString("N")[..8];

        // Arrange: все заглушки на местах (бинарник есть) — env
        // DOORMAN_CONFIG_TEMPLATE НЕ ставится вовсе: отказ обязан случиться
        // ровно на проверке шаблона.
        var tmp = Directory.CreateTempSubdirectory($"pgw-it-epec-{tag}");
        var stub = Path.Combine(tmp.FullName, "supervisord");
        await File.WriteAllTextAsync(stub, "#!/bin/sh\nexit 0\n", ct);
        await E2eFixture.RunProcessAsync("chmod", ["+x", stub], ct);
        var doormanStub = Path.Combine(tmp.FullName, "pg_doorman");
        await File.WriteAllTextAsync(doormanStub, "#!/bin/sh\nexit 0\n", ct);
        await E2eFixture.RunProcessAsync("chmod", ["+x", doormanStub], ct);

        var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
        var runner = new ContainerBuilder(PythonImage)
            .WithName($"pgw-it-epec-{tag}")
            // Без sleep: контейнер гаснет вместе с отказавшим entrypoint.
            .WithCommand("sh", "-c", "adduser -D postgres && sh /tmp/entrypoint.sh")
            .WithBindMount(stub, "/usr/bin/supervisord", AccessMode.ReadOnly)
            .WithBindMount(doormanStub, "/usr/local/bin/pg_doorman", AccessMode.ReadOnly)
            .WithBindMount(Path.Combine(root, "docker", "node", "docker-entrypoint.sh"),
                "/tmp/entrypoint.sh", AccessMode.ReadOnly)
            .Build();

        try
        {
            await runner.StartAsync(ct);

            // Act/Assert — контейнер погас с exit 1 и диагностикой шаблона.
            var (code, logs) = await ExitedWithLogsAsync(runner, ct);
            code.Should().Be(1, "the empty mandatory pooler config must fail the start");
            logs.Should().Contain("DOORMAN_CONFIG_TEMPLATE env is empty",
                "the start-failure diagnostic names the missing DOORMAN_CONFIG_TEMPLATE env");
        }
        finally
        {
            // Teardown: собственный контейнер серии.
            await runner.DisposeAsync();
            tmp.Delete(recursive: true);
        }
    }

    // Дождаться выхода контейнера (отказ entrypoint гасит его) и вернуть
    // (exit-код, логи) — диагностика старт-отказа пишется в stdout контейнера.
    private static async Task<(int Code, string Logs)> ExitedWithLogsAsync(
        IContainer runner, CancellationToken ct)
    {
        var exited = await E2eFixture.WaitForAsync(async () =>
        {
            var status = await E2eFixture.RunProcessAsync("docker",
                ["inspect", "-f", "{{.State.Status}}", runner.Name], ct);
            return status.Trim() == "exited";
        }, TimeSpan.FromSeconds(15), ct);
        exited.Should().BeTrue("the entrypoint failure must terminate the container");

        var code = await E2eFixture.RunProcessAsync("docker",
            ["inspect", "-f", "{{.State.ExitCode}}", runner.Name], ct);
        var logs = await E2eFixture.RunProcessAsync("docker", ["logs", runner.Name], ct);
        return (int.Parse(code.Trim(), System.Globalization.CultureInfo.InvariantCulture), logs);
    }
}
