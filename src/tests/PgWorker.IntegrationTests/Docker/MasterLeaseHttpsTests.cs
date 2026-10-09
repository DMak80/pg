using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using PgWorker.Core.Templates;
using PgWorker.IntegrationTests.E2e;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// master-lease.py: local_role() по https /primary с верификацией серта по
// ca-файлу ноды (t22, arch/14 §2.1: REST :8008 — TLS-only, IP 127.0.0.1 —
// в SAN). Контейнер python:3.12-alpine: мини-https-сервер на 127.0.0.1:8008
// (серт тестового CA), скрипт запускается БЕЗ аргументов (ветка on_start →
// local_role); etcd мёртв — демон не может писать ключ, факт распознавания
// роли — pid-файл демона (master) либо тихий выход (replica/no-ca).
// Гейт PGW_TEST_DOCKER=1 (DockerTrait).
[Collection(NonE2eCollection.Name)]
public class MasterLeaseHttpsTests
{
    private const string PythonImage = "python:3.12-alpine";

    // Проба готовности: код /primary (503 — тоже живой ответ; urllib кидает
    // HTTPError на не-2xx — ловим и печатаем код).
    private const string ProbeScript = """
        import urllib.request, urllib.error, ssl
        ctx = ssl.create_default_context(cafile="/tmp/pki/ca.pem")
        try:
            print(urllib.request.urlopen("https://127.0.0.1:8008/primary",
                                         context=ctx, timeout=2).status)
        except urllib.error.HTTPError as e:
            print(e.code)
        """;

    // Мини-https-сервер: /primary → 200 (master-флаг стоит) / 503 (нет).
    private const string ServerScript = """
        import http.server, ssl, pathlib, sys
        flag = pathlib.Path("/tmp/primary-flag")
        class H(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/primary":
                    code = 200 if flag.exists() else 503
                    self.send_response(code)
                    self.end_headers()
                    self.wfile.write(b"{}")
                else:
                    self.send_response(404)
                    self.end_headers()
            def log_message(self, *a):
                pass
        srv = http.server.ThreadingHTTPServer(("127.0.0.1", 8008), H)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain("/tmp/pki/node.crt", "/tmp/pki/node.key")
        srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
        srv.serve_forever()
        """;

    private static async Task<IContainer> StartRunnerAsync(string tag, string dir)
    {
        var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
        var runner = new ContainerBuilder(PythonImage)
            .WithName($"pgw-it-mlhttps-{tag}")
            .WithCommand("sleep", "600")
            .WithBindMount(dir, "/tmp/pki", AccessMode.ReadOnly)
            .WithBindMount(Path.Combine(root, "docker", "node", "master-lease.py"),
                "/tmp/master-lease.py", AccessMode.ReadOnly)
            .WithEnvironment("PGW_ETCD", "http://host.docker.internal:1")
            .WithEnvironment("PGW_MASTER_KEY", $"/pgw-it/mlhttps-{tag}/master")
            .WithEnvironment("PGW_NODE_HOST", "node1a")
            .WithEnvironment("PGW_DOORMAN_PORT", "6432")
            .Build();
        await runner.StartAsync(TestContext.Current.CancellationToken);
        return runner;
    }

    private static async Task StartServerAsync(IContainer runner)
    {
        var ct = TestContext.Current.CancellationToken;
        // exec -d: сервер — долгоживущий процесс контейнера (detached).
        await E2eFixture.RunProcessAsync("docker",
            ["exec", "-d", runner.Name, "python3", "-u", "/tmp/pki/srv.py"], ct);
        var up = await E2eFixture.WaitForAsync(async () =>
        {
            try
            {
                var probe = await E2eFixture.RunProcessAsync("docker",
                    ["exec", runner.Name, "python3", "/tmp/pki/probe.py"], ct);
                return probe.Trim() is "200" or "503";
            }
            catch (ApplicationException)
            {
                return false; // сервер ещё не готов — проба ненулевым exit'ом падает
            }
        }, TimeSpan.FromSeconds(20), ct);
        up.Should().BeTrue("мини-https-сервер /primary обязан подняться (логи: docker exec … cat /tmp/srv.log)");
    }

    [Fact]
    public async Task LocalRole_HttpsPrimary200_MasterDaemonStarts()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var tag = Guid.NewGuid().ToString("N")[..8];

        // Arrange: тестовый CA + серт ноды SAN 127.0.0.1 (RestPki); сервер
        // отвечает 200 (нода — мастер).
        var dir = await MaterializePkiAsync(tag, "pgw-it-mlhttps-a", ct);
        using var __ = new DirCleanup(dir);
        await using var runner = await StartRunnerAsync(tag, dir);
        await StartServerAsync(runner);
        await E2eFixture.RunProcessAsync("docker",
            ["exec", runner.Name, "touch", "/tmp/primary-flag"], ct);

        // Act: on_start без аргументов — local_role по https с ca-файлом ноды.
        await E2eFixture.RunProcessAsync("docker", ["exec", runner.Name, "sh", "-c",
            "PGW_NODE_CA=/tmp/pki/ca.pem python3 -u /tmp/master-lease.py >/tmp/lease.log 2>&1"], ct);

        // Assert: роль master распознана — демон стартовал (pid-файл существует;
        // ключ не пишется — etcd мёртв, это ожидаемо).
        var started = await E2eFixture.WaitForAsync(async () =>
        {
            var outp = await E2eFixture.RunProcessAsync("docker", ["exec", runner.Name, "sh", "-c",
                "test -f /tmp/master-lease.pid && echo yes || echo no"], ct);
            return outp.Trim() == "yes";
        }, TimeSpan.FromSeconds(10), ct);
        started.Should().BeTrue("local_role обязан вернуть master по https /primary 200 " +
            "(логи: docker exec … cat /tmp/lease.log)");
    }

    [Fact]
    public async Task LocalRole_HttpsPrimary503_NoDaemon()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var tag = Guid.NewGuid().ToString("N")[..8];

        // Arrange: сервер без флага → /primary 503 (нода — реплика).
        var dir = await MaterializePkiAsync(tag, "pgw-it-mlhttps-b", ct);
        using var __ = new DirCleanup(dir);
        await using var runner = await StartRunnerAsync(tag, dir);
        await StartServerAsync(runner);

        // Act: on_start без аргументов.
        await E2eFixture.RunProcessAsync("docker", ["exec", runner.Name, "sh", "-c",
            "PGW_NODE_CA=/tmp/pki/ca.pem python3 -u /tmp/master-lease.py >/tmp/lease.log 2>&1"], ct);

        // Assert: роль replica — демона нет (pid-файл не появился).
        await Task.Delay(1500, ct);
        var pid = await E2eFixture.RunProcessAsync("docker", ["exec", runner.Name, "sh", "-c",
            "test -f /tmp/master-lease.pid && echo yes || echo no"], ct);
        pid.Trim().Should().Be("no", "реплика не запускает демон мастер-ключа");
    }

    [Fact]
    public async Task LocalRole_WithoutCa_QuietExit()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var tag = Guid.NewGuid().ToString("N")[..8];

        // Arrange: контейнер без https-сервера; PGW_NODE_CA не задан.
        var dir = await MaterializePkiAsync(tag, "pgw-it-mlhttps-c", ct);
        using var __ = new DirCleanup(dir);
        await using var runner = await StartRunnerAsync(tag, dir);

        // Act: on_start без аргументов и без ca-файла.
        await E2eFixture.RunProcessAsync("docker", ["exec", runner.Name, "sh", "-c",
            "python3 -u /tmp/master-lease.py >/tmp/lease.log 2>&1"], ct);

        // Assert: local_role → None (http-запроса нет) — лог-ошибка и выход.
        var log = await E2eFixture.RunProcessAsync("docker", ["exec", runner.Name, "cat", "/tmp/lease.log"], ct);
        log.Should().Contain("PGW_NODE_CA не задан");
        var pid = await E2eFixture.RunProcessAsync("docker", ["exec", runner.Name, "sh", "-c",
            "test -f /tmp/master-lease.pid && echo yes || echo no"], ct);
        pid.Trim().Should().Be("no");
    }

    // Материализация PKI: CA + серт ноды (SAN 127.0.0.1) + скрипт сервера.
    private static async Task<string> MaterializePkiAsync(string tag, string containerName, CancellationToken ct)
    {
        var dir = Directory.CreateTempSubdirectory($"pgw-it-mlpki-{tag}").FullName;
        var (caPem, caKeyPem) = E2eTestPki.GenerateCa("mlhttps");
        var (certPem, keyPem) = RestPki.IssueNodeCertificate(caPem, caKeyPem, "n1", "pgw-mlhttps-n1");
        await File.WriteAllTextAsync(Path.Combine(dir, "ca.pem"), caPem, ct);
        await File.WriteAllTextAsync(Path.Combine(dir, "node.crt"), certPem, ct);
        await File.WriteAllTextAsync(Path.Combine(dir, "node.key"), keyPem, ct);
        await File.WriteAllTextAsync(Path.Combine(dir, "srv.py"), ServerScript, ct);
        await File.WriteAllTextAsync(Path.Combine(dir, "probe.py"), ProbeScript, ct);
        _ = containerName;
        return dir;
    }

    // Временный PKI-каталог — удаление после теста.
    private sealed class DirCleanup(string path) : IDisposable
    {
        public void Dispose()
        {
            try { Directory.Delete(path, recursive: true); }
            catch (IOException) { /* лучшее усилие */ }
        }
    }
}
