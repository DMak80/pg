using System.Net;
using FluentAssertions;
using PgWorker.NodeTools;

namespace PgWorker.NodeTools.UnitTests;

// Контракты pgwnodetool (t25-ревью, п.17): разбор CLI, guard'ы etcd-endpoints
// и путей, setdefault-семантика env-файла, классификация адресов. Команды с
// сетью/демонами — в docker-серии (MasterLeaseFailover/HttpsTests).
public class ContractTests
{
    // CommandLine: позиционная команда + именованные параметры.
    [Fact]
    public void CommandLine_PositionalAndNamed()
    {
        var cli = new CommandLine(["master-lease", "--env-file", "/tmp/e.env", "--pid-file", "/tmp/p.pid"]);
        cli.Positionals.Should().ContainSingle().Which.Should().Be("master-lease");
        cli.Value("env-file").Should().Be("/tmp/e.env");
        cli.Value("pid-file").Should().Be("/tmp/p.pid");
        cli.Value("daemon").Should().BeNull();
        cli.Has("daemon").Should().BeFalse();
    }

    [Fact]
    public void CommandLine_IntAndList()
    {
        var cli = new CommandLine(["rolecheck", "--pg-port", "5433", "--etcd-endpoints", "http://a:2379,http://b:2379"]);
        cli.Int("pg-port", 5432).Should().Be(5433);
        cli.List("etcd-endpoints", "http://etcd:2379").Should()
            .BeEquivalentTo(["http://a:2379", "http://b:2379"]);
    }

    // EndpointGuard: только http(s), без userinfo/fragment, без публичных IP.
    [Fact]
    public void EndpointGuard_AcceptsPrivateHttpEndpoints()
    {
        var endpoints = EndpointGuard.ValidateEtcdEndpoints(
        [
            "http://etcd:2379",
            "http://127.0.0.1:2379",
            "https://10.0.0.5:2379",
        ]);
        endpoints.Should().HaveCount(3);
    }

    [Theory]
    [InlineData("ftp://etcd:2379")]
    [InlineData("http://user:pass@etcd:2379")]
    [InlineData("http://etcd:2379/#frag")]
    [InlineData("http://8.8.8.8:2379")]
    [InlineData("not a url")]
    public void EndpointGuard_RejectsUnsafeEndpoints(string endpoint)
        => Assert.Throws<ArgumentException>(() => EndpointGuard.ValidateEtcdEndpoints([endpoint]));

    // EnvironmentGuard: pid-файл обязан жить под корнем.
    [Fact]
    public void EnvironmentGuard_FileUnderRoot()
    {
        EnvironmentGuard.EnsureFileUnderRoot("/tmp/master-lease.pid", "/tmp", "pid file");
        Assert.Throws<ArgumentException>(() =>
            EnvironmentGuard.EnsureFileUnderRoot("/var/master-lease.pid", "/tmp", "pid file"));
    }

    // EnvironmentGuard (мульти-корневой): попадание хотя бы в один корень —
    // ок; путь вне всех корней и выход через «..» — отказ (CWE-22).
    // AAA: arrange — корни прод (/home/postgres) и тестов (/tmp); act —
    // валидация путей; assert — в границах проходит, вне — ArgumentException.
    [Fact]
    public void EnvironmentGuard_FileUnderRoots()
    {
        string[] roots = ["/home/postgres", "/tmp"];

        EnvironmentGuard.EnsureFileUnderRoots("/home/postgres/pgw-node.env", roots, "env file");
        EnvironmentGuard.EnsureFileUnderRoots("/tmp/pgw-node.env", roots, "env file");

        Assert.Throws<ArgumentException>(() =>
            EnvironmentGuard.EnsureFileUnderRoots("/etc/passwd", roots, "env file"));
        Assert.Throws<ArgumentException>(() =>
            EnvironmentGuard.EnsureFileUnderRoots("/home/postgres/../etc/passwd", roots, "env file"));
        Assert.Throws<ArgumentException>(() =>
            EnvironmentGuard.EnsureFileUnderRoots("/tmp/../etc/passwd", roots, "env file"));
    }

    // EnvFileSource: setdefault — живой env сильнее файла.
    [Fact]
    public void EnvFileSource_SetdefaultSemantics()
    {
        var path = Path.Combine(Path.GetTempPath(), $"pgwnodetool-env-{Guid.NewGuid():N}.env");
        try
        {
            Environment.SetEnvironmentVariable("PGW_TEST_PRESENT", "from-env");
            File.WriteAllText(path, "PGW_TEST_PRESENT=from-file\nPGW_TEST_MISSING=from-file\n");
            _ = new EnvFileSource(path);
            Environment.GetEnvironmentVariable("PGW_TEST_PRESENT").Should().Be("from-env");
            Environment.GetEnvironmentVariable("PGW_TEST_MISSING").Should().Be("from-file");
        }
        finally
        {
            File.Delete(path);
            Environment.SetEnvironmentVariable("PGW_TEST_PRESENT", null);
            Environment.SetEnvironmentVariable("PGW_TEST_MISSING", null);
        }
    }

    // NetworkAddress: loopback/private — доверенный контур, публичные — нет.
    [Theory]
    [InlineData("127.0.0.1", true)]
    [InlineData("10.1.2.3", true)]
    [InlineData("192.168.0.1", true)]
    [InlineData("8.8.8.8", false)]
    public void NetworkAddress_Classification(string ip, bool expected)
        => NetworkAddress.IsLoopbackOrPrivate(IPAddress.Parse(ip)).Should().Be(expected);

    // Отсутствующий env-файл — не ошибка (ручной запуск без entrypoint).
    [Fact]
    public void EnvFileSource_MissingFile_IsQuiet()
    {
        var ex = Record.Exception(() => new EnvFileSource("/tmp/definitely-missing-env-file"));
        ex.Should().BeNull();
    }
}
