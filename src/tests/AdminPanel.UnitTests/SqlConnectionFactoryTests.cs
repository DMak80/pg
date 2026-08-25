using AdminPanel.Core;
using AdminPanel.Probes;
using FluentAssertions;
using Npgsql;
using Xunit;

namespace AdminPanel.UnitTests;

// Построитель Npgsql-строки пробы (spec §3.6, §10.5): HostMap по каждому host:port,
// пароль из настроек, read-only + TargetSessionAttributes, толерантность к null-полям.
public class SqlConnectionFactoryTests
{
    private static ShardInfo Shard() => new(
        "s1", "host=s1a,s1b port=5432 dbname=demo user=postgres",
        ["s1a", "s1b"], 5432, "demo", "postgres", 1, "s1a:5432", [], null);

    [Fact]
    public void Build_MapsHostsPerEndpoint()
    {
        // Arrange: маппится только первый хост — у остальных порт всё равно явный.
        var options = new ProbesOptions
        {
            HostMap = new Dictionary<string, string> { ["s1a:5432"] = "127.0.0.1:5433" },
        };

        // Act
        var builder = SqlProbe.BuildConnectionString(Shard(), options);

        // Assert: эндпоинт-синтаксис Npgsql host:port у каждого хоста (spec §3.6).
        builder.Host.Should().Be("127.0.0.1:5433,s1b:5432");
    }

    [Fact]
    public void Build_MergesPassword()
    {
        // Arrange
        var withPassword = new ProbesOptions { Password = "secret" };
        var withoutPassword = new ProbesOptions { Password = "" };

        // Act
        var present = SqlProbe.BuildConnectionString(Shard(), withPassword);
        var absent = SqlProbe.BuildConnectionString(Shard(), withoutPassword);

        // Assert: пустой пароль — ключа нет (стенд trust, spec §3.6).
        present.Password.Should().Be("secret");
        absent.Password.Should().BeNullOrEmpty();
    }

    [Fact]
    public void Build_ReadOnlyAndSessionAttributes()
    {
        // Arrange
        var options = new ProbesOptions { TimeoutSeconds = 7 };

        // Act
        var builder = SqlProbe.BuildConnectionString(Shard(), options);

        // Assert: маршрутизация на мастера + теги панели (arch/02 §6.2, spec §3.6).
        // Npgsql 10: TargetSessionAttributes — string c libpq-значением "read-write"
        // (enum NpgsqlTargetSessionAttributes удалён в 10-й мажорной версии);
        // двойная защита от записи — сессионный SET в ProbeAsync (несовместимость
        // read-write-фильтра и connection-Options у PostgreSQL).
        builder.TargetSessionAttributes.Should().Be("read-write");
        builder.ApplicationName.Should().Be("adminpanel");
        builder.Timeout.Should().Be(7);
        builder.CommandTimeout.Should().Be(7);
        // Spilo включает SSL (self-signed); pg_hba требует hostssl — Require.
        builder.SslMode.Should().Be(SslMode.Require);
    }

    [Fact]
    public void Build_SingleHost_OmitsTargetSessionAttributes()
    {
        // Arrange: одиночный хост — Npgsql 10 отвергает read-write не-Any с одним
        // хостом (NotSupportedException), фильтровать некого.
        var shard = Shard() with { DsnHosts = ["s1a"] };

        // Act
        var builder = SqlProbe.BuildConnectionString(shard, new ProbesOptions());

        // Assert: ключ не ставится — идём на единственный хост.
        builder.TargetSessionAttributes.Should().BeNullOrEmpty();
        builder.Host.Should().Be("s1a:5432");
    }

    [Fact]
    public void Build_NullUserAndDb_Omitted()
    {
        // Arrange: битый DSN без dbname/user (DsnParser отдал null).
        var shard = Shard() with { DbName = null, User = null, Port = null };

        // Act
        var builder = SqlProbe.BuildConnectionString(shard, new ProbesOptions());

        // Assert: ключи не ставятся; порт по умолчанию 5432 в каждом эндпоинте.
        builder.Database.Should().BeNullOrEmpty();
        builder.Username.Should().BeNullOrEmpty();
        builder.Host.Should().Be("s1a:5432,s1b:5432");
    }

    [Fact]
    public void Build_PerHostPorts_EndpointsAndHostMap()
    {
        // Arrange: dsn PgWorker-стенда — port=15000,15001, хост local;
        // HostMap матчится по фактическому порту эндпоинта.
        var shard = Shard() with
        {
            DsnHosts = ["local", "local"],
            Port = null,
            DsnPorts = [15000, 15001],
        };
        var options = new ProbesOptions
        {
            HostMap = new Dictionary<string, string> { ["local:15000"] = "127.0.0.1:15000" },
        };

        // Act
        var builder = SqlProbe.BuildConnectionString(shard, options);

        // Assert: порты расклеены по хостам; замапленный хост подменён.
        builder.Host.Should().Be("127.0.0.1:15000,local:15001");
    }
}
