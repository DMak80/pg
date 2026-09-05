using FluentAssertions;
using PgWorker.Docker.Engine;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// Парсинг endpoint-схем Engine API (arch/14 §2.2, t03): unix-сокет, tcp с
// дефолтом 2375, ssh c user@host и дефолтом 22; неизвестная схема — отказ.
public class EndpointSchemeTests
{
    [Theory]
    [InlineData("unix:///var/run/docker.sock", EndpointScheme.Unix, "/var/run/docker.sock", 0, null)]
    [InlineData("tcp://host1:2376", EndpointScheme.Tcp, "host1", 2376, null)]
    [InlineData("tcp://host1", EndpointScheme.Tcp, "host1", 2375, null)]
    [InlineData("ssh://root@dock1", EndpointScheme.Ssh, "dock1", 22, "root")]
    [InlineData("ssh://ops@dock1:2222", EndpointScheme.Ssh, "dock1", 2222, "ops")]
    [InlineData("ssh://dock1:2222", EndpointScheme.Ssh, "dock1", 2222, null)]
    public void Parse_Schemes_Defaults_And_User(string endpoint, string scheme, string host, int port, string? user)
    {
        // Act
        var parsed = EndpointScheme.Parse(endpoint);

        // Assert
        parsed.Scheme.Should().Be(scheme);
        parsed.Host.Should().Be(host);
        parsed.Port.Should().Be(port);
        parsed.User.Should().Be(user);
    }

    [Theory]
    [InlineData("http://host:2375")]
    [InlineData("dock1")]
    [InlineData("ssh://")]
    public void Parse_UnknownSchemeOrEmptyHost_FailFast(string endpoint)
    {
        // Act / Assert: конфигурационная ошибка обязана падать при старте, а не в рантайме тика
        Assert.Throws<ApplicationException>(() => EndpointScheme.Parse(endpoint));
    }
}
