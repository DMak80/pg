using System.Text.Json;
using ValkeyWorker.Docker.Engine;

namespace ValkeyWorker.UnitTests.Docker;

// RestartPolicy контейнера (t06-ревью): нода — канон arch/21 §2
// («unless-stopped»), TLS-helper — «no» (крах воркера в окне записи не
// оставляет вечно рестартуемого держателя volume).
public class ContainerSpecTests
{
    [Fact]
    public void BuildContainerBody_NodeSpec_DefaultUnlessStopped()
    {
        // Arrange — спека ноды без явной политики
        var spec = new ContainerSpec(
            "img", ["valkey-server"], [], "node1", null, null, null);

        // Act
        var body = DockerEngine.BuildContainerBody(spec);

        // Assert — HostConfig.RestartPolicy.Name = unless-stopped
        var policy = ReadRestartPolicy(body);
        policy.Should().Be("unless-stopped");
    }

    [Fact]
    public void BuildContainerBody_HelperSpec_NoRestart()
    {
        // Arrange — спека helper'а: эфемерный служебный контейнер
        var spec = new ContainerSpec(
            "img", ["sleep", "120"], [], "helper", null, null, null,
            Binds: (IReadOnlyList<string>?)new[] { "vwk-c-tls:/mnt" },
            RestartPolicy: "no");

        // Act
        var body = DockerEngine.BuildContainerBody(spec);

        // Assert — демон не рестартует helper (volume не держится занятым)
        ReadRestartPolicy(body).Should().Be("no");
    }

    // HostConfig.RestartPolicy.Name из тела create (словарь → JSON → чтение).
    private static string? ReadRestartPolicy(object body)
    {
        var json = JsonSerializer.Serialize(body);
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement
            .GetProperty("HostConfig")
            .GetProperty("RestartPolicy")
            .GetProperty("Name")
            .GetString();
    }
}
