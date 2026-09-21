using System.Text.Json;
using FluentAssertions;
using Shared.Docker;
using Xunit;

namespace Shared.Docker.UnitTests.Engine;

// Тело create супер-спеки ContainerSpec (t07): RestartPolicy (нода —
// «unless-stopped», TLS-helper — «no»), флаг ResetEntrypoint, label-пара,
// опциональность Env.
public class ContainerSpecTests
{
    [Fact]
    public void BuildContainerBody_NodeSpec_DefaultUnlessStopped()
    {
        // Arrange — спека ноды без явной политики (vwk-узел: Cmd без сброса)
        var spec = new ContainerSpec("img", [], "node1", Cmd: ["valkey-server"]);

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
        var spec = new ContainerSpec("img", [], "helper",
            Cmd: ["sleep", "120"],
            Binds: new[] { "vwk-c-tls:/mnt" },
            RestartPolicy: "no");

        // Act
        var body = DockerEngine.BuildContainerBody(spec);

        // Assert — демон не рестартует helper (volume не держится занятым)
        ReadRestartPolicy(body).Should().Be("no");
    }

    // pg-семантика (t03): Cmd + ResetEntrypoint → ENTRYPOINT образа сброшен.
    [Fact]
    public void BuildContainerBody_CmdWithResetEntrypoint_SetsEmptyEntrypoint()
    {
        // Arrange — WAL-агент бэкапов поверх образа с ENTRYPOINT джоба
        var spec = new ContainerSpec("pgworker-backup:test", [], "job",
            Cmd: ["bash", "-c", "pg_receivewal"],
            ResetEntrypoint: true);

        // Act
        var body = DockerEngine.BuildContainerBody(spec);

        // Assert
        var root = ToJson(body);
        root.GetProperty("Entrypoint").GetArrayLength().Should().Be(0);
        root.GetProperty("Cmd")[0].GetString().Should().Be("bash");
    }

    // kfw/vwk-семантика: Cmd — аргументы образного entrypoint (ключа нет).
    [Fact]
    public void BuildContainerBody_CmdWithoutFlag_CmdAsArguments()
    {
        // Arrange
        var spec = new ContainerSpec("pgworker-node:dev", [], "node1", Cmd: ["postgres", "-c", "x=y"]);

        // Act
        var body = DockerEngine.BuildContainerBody(spec);

        // Assert — Cmd есть, ключа Entrypoint нет
        var root = ToJson(body);
        root.GetProperty("Cmd")[0].GetString().Should().Be("postgres");
        root.TryGetProperty("Entrypoint", out _).Should().BeFalse();
    }

    [Fact]
    public void BuildContainerBody_LabelPair_SetsLabels()
    {
        // Arrange — полная пара: ключ — домен
        var spec = new ContainerSpec("img", [], "node1", LabelKey: "pgworker", Label: "shop");

        // Act
        var body = DockerEngine.BuildContainerBody(spec);

        // Assert
        var labels = ToJson(body).GetProperty("Labels");
        labels.GetProperty("pgworker").GetString().Should().Be("shop");
    }

    [Fact]
    public void BuildContainerBody_NoLabel_NoLabelsField()
    {
        // Arrange — оба null
        var spec = new ContainerSpec("img", [], "node1");

        // Act
        var body = DockerEngine.BuildContainerBody(spec);

        // Assert
        ToJson(body).TryGetProperty("Labels", out _).Should().BeFalse();
    }

    // Смешанный случай: неполная пара не пишется вовсе (встречается только в
    // переносимых тестах; домены всегда передают пару).
    [Theory]
    [InlineData("label-only")]
    [InlineData("key-only")]
    public void BuildContainerBody_HalfLabelPair_NoLabelsField(string mode)
    {
        // Arrange
        var spec = mode == "label-only"
            ? new ContainerSpec("img", [], "node1", Label: "shop")
            : new ContainerSpec("img", [], "node1", LabelKey: "pgworker");

        // Act
        var body = DockerEngine.BuildContainerBody(spec);

        // Assert
        ToJson(body).TryGetProperty("Labels", out _).Should().BeFalse();
    }

    // vwk-контейнеры без env не меняются: Env=null → поле в теле нет.
    [Fact]
    public void BuildContainerBody_EnvNull_EnvFieldOmitted()
    {
        // Arrange — vwk-спека (Env не передаётся)
        var spec = new ContainerSpec("img", [], "node1", Cmd: ["valkey-server"]);

        // Act
        var body = DockerEngine.BuildContainerBody(spec);

        // Assert
        ToJson(body).TryGetProperty("Env", out _).Should().BeFalse();
    }

    // HostConfig.RestartPolicy.Name из тела create (словарь → JSON → чтение).
    private static string? ReadRestartPolicy(object body)
    {
        return ToJson(body)
            .GetProperty("HostConfig")
            .GetProperty("RestartPolicy")
            .GetProperty("Name")
            .GetString();
    }

    private static JsonElement ToJson(object body)
    {
        var json = JsonSerializer.Serialize(body);
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }
}
