using System.Text.Json;
using FluentAssertions;
using KafkaWorker.Provisioning.Processes;

namespace KafkaWorker.UnitTests.Provisioning;

/// <summary>
/// Юнит-тесты сборки CLI-вызова kafka-reassign-partitions (spec t02 §6,
/// arch/16 §2.4): bootstrap INTERNAL-listener, канонический reassignment.json,
/// SASL/PLAIN properties, sh -c exec-команда без апострофов в данных.
/// </summary>
public sealed class ReassignCliTests
{
    [Fact]
    public void Bootstrap_внутренний_listener()
    {
        // Arrange: имена живых брокеров (docker-DNS alias в kfw-net-<C>).

        // Act
        var bootstrap = ReassignCli.Bootstrap(["broker1", "broker2"]);

        // Assert: INTERNAL-listener 9092, запятая-join.
        Assert.Equal("broker1:9092,broker2:9092", bootstrap);
    }

    [Fact]
    public void BuildAssignmentJson_канонический_формат()
    {
        // Arrange: один move ("t", p0, реплики [1,2,3]).

        // Act
        var json = ReassignCli.BuildAssignmentJson([new ReassignMove("t", 0, [1, 2, 3])]);

        // Assert: формат KIP-455 — version, partitions[topic/partition/
        // replicas/log_dirs], длина log_dirs == replicas.
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal(1, root.GetProperty("version").GetInt32());
        var part = Assert.Single(root.GetProperty("partitions").EnumerateArray());
        Assert.Equal("t", part.GetProperty("topic").GetString());
        Assert.Equal(0, part.GetProperty("partition").GetInt32());
        Assert.Equal([1, 2, 3], part.GetProperty("replicas").EnumerateArray().Select(e => e.GetInt32()));
        var dirs = part.GetProperty("log_dirs").EnumerateArray().Select(e => e.GetString()).ToList();
        Assert.Equal(["any", "any", "any"], dirs);
    }

    [Fact]
    public void BuildAdminProperties_SaslSslWithPemTruststore()
    {
        // Arrange: admin-креда + CA-PEM.
        // Act: properties для --command-config (arch/16 §2.4).
        var props = ReassignCli.BuildAdminProperties("admin", "AdminSecret0123456789AAAAAAA", "CAPEM");

        // Assert: SASL_SSL + JAAS admin + PEM-truststore файлом.
        props.Should().Be(
            "security.protocol=SASL_SSL\n"
            + "sasl.mechanism=PLAIN\n"
            + """sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required """
            + $"""username="admin" password="AdminSecret0123456789AAAAAAA";""" + "\n"
            + "ssl.truststore.type=PEM\n"
            + "ssl.truststore.location=/tmp/kfw-ca.pem");
    }

    [Fact]
    public void BuildExecCommand_WritesCaPemFileThenProperties()
    {
        // Arrange: CA-PEM из трёх строк (BEGIN/body/END — без апострофов и \).
        var caPem = "-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----";

        // Act: sh -c команда.
        var cmd = ReassignCli.BuildExecCommand([new ReassignMove("t", 0, [1])], "broker1:9092", "admin", "p", caPem);

        // Assert: первым — файл CA (printf '%s\n' по строкам), затем properties, затем CLI.
        cmd.Should().HaveCount(3);
        cmd[2].Should().StartWith(
            "printf '%s\\n%s\\n%s\\n' '-----BEGIN CERTIFICATE-----' 'ZmFrZQ==' '-----END CERTIFICATE-----' > /tmp/kfw-ca.pem && ");
        cmd[2].Should().Contain("> /tmp/kfw-cmd.properties &&");
        cmd[2].Should().Contain("KAFKA_HEAP_OPTS=-Xmx256m /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server broker1:9092");
    }

    [Fact]
    public void BuildExecCommand_без_апострофов_в_данных()
    {
        // Arrange: один move, INTERNAL-bootstrap, креды admin + CA.
        var moves = new[] { new ReassignMove("t", 0, [1, 2, 3]) };

        // Act
        var cmd = ReassignCli.BuildExecCommand(moves, "broker1:9092,broker2:9092", "admin", "p", "CAPEM");

        // Assert: ["sh","-c", <одна строка>] — printf файлов (префикс kfw-)
        // + CLI c KAFKA_HEAP_OPTS=-Xmx256m; JSON-подстрока без апострофов
        // (printf-обёртка безопасна).
        Assert.Equal("sh", cmd[0]);
        Assert.Equal("-c", cmd[1]);
        Assert.Equal(3, cmd.Count);
        Assert.DoesNotContain('\n', cmd[2]);
        Assert.Contains("/opt/kafka/bin/kafka-reassign-partitions.sh", cmd[2]);
        Assert.Contains("--execute", cmd[2]);
        Assert.Contains("--bootstrap-server broker1:9092", cmd[2]);
        Assert.Contains("--reassignment-json-file /tmp/kfw-reassign.json", cmd[2]);
        Assert.Contains("--command-config /tmp/kfw-cmd.properties", cmd[2]);
        Assert.Contains("KAFKA_HEAP_OPTS=-Xmx256m", cmd[2]);

        var jsonStart = cmd[2].IndexOf("'{\"version\"", StringComparison.Ordinal);
        Assert.True(jsonStart >= 0);
        var jsonEnd = cmd[2].IndexOf("}'", jsonStart, StringComparison.Ordinal);
        Assert.True(jsonEnd > jsonStart);
        var jsonPayload = cmd[2][jsonStart..(jsonEnd + 2)];
        Assert.DoesNotContain("'", jsonPayload[1..^1]);
    }
}
