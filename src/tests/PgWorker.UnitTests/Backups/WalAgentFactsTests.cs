using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Parsing;
using Shared.Docker;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Факты agents per-node из листинга драйвера (t27 §3.5, arch/19 §4, ревью
// Фазы 7 F3): exited-контейнер обязан отражаться Exited — permanent-выход
// приёмника виден наблюдаемости до пересоздания супервизом следующего тика,
// а не маскируется running по desired-списку.
public class WalAgentFactsTests
{
    // Arrange-хелпер: контейнер per-node имени в заданном состоянии.
    private static DockerContainer Container(string cluster, string shard, string node, string state)
        => new($"id-{node}", [BackupAgentNames.Container(cluster, shard, node)], state, "img");

    [Fact]
    public void Build_FromListing_Running_Exited_Absent()
    {
        // Arrange — листинг: агент мастера running, агенты sync exited,
        // третьей ноды контейнера нет вовсе.
        var (cluster, shard) = ("c1", "shard1");
        var listed = new[]
        {
            Container(cluster, shard, "shard1a", "running"),
            Container(cluster, shard, "shard1b", "exited"),
        };

        // Act — факты по нодам portalloc шарда.
        var facts = WalStreamProcess.BuildAgentFacts(
            cluster, shard, ["shard1a", "shard1b", "shard1c"], listed);

        // Assert — контракт §4: running → Running, exited → Exited, нет → Absent.
        facts.Should().BeEquivalentTo(
        [
            new WalAgentState("shard1a", WalAgentPresence.Running),
            new WalAgentState("shard1b", WalAgentPresence.Exited),
            new WalAgentState("shard1c", WalAgentPresence.Absent),
        ]);
    }

    [Fact]
    public void Build_MatchesOldNameFormat_WithSlash()
    {
        // Arrange — движок отдал имя с ведущей «/» (устаревший формат листинга).
        var (cluster, shard) = ("c1", "shard1");
        var listed = new[]
        {
            new DockerContainer("id", [$"/{BackupAgentNames.Container(cluster, shard, "shard1a")}"], "exited", "img"),
        };

        // Act
        var facts = WalStreamProcess.BuildAgentFacts(cluster, shard, ["shard1a"], listed);

        // Assert — «/»-литерал не ломает матч: exited виден, не absent.
        facts.Should().ContainSingle(f =>
            f.Node == "shard1a" && f.State == WalAgentPresence.Exited);
    }

    // AAA: AgentEnv передаёт S3_CA_PEM при заданном пути; без пути — не передаёт;
    // списки пула — в S3_ENDPOINTS/S3_READ_ENDPOINTS (t36, arch/19 §3).
    [Fact]
    public void AgentEnv_CaBundle_PassedCorrectly()
    {
        var caPath = Path.Combine(Path.GetTempPath(), $"ca-{Guid.NewGuid():N}.pem");
        File.WriteAllText(caPath, "PEM-BODY");
        var withCa = WalStreamProcess.AgentEnv(
            new BackupsRuntimeOptions { S3CaBundlePath = caPath }, "c1", "s1", "slot", "h", 5432, "pw",
            ["http://n1:9000", "http://n2:9000"], ["http://n1:9000"], "ak", "sk");
        var withoutCa = WalStreamProcess.AgentEnv(
            new BackupsRuntimeOptions { S3CaBundlePath = null }, "c1", "s1", "slot", "h", 5432, "pw",
            ["http://n1:9000"], ["http://n1:9000"], "ak", "sk");
        withCa["S3_CA_PEM"].Should().Be("PEM-BODY");
        withoutCa.Should().NotContainKey("S3_CA_PEM");
        withCa["S3_ENDPOINTS"].Should().Be("http://n1:9000,http://n2:9000");
        withCa["S3_READ_ENDPOINTS"].Should().Be("http://n1:9000");
        withCa["S3_ACCESS_KEY"].Should().Be("ak");
        withCa["S3_SECRET_KEY"].Should().Be("sk");
        withCa.Should().NotContainKey("S3_ENDPOINT");
        withCa.Should().NotContainKey("S3_REGION");
        withCa.Should().NotContainKey("S3_PATHSTYLE");
    }
}
