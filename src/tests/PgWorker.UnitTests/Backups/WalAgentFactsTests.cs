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
    public void Build_Из_Листинга_Running_Exited_Absent()
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
    public void Build_Матчит_Старый_Формат_Имени_С_Косой()
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
}
