using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Inline-команда контейнера агента (arch/19 §2/§3): механика шиппера версионируется
// кодом воркера; секреты — env контейнера, не строка команды и не argv процессов.
public class WalAgentCommandTests
{
    [Fact]
    public void Build_bash_с_скриптом()
    {
        // Arrange / Act
        var cmd = WalAgentCommand.Build();

        // Assert
        cmd.Should().HaveCount(3);
        cmd[0].Should().Be("bash");
        cmd[1].Should().Be("-c");
        cmd[2].Should().ContainAll(
            "pg_receivewal", "--slot=", "mc cp", "rm -f",
            "*.partial" /* AC3: фильтр .partial */,
            "pgwbkp/$S3_BUCKET" /* alias из env MC_HOST_pgwbkp, без alias set */,
            "PG_HOST", "PG_PASSWORD", "SLOT");
    }

    [Fact]
    public void Build_скрипт_без_S3_кредов_и_alias_set_в_команде()
    {
        // Arrange / Act — креды уходят ТОЛЬКО env-строкой MC_HOST (ревью Ф4-2 №5),
        // mc alias set не вызывается: секреты не попадают в argv процессов (ps)
        var script = WalAgentCommand.Build()[2];

        // Assert
        script.Should().NotContain("mc alias set");
        script.Should().NotContain("$S3_ACCESS_KEY");
        script.Should().NotContain("$S3_SECRET_KEY");
        script.Should().NotContain("minioadmin"); // никаких литеральных секретов
    }

    [Fact]
    public void Build_скрипт_не_содержит_литеральных_PG_секретов()
    {
        // Arrange / Act
        var script = string.Join(" ", WalAgentCommand.Build());

        // Assert — env-имена, но никакой интерполяции значений в Cmd:
        // нет литеральных host/password (только переменные, раскрываемые в контейнере)
        script.Should().NotContainAny(["host=localhost", "host=host.docker.internal", "password=pgw"]);
        // conninfo собирается ВНУТРИ контейнера из env, не в строке команды
        script.Should().Contain("\"host=$PG_HOST port=$PG_PORT user=$PG_USER password=$PG_PASSWORD dbname=$PG_DBNAME");
    }

    [Fact]
    public void Build_грузит_history_обязательно()
    {
        // Arrange / Act — history не матчится *.partial-фильтром → попадает в mc cp
        var script = WalAgentCommand.Build()[2];

        // Assert — AC3: фильтр строго по .partial, расширения .history не исключаются
        script.Should().Contain("! -name '*.partial'");
        script.Should().NotContain("! -name '*.history'");
    }

    [Fact]
    public void Build_умирает_при_смерти_pg_receivewal()
    {
        // Arrange / Act — смерть приёмника обязана гасить контейнер (restart-контур
        // супервиза: exited/restarting → пересоздание воркером)
        var script = WalAgentCommand.Build()[2];

        // Assert
        script.Should().Contain("kill -0").And.Contain("wait");
    }

    [Fact]
    public void Build_квота_staging_при_заданной()
    {
        // Arrange / Act
        var script = WalAgentCommand.Build()[2];

        // Assert — guard «нет места» (arch/19 §6): du-проверка → exit
        script.Should().Contain("STAGING_QUOTA_BYTES").And.Contain("du -sb");
    }

    [Fact]
    public void Build_пишет_в_wal_префикс_шарда()
    {
        // Arrange / Act
        var script = WalAgentCommand.Build()[2];

        // Assert — layout arch/19 §5: <C>/<X>/wal/<name>
        script.Should().Contain("/$CLUSTER/$SHARD/wal/");
    }

    [Fact]
    public void McHost_собирает_URL_с_URL_escape_кредов()
    {
        // Arrange / Act — mc читает MC_HOST_<alias>: scheme://access:secret@authority;
        // escape обязателен: секреты per-install могут содержать спецсимволы URL
        var host = WalAgentCommand.McHost("http://localhost:9000", "ak", "sk/p@ss");

        // Assert
        host.Should().Be("http://ak:sk%2Fp%40ss@localhost:9000");
    }

    [Fact]
    public void McHost_именованный_переменной_агента()
    {
        // Arrange / Act / Assert — имя env-переменной = alias шиппера (контракт with AgentEnv Task 9)
        WalAgentCommand.EnvMcHostVariable.Should().Be("MC_HOST_pgwbkp");
    }
}
