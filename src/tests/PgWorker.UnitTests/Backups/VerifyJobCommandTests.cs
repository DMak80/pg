using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Inline-команда verify-джоба (arch/19 §2/§5, t04): mc cp → pg_verifybackup;
// протокол t02 — result-JSON в stdout + exit-код; фазы download/verify.
public class VerifyJobCommandTests
{
    // AAA: Cmd = bash -c script; скрипт качает full/<id>/ 1:1 и зовёт pg_verifybackup
    [Fact]
    public void Build_BashСкрипт_КачаетИВерифицирует()
    {
        // Act
        var cmd = VerifyJobCommand.Build();

        // Assert
        cmd.Should().HaveCount(3).And.HaveElementAt(0, "bash").And.HaveElementAt(1, "-c");
        var script = cmd[2];
        script.Should().Contain("mc cp --recursive \"pgw/$PGW_BK_S3_BUCKET/$PGW_BK_PREFIX/full/$PGW_BK_ID/\" \"$PGW_BK_STAGING_DIR/full/\"");
        script.Should().Contain("pg_verifybackup \"$PGW_BK_STAGING_DIR/full\"");
    }

    // AAA: протокол — phase-маркеры download/verify и result-JSON ok/error; exit 1 при провале
    [Fact]
    public void Build_ПротоколResultJson_иФазы()
    {
        // Act
        var script = VerifyJobCommand.Build()[2];

        // Assert
        script.Should().Contain("\"phase\":\"download\"").And.Contain("\"phase\":\"verify\"");
        script.Should().Contain("\"ok\":true");
        script.Should().Contain("exit 1"); // FAIL() — провал фазы
        script.Should().Contain("tr -d '\"'"); // JSON-безопасность ошибки — образец FAIL() t02
    }

    // AAA: McHost — scheme://ak:sk@authority с URL-escape кредов (секреты не в argv)
    [Fact]
    public void McHost_ФорматИEscape()
    {
        // Act
        var value = VerifyJobCommand.McHost("http://minio:9000", "ak/1", "sk:2");

        // Assert
        value.Should().Be("http://ak%2F1:sk%3A2@minio:9000");
    }
}
