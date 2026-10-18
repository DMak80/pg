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
    public void Build_BashScript_DownloadsAndVerifies()
    {
        // Act
        var cmd = VerifyJobCommand.Build();

        // Assert
        cmd.Should().HaveCount(3).And.HaveElementAt(0, "bash").And.HaveElementAt(1, "-c");
        var script = cmd[2];
        // t36: скачивание mc_copy_first с перебором нод пула ({A}-алиас).
        script.Should().Contain("mc_copy_first \"{A}/$S3_BUCKET/$PGW_BK_PREFIX/full/$PGW_BK_ID/\" \"$PGW_BK_STAGING_DIR/full/\"");
        script.Should().Contain("pg_verifybackup \"$PGW_BK_STAGING_DIR/full\"");
    }

    // AAA: протокол — phase-маркеры download/verify и result-JSON ok/error; exit 1 при провале
    [Fact]
    public void Build_ResultJsonProtocol_AndPhases()
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
    // AAA: скрипт материализует S3_CA_PEM → SSL_CERT_FILE (t42, arch/19 §7)
    [Fact]
    public void Build_MaterializesCaPem_And_SslCertFile()
    {
        VerifyJobCommand.Build()[2].Should().Contain("S3_CA_PEM").And.Contain("SSL_CERT_FILE");
    }
}
