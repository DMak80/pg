using System.Diagnostics;
using FluentAssertions;
using PgWorker.WalReceiver.Operations;
using Xunit;

namespace PgWorker.UnitTests.WalReceiver;

// Node-операции приёмника (arch/25 №22): обеспечение pg_hba (arch/19 §7),
// pg_dump схемы (arch/14 §Move M1). HbaEnsure — файловая механика на tmpdir
// (пути NodePaths.Pgdata заменяются направляющим symlink? нет — операции
// читают константы NodePaths: файловая часть тестируется вставкой в секцию
// (InsertIntoPgHbaSection) и идемпотентностью логики через tmp-подмену root).
public class NodeOperationsTests
{
    /// <summary>Фейк runner: фиксированный исход + журнал invocations.</summary>
    private sealed class FakeRunner : IProcessRunner
    {
        public readonly List<ProcessStartInfo> Invocations = [];
        public int ExitCode;
        public string StdOut = "";
        public string StdErr = "";

        public async Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(
            ProcessStartInfo psi, CancellationToken ct)
        {
            Invocations.Add(psi);
            await Task.Delay(1, ct);
            return (ExitCode, StdOut, StdErr);
        }
    }

    // ── HbaEnsureOperation: вставка строки в секцию pg_hba ──

    // AAA: строка списка вставляется после «  pg_hba:» (секция postgresql
    // конфига Patroni), остальной текст не тронут
    [Fact]
    public void InsertIntoPgHbaSection_AppendsLineAfterSection()
    {
        // Arrange — Spilo-шаблонный конфиг (pg_hba — единственная секция)
        var text = """
            bootstrap:
              dcs:
                ttl: 30
            postgresql:
              parameters:
                shared_preload_libraries: 'bg_mon'
              pg_hba:
                - local   all             all                   trust
                - host    all             all                127.0.0.1/32       md5
            """;

        // Act
        var patched = HbaEnsureOperation.InsertIntoPgHbaSection(text);

        // Assert — строка сразу после секции, на уровне элементов списка
        var lines = patched.Split('\n');
        var idx = lines.Select((l, i) => (l, i)).First(x => x.l.TrimEnd() == "  pg_hba:").i;
        lines[idx + 1].Should().Be("  - hostssl replication backup_exec all scram-sha-256");
        patched.Should().Contain("- local   all             all                   trust",
            "the existing pg_hba entries are preserved");
    }

    // AAA: секции pg_hba нет — отказ (конфигурация ноды не Spilo-шаблонная)
    [Fact]
    public void InsertIntoPgHbaSection_NoSection_Throws()
    {
        // Arrange
        var text = "postgresql:\n  parameters:\n    work_mem: 4MB\n";

        // Act
        var act = () => HbaEnsureOperation.InsertIntoPgHbaSection(text);

        // Assert
        act.Should().Throw<ApplicationException>().WithMessage("*pg_hba section not found*");
    }

    // ── PgDumpOperation ──

    // AAA: pg_dump запускается с флагами 1:1 прежнего exec-варианта,
    // socket-коннектом под postgres; текст дампа — в ответе
    [Fact]
    public async Task PgDump_ValidBucket_RunsPgDumpWithSocketConn()
    {
        // Arrange
        var runner = new FakeRunner { StdOut = "-- dump text" };
        var op = new PgDumpOperation(runner);

        // Act
        var response = await op.RunAsync("bucket_42", "shop", CancellationToken.None);

        // Assert
        response.Ddl.Should().Be("-- dump text");
        var psi = runner.Invocations.Should().ContainSingle().Subject;
        psi.FileName.Should().Be("pg_dump");
        psi.ArgumentList.Should().BeEquivalentTo(
        [
            "--schema-only", "--no-owner", "--no-privileges", "--schema=bucket_42",
            "-h", "/var/run/postgresql", "-U", "postgres", "shop",
        ]);
    }

    // AAA: невалидное имя схемы — отказ до запуска процесса (инъекция в argv)
    [Theory]
    [InlineData("B;DROP SCHEMA x")]
    [InlineData("bucket-42")]
    [InlineData("42bucket")]
    public async Task PgDump_InvalidSchema_FailsBeforeProcess(string bad)
    {
        // Arrange
        var runner = new FakeRunner();
        var op = new PgDumpOperation(runner);

        // Act
        var act = () => op.RunAsync(bad, "shop", CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<ApplicationException>().WithMessage("*invalid bucket name*");
        runner.Invocations.Should().BeEmpty("nothing is executed for an invalid identifier");
    }

    // AAA: не-0 exit pg_dump — отказ с первой строкой stderr (диагностика тика)
    [Fact]
    public async Task PgDump_NonZeroExit_FailsWithStderr()
    {
        // Arrange
        var runner = new FakeRunner { ExitCode = 2, StdErr = "pg_dump: error: schema \"bucket_42\" does not exist\nmore" };
        var op = new PgDumpOperation(runner);

        // Act
        var act = () => op.RunAsync("bucket_42", "shop", CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<ApplicationException>()
            .WithMessage("*exit 2*pg_dump: error: schema \"bucket_42\" does not exist*");
    }
}
