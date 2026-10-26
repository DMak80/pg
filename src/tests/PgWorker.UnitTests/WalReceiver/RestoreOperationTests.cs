using System.Diagnostics;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.WalReceiver;
using PgWorker.WalReceiver.Operations;
using Shared.Core;
using Xunit;

namespace PgWorker.UnitTests.WalReceiver;

// Инцидентная механика restore/drill в .NET (arch/19 §3.5, spec t26 §3.4,
// Р15 — 1:1 из bash RestoreJobCommand): сигналы (standby.signal →
// recovery.signal), override-строки auto.conf (в т.ч. target_time только при
// цели; latest — без строки), trust-строка pg_hba, порядок runner-вызовов
// (chown -R 101:101 → chmod 700 → pg_ctl start под setpriv с HOME=/tmp →
// psql-поллинг → LSN → pg_ctl stop -m fast → chown auto.conf), возврат
// pristine auto.conf без archive_mode/archive_command, снятие trust-строки,
// system_id из pg_controldata; fetch-wal: разбор env + строчный регистр
// сегмента. AAA-комментарии.
public class RestoreOperationTests
{
    private const string Id = "20260101000000Z";

    private static StartRestoreRequest Request(string? targetTime = null) => new(
        BackupId: Id,
        TargetTime: targetTime,
        RecoveryTimeoutSec: 60,
        S3: new S3Context(
            Endpoints: ["http://s3a:9000"],
            AccessKey: "ak",
            SecretKey: "sk",
            Bucket: "backups",
            Prefix: "c1/s1"));

    /// <summary>Фейк runner: пишет stdout по FileName (pg_controldata/psql),
    /// программируемый исход pg_ctl start; журнал вызовов для порядка.</summary>
    private sealed class FakeRunner : IProcessRunner
    {
        public readonly List<(string FileName, string Args, string Env)> Invocations = [];
        public int StartExitCode;
        public int PsqlPollFailsBeforeOk;

        public async Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(
            ProcessStartInfo psi, CancellationToken ct)
        {
            var args = string.Join(" ", psi.ArgumentList);
            var home = psi.EnvironmentVariables["HOME"];
            var env = home is not null ? $"HOME={home}" : "";
            Invocations.Add((psi.FileName, args, env));
            switch (psi.FileName)
            {
                case "psql":
                    if (PsqlPollFailsBeforeOk > 0 && args.Contains("pg_is_in_recovery"))
                    {
                        PsqlPollFailsBeforeOk--;
                        return (2, "", "connection refused");
                    }

                    return (0,
                        args.Contains("pg_is_in_recovery") ? "f" : "0/2000028", "");

                case "pg_controldata":
                    return (0, "Database system identifier:           7000000000000000001\n", "");

                case "setpriv":
                    // pg_ctl под setpriv: программируемый исход СТАРТА.
                    return args.Contains(" start") && StartExitCode != 0
                        ? (StartExitCode, "", "pg_ctl: could not start server")
                        : (0, "", "");

                default:
                    return (0, "", "");
            }
        }
    }

    /// <summary>Фейк IBackupS3: list одного backup_label; GET пишет dest.</summary>
    private sealed class FakeS3 : IBackupS3
    {
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
            string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(Result<IReadOnlyList<S3ObjectInfo>>.Success(
                new List<S3ObjectInfo> { new($"{prefix}backup_label", 60, DateTimeOffset.UnixEpoch) }));

        public Task<Result> GetObjectToFileAsync(
            string cluster, string shard, string key, string destFilePath, CancellationToken ct = default)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(destFilePath)!);
            File.WriteAllText(destFilePath,
                "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n");
            return Task.FromResult(Result.Success());
        }

        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(string cluster, string shard, string objectKey, CancellationToken ct = default) => throw new NotSupportedException();
    }

    /// <summary>Temp-PGDATA: pristine auto.conf (с archive_mode=on — Spilo-
    /// наследие источника), pg_hba.conf, standby.signal (набор с реплики).</summary>
    private static string NewPgdata()
    {
        var pgdata = Path.Combine(Path.GetTempPath(), $"t26-restore-{Guid.NewGuid():N}");
        Directory.CreateDirectory(Path.Combine(pgdata, "pgroot", "data"));
        File.WriteAllText(
            Path.Combine(pgdata, "pgroot", "data", "postgresql.auto.conf"),
            "archive_mode = 'on'\n");
        File.WriteAllText(
            Path.Combine(pgdata, "pgroot", "data", "pg_hba.conf"),
            "host all all all scram-sha-256\n");
        File.WriteAllText(Path.Combine(pgdata, "pgroot", "data", "standby.signal"), "");
        return pgdata;
    }

    private static RestoreOperation Create(
        string pgdata, FakeRunner runner, string? targetTime = null)
    {
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        return new RestoreOperation(Request(targetTime), runner, registry,
            pgdata: Path.Combine(pgdata, "pgroot", "data"), _ => new FakeS3());
    }

    [Fact]
    public async Task HappyPath_IncidentEditsAndCallOrder_ThenSucceeded()
    {
        // Arrange: temp-PGDATA + фейки.
        var root = NewPgdata();
        var pgdata = Path.Combine(root, "pgroot", "data");
        var runner = new FakeRunner();
        var op = Create(root, runner);

        // Act: полный пайплайн.
        await op.RunAsync(CancellationToken.None);

        // Assert: succeeded + restored_to_lsn + system_id.
        var snap = registrySnapshot(op);
        snap!.State.Should().Be("succeeded");
        snap.RestoredToLsn.Should().Be("0/2000028");
        snap.SystemId.Should().Be("7000000000000000001");
        snap.Phase.Should().Be("recovering");

        var autoConf = Path.Combine(pgdata, "postgresql.auto.conf");
        var hbaConf = Path.Combine(pgdata, "pg_hba.conf");

        // Assert: сигналы — standby удалён, recovery создан.
        File.Exists(Path.Combine(pgdata, "standby.signal")).Should().BeFalse();
        File.Exists(Path.Combine(pgdata, "recovery.signal")).Should().BeFalse(
            "recovery-остатки чистятся после итога");

        // Assert: auto.conf восстановлен из .orig — без archive_mode
        // (pristine ноды-источника вычищается), без override-строк.
        File.ReadAllText(autoConf).Should().NotContain("archive_mode");
        File.ReadAllText(autoConf).Should().NotContain("data_directory");
        File.Exists(autoConf + ".orig").Should().BeFalse();

        // Assert: trust-строка снята.
        File.ReadAllLines(hbaConf).Should().NotContain("local all all trust");

        // Assert: порядок runner-вызовов: chown -R 101:101 (volume) → chmod 700
        // → pg_ctl start (setpriv) → psql (poll) → psql (LSN) → pg_ctl stop →
        // chown auto.conf.
        var fileNames = runner.Invocations.Select(i => i.FileName).ToArray();
        fileNames.Should().ContainInOrder("chown", "chmod", "setpriv", "psql", "psql", "setpriv", "chown");
        var chown = runner.Invocations.First(i => i.FileName == "chown");
        chown.Args.Should().Be($"-R 101:101 {root}");
        var chmod = runner.Invocations.First(i => i.FileName == "chmod");
        chmod.Args.Should().Be($"700 {pgdata}");
        var start = runner.Invocations.First(i => i.FileName == "setpriv");
        start.Args.Should().StartWith("--reuid=101 --regid=101 --clear-groups pg_ctl");
        start.Args.Should().Contain("start");
        start.Args.Should().Contain("listen_addresses=''");
        start.Env.Should().Be("HOME=/tmp");
        // Assert: S3-контекст fetch-wal — в env процесса (секреты не в argv).
        var startPsiEnv = start.Env;
        start.Args.Should().NotContain("sk");

        // Cleanup.
        Directory.Delete(root, recursive: true);
    }

    [Fact]
    public async Task AutoConfOverrides_ContainFullSet_TargetTimeOnlyWhenSet()
    {
        // Arrange: target_time задан; перехват содержимого auto.conf —
        // через состояние после сбоя на старте postgres (override'ы уже
        // записаны,pg_ctl start fail → итог failed, файлы не чистятся —
        // bash 1:1: чистку сигналов делает только успешный путь/cancel).
        var root = NewPgdata();
        var pgdata = Path.Combine(root, "pgroot", "data");
        var runner = new FakeRunner { StartExitCode = 1 };
        var op = Create(root, runner, targetTime: "2026-01-02 03:04:05+03");

        // Act.
        await op.RunAsync(CancellationToken.None);

        // Assert: failed (старт не удался), но auto.conf несёт ВСЕ строки.
        var snap = registrySnapshot(op);
        snap!.State.Should().Be("failed");
        var autoConf = Path.Combine(pgdata, "postgresql.auto.conf");
        var auto = File.ReadAllText(autoConf);
        auto.Should().Contain($"data_directory = '{pgdata}'");
        auto.Should().Contain($"hba_file = '{pgdata}/pg_hba.conf'");
        auto.Should().Contain($"ident_file = '{pgdata}/pg_ident.conf'");
        auto.Should().Contain("wal_level = 'replica'");
        auto.Should().Contain("summarize_wal = 'off'");
        auto.Should().Contain("restore_command = '/opt/pgw-wal/walreceiver fetch-wal %f %p'");
        auto.Should().Contain("recovery_target_action = 'promote'");
        auto.Should().Contain("recovery_target_time = '2026-01-02 03:04:05+03'");
        auto.Should().Contain("shared_preload_libraries = ''");
        auto.Should().Contain("ssl = off");
        auto.Should().Contain("logging_collector = off");
        // Assert: trust-строка первая в pg_hba.
        File.ReadAllLines(Path.Combine(pgdata, "pg_hba.conf")).First()
            .Should().Be("local all all trust");
        // Assert: recovery.signal создан, standby.signal удалён.
        File.Exists(Path.Combine(pgdata, "recovery.signal")).Should().BeTrue();
        File.Exists(Path.Combine(pgdata, "standby.signal")).Should().BeFalse();

        Directory.Delete(root, recursive: true);
    }

    [Fact]
    public async Task LatestTarget_NoRecoveryTargetTimeLine()
    {
        // Arrange: target_time = null (latest — воркер маппит "" → null).
        var root = NewPgdata();
        var pgdata = Path.Combine(root, "pgroot", "data");
        var runner = new FakeRunner { StartExitCode = 1 };
        var op = Create(root, runner, targetTime: null);

        // Act.
        await op.RunAsync(CancellationToken.None);

        // Assert: строки recovery_target_time нет (конец WAL = promote).
        var auto = File.ReadAllText(Path.Combine(pgdata, "postgresql.auto.conf"));
        auto.Should().NotContain("recovery_target_time");
        Directory.Delete(root, recursive: true);
    }

    [Fact]
    public async Task PollRetries_PromoteWindow_BeforeOk()
    {
        // Arrange: первые две пробы psql — err (рестарт postmaster), затем f.
        var root = NewPgdata();
        var runner = new FakeRunner { PsqlPollFailsBeforeOk = 2 };
        var op = Create(root, runner);

        // Act.
        await op.RunAsync(CancellationToken.None);

        // Assert: promote-окно отретраилось — итог succeeded (бюджет не
        // исчерпан), psql-вызовов 3 (2 err + ok).
        registrySnapshot(op)!.State.Should().Be("succeeded");
        runner.Invocations.Count(i => i.FileName == "psql"
                && i.Args.Contains("pg_is_in_recovery")).Should().Be(3);
        Directory.Delete(root, recursive: true);
    }

    [Fact]
    public void FetchWal_NormalizeSegment_LowerInvariant()
    {
        // Arrange/Act/Assert: %f приходит ВЕРХНИМ hex — ключи S3 строчные.
        FetchWalCommand.NormalizeSegment("00000001000000000000000A")
            .Should().Be("00000001000000000000000a");
        FetchWalCommand.NormalizeSegment("00000002.HISTORY")
            .Should().Be("00000002.history");
    }

    [Fact]
    public void FetchWal_ParseEnv_FullContextAndMissing()
    {
        // Arrange: полный env-контекст (порядок endpoints сохраняется).
        var env = new Dictionary<string, string?>
        {
            ["PGW_S3_ENDPOINTS"] = "http://s3a:9000, http://s3b:9000",
            ["PGW_S3_BUCKET"] = "backups",
            ["PGW_S3_PREFIX"] = "c1/s1",
            ["PGW_S3_ACCESS_KEY"] = "ak",
            ["PGW_S3_SECRET_KEY"] = "sk",
            ["PGW_S3_CA_PEM"] = "-----BEGIN CERTIFICATE-----",
        };

        // Act.
        var context = FetchWalCommand.ParseEnv(k => env.TryGetValue(k, out var v) ? v : null);

        // Assert: контекст разобран целиком (секреты — из env, не argv).
        context.Should().NotBeNull();
        context!.Endpoints.Should().Equal("http://s3a:9000", "http://s3b:9000");
        context.Bucket.Should().Be("backups");
        context.Prefix.Should().Be("c1/s1");
        context.AccessKey.Should().Be("ak");
        context.SecretKey.Should().Be("sk");
        context.CaPem.Should().NotBeNull();

        // Arrange/Act/Assert: пустые обязательные — null (exit 1).
        FetchWalCommand.ParseEnv(_ => null).Should().BeNull();
    }

    // Snapshot читается из операции (реестр — приватный в тесте).
    private static OperationSnapshot? registrySnapshot(RestoreOperation op)
        => op.Snapshot;
}
