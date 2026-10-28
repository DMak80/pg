using System.Diagnostics;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.WalReceiver;
using PgWorker.WalReceiver.Operations;
using Shared.Core;
using Xunit;

namespace PgWorker.UnitTests.WalReceiver;

// Инцидентная механика restore/drill в .NET (arch/19 §3.5, spec t26 §3.4,
// Р15 — перенос 1:1): сигналы (standby.signal →
// recovery.signal), override-строки auto.conf (в т.ч. target_time только при
// цели; latest — без строки), trust-строка pg_hba, порядок runner-вызовов
// (chmod 700 → pg_ctl start с HOME=/tmp →
// psql-поллинг → LSN → pg_ctl stop -m fast), возврат
// pristine auto.conf без archive_mode/archive_command, снятие trust-строки,
// system_id из pg_controldata; fetch-wal: разбор env + строчный регистр
// сегмента. AAA-комментарии.
public class RestoreOperationTests
{
    private const string Id = "20260101000000Z";

    private static PgWorker.WalReceiver.Operations.StartRestoreRequest Request(string? targetTime = null) => new(
        BackupId: Id,
        Id: null,
        TargetTime: targetTime,
        RecoveryTimeoutSec: 60,
        S3: new PgWorker.WalReceiver.Operations.S3Context(
            Endpoints: ["http://s3a:9000"],
            AccessKey: "ak",
            SecretKey: "sk",
            Bucket: "backups",
            Prefix: "c1/s1"));

    /// <summary>Фейк runner: пишет stdout по FileName (pg_controldata/psql),
    /// программируемый исход pg_ctl start; журнал вызовов для порядка;
    /// PsqlPollHang — psql-поллинг висит до отмены токена (cancel API).</summary>
    private sealed class FakeRunner : IProcessRunner
    {
        public readonly List<(string FileName, string Args, string Env)> Invocations = [];
        public int StartExitCode;
        public int PsqlPollFailsBeforeOk;
        public bool PsqlPollHang;

        public async Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(
            ProcessStartInfo psi, CancellationToken ct)
        {
            var args = string.Join(" ", psi.ArgumentList);
            // Журнал env: HOME + S3-контекст fetch-wal (PGW_S3_*) — ассерты
            // «секреты в env процесса, не в argv» проверяются по факту.
            var env = string.Join(";",
                psi.EnvironmentVariables.Keys.Cast<string>()
                    .Where(k => k == "HOME" || k.StartsWith("PGW_S3_", StringComparison.Ordinal))
                    .Select(k => $"{k}={psi.EnvironmentVariables[k]}"));
            Invocations.Add((psi.FileName, args, env));
            switch (psi.FileName)
            {
                case "psql":
                    if (PsqlPollHang && args.Contains("pg_is_in_recovery"))
                        await Task.Delay(Timeout.InfiniteTimeSpan, ct); // OperationCanceledException
                    if (PsqlPollFailsBeforeOk > 0 && args.Contains("pg_is_in_recovery"))
                    {
                        PsqlPollFailsBeforeOk--;
                        return (2, "", "connection refused");
                    }

                    return (0,
                        args.Contains("pg_is_in_recovery") ? "f" : "0/2000028", "");

                case "pg_controldata":
                    return (0, "Database system identifier:           7000000000000000001\n", "");

                case "pg_ctl":
                    // pg_ctl (процесс приёмника уже postgres): программируемый
                    // исход СТАРТА.
                    return args.Contains(" start") && StartExitCode != 0
                        ? (StartExitCode, "", "pg_ctl: could not start server")
                        : (0, "", "");

                default:
                    return (0, "", "");
            }
        }
    }

    /// <summary>Фейк IBackupS3: list одного backup_label; GET пишет dest.
    /// DownloadedKeys — композированные ключи запросов (ассерт состава ключей
    /// ловит задвоение префикса: полный obj.Key вместо относительного).</summary>
    private sealed class FakeS3 : IBackupS3
    {
        public readonly List<string> DownloadedKeys = [];
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
            // Композированный ключ — ассерт состава ключей ловит задвоение
            // префикса (полный obj.Key вместо относительного).
            DownloadedKeys.Add($"{cluster}/{shard}/{key}");
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
        string pgdata, FakeRunner runner, string? targetTime = null, FakeS3? s3 = null)
    {
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        return new RestoreOperation(Request(targetTime), runner, registry,
            pgdata: Path.Combine(pgdata, "pgroot", "data"), _ => s3 ?? new FakeS3());
    }

    [Fact]
    public async Task HappyPath_IncidentEditsAndCallOrder_ThenSucceeded()
    {
        // Arrange: temp-PGDATA + фейки.
        var root = NewPgdata();
        var pgdata = Path.Combine(root, "pgroot", "data");
        var runner = new FakeRunner();
        var s3 = new FakeS3();
        var op = Create(root, runner, s3: s3);

        // Act: полный пайплайн.
        await op.RunAsync(CancellationToken.None);

        // Assert: succeeded + restored_to_lsn + system_id.
        var snap = registrySnapshot(op);
        snap!.State.Should().Be("succeeded");
        snap.RestoredToLsn.Should().Be("0/2000028");
        snap.SystemId.Should().Be("7000000000000000001");
        snap.Phase.Should().Be("recovering");
        // Assert: ключ скачивания — композиция cluster/shard + относительного
        // ключа листинга (полный obj.Key давал бы задвоение c1/s1/c1/s1/...).
        s3.DownloadedKeys.Should().BeEquivalentTo([$"c1/s1/full/{Id}/backup_label"]);

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

        // Assert: порядок runner-вызовов: chmod 700 → pg_ctl start (прямой —
        // процесс приёмника уже postgres) → psql (poll) → psql (LSN) →
        // pg_ctl stop; chown в приёмнике ноды недоступен и не нужен.
        var fileNames = runner.Invocations.Select(i => i.FileName).ToArray();
        fileNames.Should().ContainInOrder("chmod", "pg_ctl", "psql", "psql", "pg_ctl");
        var chmod = runner.Invocations.First(i => i.FileName == "chmod");
        chmod.Args.Should().Be($"700 {pgdata}");
        var start = runner.Invocations.First(i => i.FileName == "pg_ctl");
        start.Args.Should().Contain("start");
        start.Args.Should().Contain("listen_addresses=''");
        start.Env.Should().Contain("HOME=/tmp");
        // Assert: S3-контекст fetch-wal — в env процесса (postgres наследует до
        // restore_command; секреты не в argv, Р13).
        start.Env.Should().Contain("PGW_S3_ENDPOINTS=http://s3a:9000");
        start.Env.Should().Contain("PGW_S3_BUCKET=backups");
        start.Env.Should().Contain("PGW_S3_PREFIX=c1/s1");
        start.Env.Should().Contain("PGW_S3_ACCESS_KEY=ak");
        start.Env.Should().Contain("PGW_S3_SECRET_KEY=sk");
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

    // AAA: операторская отмена в recovery-поллинге (cancel API — внешний канал
    // мимо воркера) — снимок state=cancelled, НЕ failed («операции не было»,
    // arch/19 §3.5; воркер отличает её от vanished: удаляет запись заявки и
    // сносит volume); чистку сигналов НЕ делаем (контейнер снесёт воркер);
    // терминальный снимок персистентен и не активен — гвард пропускает новую
    // операцию.
    [Fact]
    public async Task CancelDuringRecoveryPoll_StateCancelled_NotFailed()
    {
        // Arrange: psql-поллинг висит до отмены; операция доходит до поллинга.
        var root = NewPgdata();
        var pgdata = Path.Combine(root, "pgroot", "data");
        var runner = new FakeRunner { PsqlPollHang = true };
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        var op = new RestoreOperation(Request(), runner, registry,
            pgdata, _ => new FakeS3());
        var run = op.RunAsync(CancellationToken.None);
        SpinWait.SpinUntil(() => runner.Invocations.Any(i =>
                    i.FileName == "psql" && i.Args.Contains("pg_is_in_recovery")),
                TimeSpan.FromSeconds(10))
            .Should().BeTrue("the operation reaches the recovery poll before the cancel");

        // Act: cancel API гасит операцию (pg_ctl stop best-effort — фейк 0).
        await op.CancelAsync();
        await run;

        // Assert: снимок cancelled (НЕ failed) с однострочной причиной.
        registrySnapshot(op)!.State.Should().Be("cancelled");
        registrySnapshot(op)!.Error.Should().Be("cancelled by operator");
        // Assert: чистку сигналов НЕ делали (воркер снесёт volume целиком).
        File.Exists(Path.Combine(pgdata, "recovery.signal")).Should().BeTrue();
        // Assert: терминальный снимок не активен — гвард принимает новую
        // операцию (повторный GET отдаёт снимок cancelled — не resurrect).
        registry.TryBegin(new OperationSnapshot(
                "20260102000000Z", "running", "downloading"))
            .Should().BeTrue("a terminal snapshot is not an active operation");
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

    // AAA: dest (%p postgres) в границах корня данных проходит — относительный
    // (%p, cwd = PGDATA) нормализуется против корня, абсолютный — под корнем.
    [Fact]
    public void EnsureDestUnderRoot_AcceptsDestInsideRoot()
    {
        var root = FetchWalCommand.PgDataRoot;

        // Act
        var relative = FetchWalCommand.EnsureDestUnderRoot("pg_wal/RECOVERYXLOG", root);
        var history = FetchWalCommand.EnsureDestUnderRoot("pg_wal/00000002.history", root);
        var absolute = FetchWalCommand.EnsureDestUnderRoot(
            $"{FetchWalCommand.PgDataRoot}/pg_wal/RECOVERYXLOG", root);

        // Assert — нормализованный полный путь остался под корнем
        relative.Should().Be($"{FetchWalCommand.PgDataRoot}/pg_wal/RECOVERYXLOG");
        history.Should().Be($"{FetchWalCommand.PgDataRoot}/pg_wal/00000002.history");
        absolute.Should().Be($"{FetchWalCommand.PgDataRoot}/pg_wal/RECOVERYXLOG");
    }

    // AAA: dest вне корня данных — отказ (CWE-22): выход через «..» и
    // абсолютный путь вне PGDATA — ArgumentException (RunAsync → exit 1).
    [Fact]
    public void EnsureDestUnderRoot_RejectsDestOutsideRoot()
    {
        Assert.Throws<ArgumentException>(() =>
            FetchWalCommand.EnsureDestUnderRoot("../../../../etc/passwd", FetchWalCommand.PgDataRoot));
        Assert.Throws<ArgumentException>(() =>
            FetchWalCommand.EnsureDestUnderRoot("pg_wal/../../etc/passwd", FetchWalCommand.PgDataRoot));
        Assert.Throws<ArgumentException>(() =>
            FetchWalCommand.EnsureDestUnderRoot("/etc/passwd", FetchWalCommand.PgDataRoot));
        Assert.Throws<ArgumentException>(() =>
            FetchWalCommand.EnsureDestUnderRoot("/tmp/seg", FetchWalCommand.PgDataRoot));
    }

    /// <summary>Фейк IBackupS3 fetch-wal-прохода: программируемый исход GET
    /// (успех / not found / транспортная ошибка).</summary>
    private sealed class FakeFetchS3(Result outcome) : IBackupS3
    {
        public Task<Result> GetObjectToFileAsync(
            string cluster, string shard, string key, string destFilePath, CancellationToken ct = default)
            => Task.FromResult(outcome);

        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(string cluster, string shard, string objectKey, CancellationToken ct = default) => throw new NotSupportedException();
    }

    private static Result NotFound() =>
        Result.Failed(new ApplicationException("S3 get-to-file c1/s1/wal/x: not found"));

    private static Result Transport() =>
        Result.Failed(new ApplicationException("S3 get-to-file c1/s1/wal/x: connection reset"));

    // AAA: сегмент найден на ВТОРОЙ ноде пула — exit 0, перебор до найденного
    [Fact]
    public async Task FetchOnce_FoundOnSecondEndpoint_ExitsZero()
    {
        // Arrange — первая нода: not found; вторая: успех
        var clients = new List<IBackupS3> { new FakeFetchS3(NotFound()), new FakeFetchS3(Result.Success()) };

        // Act
        var (exitCode, transport) = await FetchWalCommand.FetchOnceAsync(
            clients, "c1", "s1", "00000001000000000000000A", "/tmp/fetch-wal-test", CancellationToken.None);

        // Assert
        exitCode.Should().Be(0);
        transport.Should().BeNull();
    }

    // AAA: «сегмента нет» на ВСЕХ endpoints — exit 1 БЕЗ transportError
    // (конец WAL — сразу, без ретрая)
    [Fact]
    public async Task FetchOnce_NotFoundEverywhere_IsEndOfWal()
    {
        // Arrange
        var clients = new List<IBackupS3> { new FakeFetchS3(NotFound()), new FakeFetchS3(NotFound()) };

        // Act
        var (exitCode, transport) = await FetchWalCommand.FetchOnceAsync(
            clients, "c1", "s1", "seg", "/tmp/fetch-wal-test", CancellationToken.None);

        // Assert — транспортного сбоя нет: ретрай не нужен (конец WAL)
        exitCode.Should().Be(1);
        transport.Should().BeNull();
    }

    // AAA: транспортный сбой отличим от «сегмента нет» — transportError несёт
    // причину (RunAsync ретраит 3×2 с и exit 1 c диагностикой в stderr)
    [Fact]
    public async Task FetchOnce_TransportFailure_IsDistinguishable()
    {
        // Arrange — первая нода not found, вторая транспортный сбой:
        // ответ «сегмента нет» НЕдостоверен (вторая нода не ответила по сути)
        var clients = new List<IBackupS3> { new FakeFetchS3(NotFound()), new FakeFetchS3(Transport()) };

        // Act
        var (exitCode, transport) = await FetchWalCommand.FetchOnceAsync(
            clients, "c1", "s1", "seg", "/tmp/fetch-wal-test", CancellationToken.None);

        // Assert
        exitCode.Should().Be(1);
        transport.Should().NotBeNull().And.Contain("connection reset",
            "a transport failure must be retried, not read as end-of-WAL");
    }

    // AAA: транспортный ретрай ограничен ЛИБО лимитом попыток, ЛИБО жёстким
    // дедлайном вызова (arch/19 §3.5): первый транспортный сбой при запасе
    // лимита и дедлайна — ретрай; последняя попытка — стоп; дедлайн наступил
    // — стоп при неисчерпанном лимите (мёртвая нода не растягивает вызов
    // сверх бюджета, промоут восстановления не уезжает из бюджета операции)
    [Fact]
    public void ShouldRetryTransport_LimitedByAttemptsOrDeadline()
    {
        // Arrange — старт вызова и тики «теперь»
        var start = Environment.TickCount64;

        // Act + Assert — первая попытка при запасе обоих лимитов: ретрай
        FetchWalCommand.ShouldRetryTransport(1, start, start).Should().BeTrue(
            "the first transport attempt with both budgets intact is retried");
        // исчерпан лимит попыток: стоп даже при запасе дедлайна
        FetchWalCommand.ShouldRetryTransport(
            FetchWalCommand.TransportAttempts, start, start).Should().BeFalse(
            "the transport retry limit stops the call even with the deadline intact");
        // наступил дедлайн вызова: стоп при неисчерпанном лимите попыток
        FetchWalCommand.ShouldRetryTransport(
            1, start, start + FetchWalCommand.CallDeadline.Ticks).Should().BeFalse(
            "the call deadline stops the call even with attempts left (arch/19 §3.5)");
    }

    // Snapshot читается из операции (реестр — приватный в тесте).
    private static OperationSnapshot? registrySnapshot(RestoreOperation op)
        => op.Snapshot;
}
