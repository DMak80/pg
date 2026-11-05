using System.Diagnostics;
using System.Text;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Tar;
using PgWorker.PgNode;
using PgWorker.PgNode.Operations;
using Shared.Core;
using Xunit;

namespace PgWorker.UnitTests.PgNode;

// Инцидентная механика restore/drill в.NET (arch/19 §3.5, spec §3.4,
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

    private static PgWorker.PgNode.Operations.StartRestoreRequest Request(string? targetTime = null) => new(
        BackupId: Id,
        Id: null,
        TargetTime: targetTime,
        RecoveryTimeoutSec: 60,
        S3: new PgWorker.PgNode.Operations.S3Context(
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

    /// <summary>Фейк S3 набора: ОДИН tar-объект full/&lt;id&gt;.tar (writer шага 2):
    /// HEAD — существование/размер, стрим — байты объекта, Range — срез.
    /// Ranges — запрошенные диапазоны (ассерт фиксации endpoint'а/границ).</summary>
    private sealed class FakeTarS3 : IBackupS3, IStreamObjectS3
    {
        public FakeTarS3(byte[]? tar = null, bool objectExists = true)
        {
            Tar = tar ?? BuildTar();
            ObjectExists = objectExists;
        }

        public byte[] Tar { get; }

        public bool ObjectExists { get; }

        public List<(string Key, long Start, long End)> Ranges { get; } = [];

        public List<string> HeadKeys { get; } = [];

        public Task<Result<(bool Exists, long SizeBytes)>> HeadObjectAsync(
            string cluster, string shard, string key, CancellationToken ct)
        {
            HeadKeys.Add($"{cluster}/{shard}/{key}");
            return Task.FromResult(Result<(bool Exists, long SizeBytes)>.Success(
                ObjectExists ? (true, (long)Tar.Length) : (false, 0L)));
        }

        public Task<Result<(Stream Body, long Size)>> GetObjectStreamAsync(
            string cluster, string shard, string key, CancellationToken ct)
            => Task.FromResult(Result<(Stream Body, long Size)>.Success(
                ((Stream)new MemoryStream(Tar), (long)Tar.Length)));

        public Task<Result<byte[]>> GetObjectRangeAsync(
            string cluster, string shard, string key, long start, long end, CancellationToken ct)
        {
            Ranges.Add(($"{cluster}/{shard}/{key}", start, end));
            return Task.FromResult(Result<byte[]>.Success(Tar[(int)start..(int)(end + 1)]));
        }

        // IBackupS3 — restore читает только стрим/Range/HEAD.
        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectMd5Async(string key, byte[] data, string md5Hex, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(string cluster, string shard, string objectKey, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> GetObjectToFileAsync(string cluster, string shard, string key, string destFilePath, CancellationToken ct = default) => throw new NotSupportedException();
    }

    // Tar-объект тестового набора: backup_label (валидный START WAL LOCATION),
    // файл, каталог и WAL-сегмент (распаковка в PGDATA).
    private static byte[] BuildTar()
    {
        var buffer = new MemoryStream();
        using var writer = new TarStreamWriter(buffer);
        var label = Encoding.ASCII.GetBytes(
            "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n" +
            "CHECKPOINT LOCATION: 0/2000028\nBACKUP METHOD: streamed\n");
        writer.WriteEntryAsync(
            new TarEntry("backup_label", TarTypeFlags.File, label.Length, 0x1A0, ""), label, default)
            .GetAwaiter().GetResult();
        var version = Encoding.ASCII.GetBytes("18\n");
        writer.WriteEntryAsync(
            new TarEntry("PG_VERSION", TarTypeFlags.File, version.Length, 0x1A4, ""), version, default)
            .GetAwaiter().GetResult();
        writer.WriteEntryAsync(
            new TarEntry("pg_wal/", TarTypeFlags.Dir, 0, 0x1ED, ""), [], default)
            .GetAwaiter().GetResult();
        var segment = new byte[16];
        writer.WriteEntryAsync(
            new TarEntry("pg_wal/000000010000000000000002", TarTypeFlags.File, segment.Length, 0x1A0, ""),
            segment, default).GetAwaiter().GetResult();
        writer.FinishAsync(default).GetAwaiter().GetResult();
        return buffer.ToArray();
    }

    /// <summary>Temp-PGDATA: pristine auto.conf (с archive_mode=on — Spilo-
    /// наследие источника), pg_hba.conf, standby.signal (набор с реплики).</summary>
    private static string NewPgdata()
    {
        var pgdata = Path.Combine(Path.GetTempPath(), $"restore-{Guid.NewGuid():N}");
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
        string pgdata, FakeRunner runner, string? targetTime = null, FakeTarS3? s3 = null)
    {
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        return new RestoreOperation(Request(targetTime), runner, registry,
            pgdata: Path.Combine(pgdata, "pgroot", "data"), _ => s3 ?? new FakeTarS3());
    }

    [Fact]
    public async Task HappyPath_IncidentEditsAndCallOrder_ThenSucceeded()
    {
        // Arrange: temp-PGDATA + фейки.
        var root = NewPgdata();
        var pgdata = Path.Combine(root, "pgroot", "data");
        var runner = new FakeRunner();
        var s3 = new FakeTarS3();
        var op = Create(root, runner, s3: s3);

        // Act: полный пайплайн.
        await op.RunAsync(CancellationToken.None);

        // Assert: succeeded + restored_to_lsn + system_id.
        var snap = registrySnapshot(op);
        snap!.State.Should().Be("succeeded");
        snap.RestoredToLsn.Should().Be("0/2000028");
        snap.SystemId.Should().Be("7000000000000000001");
        snap.Phase.Should().Be("recovering");
        // Assert: HEAD/стрим — ОДИН объект full/<id>.tar (единственный ключ
        // набора, spec t01 §6.1); набор распакован в PGDATA.
        s3.HeadKeys.Should().Equal($"c1/s1/full/{Id}.tar");
        File.ReadAllText(Path.Combine(pgdata, "backup_label"))
            .Should().Contain("START WAL LOCATION");
        File.ReadAllText(Path.Combine(pgdata, "PG_VERSION")).TrimEnd().Should().Be("18");
        File.Exists(Path.Combine(pgdata, "pg_wal", "000000010000000000000002"))
            .Should().BeTrue("the tar entries are extracted into PGDATA");

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
        var op = Create(root, runner, targetTime: "2026-01-02 03:04:05+03:00");

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
        auto.Should().Contain("restore_command = '/opt/pgw-node/pgnode fetch-wal %f %p'");
        auto.Should().Contain("recovery_target_action = 'promote'");
        auto.Should().Contain("recovery_target_time = '2026-01-02 03:04:05+03:00'");
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
            pgdata, _ => new FakeTarS3());
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

    // AAA: объекта full/<id>.tar нет ни на одном endpoint → failed/downloading
    // (transient-семантика прежняя), причина — object not found.
    [Fact]
    public async Task TarObjectNotFound_AllEndpointsMiss_FailedDownloading()
    {
        // Arrange — HEAD отвечает «нет объекта» на всех целях команды.
        var root = NewPgdata();
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        var op = new RestoreOperation(Request(), new FakeRunner(), registry,
            pgdata: Path.Combine(root, "pgroot", "data"),
            _ => new FakeTarS3(objectExists: false));

        // Act
        await op.RunAsync(CancellationToken.None);

        // Assert
        var snap = op.Snapshot!;
        snap.State.Should().Be("failed");
        snap.Phase.Should().Be("downloading");
        snap.Error.Should().Contain("not found");
        Directory.Delete(root, recursive: true);
    }

    // AAA: объект-мусор (не tar, короткий EOF) → failed/downloading однострочной
    // причиной: обрыв потока до заголовков неотличим от транспорта (transient);
    // tar-slip-нарушения (имя/симлинк) — permanent, см. тесты ниже.
    [Fact]
    public async Task CorruptTar_FailedDownloading()
    {
        // Arrange — байты без валидных tar-заголовков.
        var root = NewPgdata();
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        var op = new RestoreOperation(Request(), new FakeRunner(), registry,
            pgdata: Path.Combine(root, "pgroot", "data"),
            _ => new FakeTarS3(Encoding.ASCII.GetBytes("not a tar archive at all")));

        // Act
        await op.RunAsync(CancellationToken.None);

        // Assert — transient-маркер downloading, одна строка причины
        var snap = op.Snapshot!;
        snap.State.Should().Be("failed");
        snap.Phase.Should().Be("downloading");
        snap.Error.Should().NotContain("\n");
        Directory.Delete(root, recursive: true);
    }

    // AAA (L4/tar-slip): абсолютное имя записи → permanent-отказ ДО файловой
    // операции (иначе имя перекрыло бы PGDATA); файл вне корня не создан.
    [Fact]
    public async Task AbsoluteEntryName_RejectedPermanent()
    {
        // Arrange — tar с абсолютным именем записи.
        var root = NewPgdata();
        var tar = BuildRawTar((new TarEntry("/tmp/pgw-escape", TarTypeFlags.File, 3, 0x1A4, ""),
            "bad"u8.ToArray()));
        var snap = await RunRestoreAsync(root, tar);

        // Assert — вердикт о данных, вне корня ничего не создано.
        snap.State.Should().Be("failed");
        snap.Phase.Should().Be("recovering");
        snap.Error.Should().Contain("not relative");
        File.Exists("/tmp/pgw-escape").Should().BeFalse();
        Directory.Delete(root, recursive: true);
    }

    // AAA: штатная форма tar `pg_basebackup` — имена с ведущим `./` и
    // пустыми сегментами — нормализуются и распаковываются (не tar-slip).
    [Fact]
    public async Task DotSlashPrefixedNames_Accepted()
    {
        // Arrange — backup_label и файл с префиксом "./".
        var root = NewPgdata();
        var label = Encoding.ASCII.GetBytes(
            "START WAL LOCATION: 0/2000028 (file 000000010000000000000002)\n");
        var tar = BuildRawTar(
            (new TarEntry("./backup_label", TarTypeFlags.File, label.Length, 0x1A0, ""), label),
            (new TarEntry("./PG_VERSION", TarTypeFlags.File, 3, 0x1A4, ""), "18\n"u8.ToArray()));
        var snap = await RunRestoreAsync(root, tar);

        // Assert — распаковка прошла, файлы на месте.
        snap.State.Should().Be("succeeded", snap.Error);
        File.Exists(Path.Combine(root, "pgroot", "data", "PG_VERSION")).Should().BeTrue(
            "a leading ./ is the normal pg_basebackup tar form");
        Directory.Delete(root, recursive: true);
    }

    // AAA (L4/tar-slip): сегмент `..` в имени → permanent-отказ (выход из корня).
    [Fact]
    public async Task DotDotEntryName_RejectedPermanent()
    {
        // Arrange — tar с именем «../escaped».
        var root = NewPgdata();
        var tar = BuildRawTar((new TarEntry("../escaped", TarTypeFlags.File, 3, 0x1A4, ""),
            "bad"u8.ToArray()));
        var snap = await RunRestoreAsync(root, tar);

        // Assert
        snap.State.Should().Be("failed");
        snap.Error.Should().Contain("dot segment");
        File.Exists(Path.Combine(root, "escaped")).Should().BeFalse(
            "the entry is rejected before any file operation");
        Directory.Delete(root, recursive: true);
    }

    // AAA (L4/tar-slip): symlink-escape (цель вне корня) → permanent-отказ;
    // абсолютная цель — тоже (политика цели внутри базы, 06 §2).
    [Fact]
    public async Task SymlinkEscapeTarget_RejectedPermanent()
    {
        // Arrange 1 — относительная цель выходит из корня.
        var root = NewPgdata();
        var tar = BuildRawTar((new TarEntry("evil", TarTypeFlags.Symlink, 0, 0x1FF, "../../etc"),
            []));
        var snap = await RunRestoreAsync(root, tar);
        snap.State.Should().Be("failed");
        snap.Error.Should().Contain("symlink target escapes");

        // Arrange 2 — абсолютная цель.
        var root2 = NewPgdata();
        var tar2 = BuildRawTar((new TarEntry("evil", TarTypeFlags.Symlink, 0, 0x1FF, "/etc"),
            []));
        var snap2 = await RunRestoreAsync(root2, tar2);
        snap2.State.Should().Be("failed");
        snap2.Error.Should().Contain("symlink target is not relative");

        Directory.Delete(root, recursive: true);
        Directory.Delete(root2, recursive: true);
    }

    // AAA (L4/tar-slip): запись СКВОЗЬ symlink (tar: ссылка + файл в ней) →
    // permanent-отказ: подменённый архив не пишет за пределы корня.
    [Fact]
    public async Task WriteThroughSymlink_RejectedPermanent()
    {
        // Arrange — ссылка link→«.» (внутри корня), затем файл link/pwned:
        // родитель-ссылка запрещена (запись сквозь неё).
        var root = NewPgdata();
        var tar = BuildRawTar(
            (new TarEntry("link", TarTypeFlags.Symlink, 0, 0x1FF, "."), []),
            (new TarEntry("link/pwned", TarTypeFlags.File, 3, 0x1A4, ""), "bad"u8.ToArray()));
        var snap = await RunRestoreAsync(root, tar);

        // Assert
        snap.State.Should().Be("failed");
        snap.Error.Should().Contain("writes through a symlink");
        File.Exists(Path.Combine(root, "pgroot", "data", "pwned")).Should().BeFalse();
        Directory.Delete(root, recursive: true);
    }

    // AAA (#25): сквозной sha256 объекта не сошёлся с полем команды →
    // failed ДО старта postgres (pg_ctl не звался).
    [Fact]
    public async Task ObjectShaMismatch_FailedBeforePostgresStart()
    {
        // Arrange — ожидаемый хэш заведомо другой.
        var root = NewPgdata();
        var runner = new FakeRunner();
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        var request = Request() with { Sha256 = new string('0', 64) };
        var op = new RestoreOperation(request, runner, registry,
            pgdata: Path.Combine(root, "pgroot", "data"), _ => new FakeTarS3());

        // Act
        await op.RunAsync(CancellationToken.None);

        // Assert
        op.Snapshot!.State.Should().Be("failed");
        op.Snapshot!.Error.Should().Contain("object sha256 mismatch");
        runner.Invocations.Should().NotContain(i => i.FileName == "pg_ctl",
            "pg_ctl does not start on a data verdict");
        Directory.Delete(root, recursive: true);
    }

    // AAA (#26): target_time с переводом строки/чужой строкой конфига —
    // отклоняется ДО записи auto.conf (инъекция параметра невозможна).
    [Fact]
    public async Task TargetTimeInjection_Rejected()
    {
        // Arrange — target_time с внедрённой строкой.
        var root = NewPgdata();
        var pgdata = Path.Combine(root, "pgroot", "data");
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        var op = new RestoreOperation(
            Request("2026-01-02 03:04:05+03\nrecovery_target_action = 'shutdown'"),
            new FakeRunner(), registry, pgdata: pgdata, _ => new FakeTarS3());

        // Act
        await op.RunAsync(CancellationToken.None);

        // Assert — отказ; в auto.conf нет внедрённой строки.
        op.Snapshot!.State.Should().Be("failed");
        op.Snapshot!.Error.Should().Contain("invalid target_time");
        File.ReadAllText(Path.Combine(pgdata, "postgresql.auto.conf"))
            .Should().NotContain("shutdown");
        Directory.Delete(root, recursive: true);
    }

    // Общий прогон restore на заданном tar-объекте (tar-slip сценарии):
    // возвращает терминальный снимок.
    private static async Task<OperationSnapshot> RunRestoreAsync(string root, byte[] tar)
    {
        var registry = new OperationRegistry();
        registry.TryBegin(new OperationSnapshot(Id, "running", "downloading"));
        var op = new RestoreOperation(Request(), new FakeRunner(), registry,
            pgdata: Path.Combine(root, "pgroot", "data"), _ => new FakeTarS3(tar));
        await op.RunAsync(CancellationToken.None);
        return op.Snapshot!;
    }

    // Tar-байты из произвольных записей (недоверенный вход tar-slip тестов).
    private static byte[] BuildRawTar(params (TarEntry Entry, byte[] Data)[] entries)
    {
        var buffer = new MemoryStream();
        using var writer = new TarStreamWriter(buffer);
        foreach (var (entry, data) in entries)
        {
            writer.WriteEntryAsync(entry, data, default).GetAwaiter().GetResult();
        }

        writer.FinishAsync(default).GetAwaiter().GetResult();
        return buffer.ToArray();
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
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(
            string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            // Версий .partial нет — конец WAL (fetch-wal t14: ListAsync вызывается
            // после NotFound-финалов; пустой список = «лучшей версии нет»).
            => Task.FromResult(Result<IReadOnlyList<WalObject>>.Success([]));
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectMd5Async(string key, byte[] data, string md5Hex, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(string cluster, string shard, string objectKey, CancellationToken ct = default) => throw new NotSupportedException();
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
