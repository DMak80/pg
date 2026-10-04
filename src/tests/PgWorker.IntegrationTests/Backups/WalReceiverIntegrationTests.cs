using System.Text;
using Amazon.Runtime;
using Amazon.S3;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции WAL-приёмника против живого PG + MinIO (t27 Ф2, docker, динамические
// порты, guid-окружение OwnPostgres/OwnMinio, полный teardown при любом исходе).
// AC1: restart_lsn ≤ доставленного в S3 хвоста; AC6: обрыв S3 → backpressure →
// восстановление без дыр; рестарт — дозасылка от хвоста; segment-size mismatch —
// permanent (код 3).
// ВЕРДИКТ TIMELINE_HISTORY: Npgsql 10.0.3 ЭКСПОНИРУЕТ публичный API
// ReplicationConnection.TimelineHistory(tli) (XML-doc + рефлексия пакета) —
// NpgsqlXLogSource реализует ReadTimelineHistoryAsync через него; условный
// fallback (docker-exec) НЕ нужен. TLI-переход в Ф2 не воспроизводится
// (single-node) — путь закрыт юнитом WalReceiverCoreTests и E2E (AC5, promote).
public class WalReceiverIntegrationTests
{
    // Нагрузка: INSERT ~8 MiB + pg_switch_wal (superuser) — форсированное закрытие.
    private const int Switches = 6;

    private readonly ITestOutputHelper _output;

    public WalReceiverIntegrationTests(ITestOutputHelper output) => _output = output;

    /// <summary>Stdout приёмника — живьём в лог теста (диагностика «что произошло»
    /// по канону телеметрии) + буфер для ассертов.</summary>
    private sealed class TeeWriter(ITestOutputHelper output) : TextWriter
    {
        private readonly StringWriter _buffer = new();

        public override Encoding Encoding => _buffer.Encoding;

        public override void Write(char value)
        {
            _buffer.Write(value);
            output.Write(value.ToString());
        }

        public override void WriteLine(string? value)
        {
            _buffer.WriteLine(value);
            output.WriteLine(value ?? "");
        }

        public override async Task WriteLineAsync(string? value)
        {
            WriteLine(value);
            await Task.CompletedTask;
        }

        public override string ToString() => _buffer.ToString();
    }

    private static async Task GenerateWalAsync(string adminDsn, int switches, CancellationToken ct)
    {
        await using var conn = new Npgsql.NpgsqlConnection(adminDsn);
        await conn.OpenAsync(ct);
        await using (var create = new Npgsql.NpgsqlCommand(
            "CREATE TABLE IF NOT EXISTS wal_load(id bigserial, payload text)", conn))
            await create.ExecuteNonQueryAsync(ct);
        for (var i = 0; i < switches; i++)
        {
            await using var insert = new Npgsql.NpgsqlCommand(
                "INSERT INTO wal_load(payload) SELECT repeat('x', 1048576) FROM generate_series(1, 8)", conn);
            await insert.ExecuteNonQueryAsync(ct);
            await using var switchWal = new Npgsql.NpgsqlCommand("SELECT pg_switch_wal()", conn);
            await switchWal.ExecuteScalarAsync(ct);
        }
    }

    private static BackupS3 HostS3(OwnMinio minio)
    {
        var options = minio.Runtime();
        return new BackupS3(options);
    }

    /// <summary>Запуск ядра приёмника в фоне (in-process, host-клиенты).</summary>
    private async Task<(CancellationTokenSource Cts, Task<int> Run)> StartCoreAsync(
        OwnPostgres postgres, OwnMinio minio, string cluster, string shard)
    {
        var options = postgres.ReceiverOptions(
            minio.HostEndpoint, cluster, shard, OwnMinio.AccessKey, OwnMinio.SecretKey);
        var s3 = HostS3(minio);
        var source = new PgWorker.WalReceiver.NpgsqlXLogSource(options);
        var slots = new PgWorker.WalReceiver.NpgsqlSlotPositionReader(options);
        var cts = new CancellationTokenSource();
        var stdout = new TeeWriter(_output);
        var run = PgWorker.WalReceiver.WalReceiverCore.RunAsync(options, source, slots, s3, stdout, cts.Token);
        return (cts, run);
    }

    /// <summary>Листинг сегментов wal-префикса: (минимальное имя, все имена).</summary>
    private static async Task<(string? MinName, IReadOnlyList<string> Names)> ListSegmentsAsync(
        BackupS3 s3, string cluster, string shard, CancellationToken ct)
    {
        var listed = await s3.ListWalAsync(cluster, shard, ct: ct);
        listed.IsSuccess.Should().BeTrue(listed.Error?.Message);
        var names = listed.Value.Select(o => o.Name).ToList();
        var min = names
            .Select(n => WalFileName.TryParse(n))
            .OfType<WalFileName>()
            .OrderBy(s => s.Name, StringComparer.Ordinal)
            .Cast<WalFileName?>()
            .FirstOrDefault();
        return (min?.Name, names);
    }

    private static async Task WaitUntilAsync(
        Func<Task<bool>> condition, TimeSpan budget, Func<Task<string>>? debug = null)
    {
        var deadline = DateTime.UtcNow + budget;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
                return;
            await Task.Delay(500, TestContext.Current.CancellationToken);
        }

        var info = debug is null ? "нет" : await debug();
        Assert.Fail($"условие не выполнилось за бюджет {budget.TotalSeconds} c; debug: {info}");
    }

    [Fact]
    public async Task Доставка_сегментов_и_непрерывность()
    {
        // Arrange — своё окружение: PG (TLS+слот) + MinIO, guid cluster/shard
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = await OwnPostgres.StartAsync("wal-it-delivery", ct);
        await using var minio = await OwnMinio.StartAsync("wal-it-delivery", ct);
        var cluster = $"c{postgres.RunId[..8]}";
        var shard = "shard1";
        await using var s3 = HostS3(minio);

        // Act — приёмник стартует (пустой префикс → старт от restart_lsn слота)
        var (cts, run) = await StartCoreAsync(postgres, minio, cluster, shard);
        try
        {
            // Диагностика старта: жив ли run через 5 c
            await Task.Delay(5000, ct);
            _output.WriteLine($"[DIAG] core run 5s: status={run.Status}" +
                (run.IsFaulted ? $" exception={run.Exception?.GetBaseException().Message}" : ""));
            // WAL-нагрузка: 6 переключений ≈ 50 MiB → ≥2 полных сегмента 16 MiB
            await GenerateWalAsync(postgres.AdminDsn, Switches, ct);
            await WaitUntilAsync(async () =>
                (await ListSegmentsAsync(s3, cluster, shard, ct)).Names.Count >= 2,
                TimeSpan.FromSeconds(60),
                async () =>
                {
                    var (_, names) = await ListSegmentsAsync(s3, cluster, shard, ct);
                    var restart = await postgres.ReadRestartLsnAsync(ct);
                    return $"segments=[{string.Join(",", names)}] restart_lsn={restart}";
                });

            // Assert — сегменты в S3, без .partial, цепочка непрерывна
            var (minName, names) = await ListSegmentsAsync(s3, cluster, shard, ct);
            minName.Should().NotBeNull("цепочка закреплена первым полным сегментом");
            names.Should().NotContain(n => n.EndsWith(".partial"), ".partial не появляются");
            var chain = WalChain.Check(
                WalFileName.TryParse(minName!)!.Value, names);
            chain.IsContinuous.Should().BeTrue(chain.GapError ?? "цепочка непрерывна");

            // Assert AC1 — restart_lsn не ушёл за доставленный хвост
            var restartLsn = await postgres.ReadRestartLsnAsync(ct);
            var tail = PgWorker.WalReceiver.S3TailResolver.EndLsn(chain.LastSegment!.Value);
            restartLsn.Should().BeLessThanOrEqualTo(tail, "инвариант подтверждения-по-доставке");
        }
        finally
        {
            // Остановка core: чистая отмена → код 0
            cts.Cancel();
            (await run).Should().Be(0);
        }
    }

    [Fact]
    public async Task Рестарт_дозасылает_от_хвоста()
    {
        // Arrange — окружение; первый запуск доставляет ≥2 сегмента и останавливается
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = await OwnPostgres.StartAsync("wal-it-restart", ct);
        await using var minio = await OwnMinio.StartAsync("wal-it-restart", ct);
        var cluster = $"c{postgres.RunId[..8]}";
        var shard = "shard1";
        await using var s3 = HostS3(minio);

        var (cts1, run1) = await StartCoreAsync(postgres, minio, cluster, shard);
        await GenerateWalAsync(postgres.AdminDsn, Switches, ct);
        await WaitUntilAsync(async () =>
            (await ListSegmentsAsync(s3, cluster, shard, ct)).Names.Count >= 2,
            TimeSpan.FromSeconds(60));
        cts1.Cancel();
        (await run1).Should().Be(0);
        var (_, beforeNames) = await ListSegmentsAsync(s3, cluster, shard, ct);

        // Act — новая WAL-нагрузка и ПОВТОРНЫЙ запуск приёмника (дозасылка от хвоста)
        await GenerateWalAsync(postgres.AdminDsn, 2, ct);
        var (cts2, run2) = await StartCoreAsync(postgres, minio, cluster, shard);
        try
        {
            await WaitUntilAsync(async () =>
                (await ListSegmentsAsync(s3, cluster, shard, ct)).Names.Count > beforeNames.Count,
                TimeSpan.FromSeconds(60));

            // Assert — цепочка непрерывна (дыр нет), AC1 сохраняется
            var (minName, names) = await ListSegmentsAsync(s3, cluster, shard, ct);
            var chain = WalChain.Check(WalFileName.TryParse(minName!)!.Value, names);
            chain.IsContinuous.Should().BeTrue(chain.GapError ?? "цепочка непрерывна");
            var restartLsn = await postgres.ReadRestartLsnAsync(ct);
            var tail = PgWorker.WalReceiver.S3TailResolver.EndLsn(chain.LastSegment!.Value);
            restartLsn.Should().BeLessThanOrEqualTo(tail);
        }
        finally
        {
            cts2.Cancel();
            (await run2).Should().Be(0);
        }
    }

    [Fact]
    public async Task Обрыв_S3_backpressure_восстановление()
    {
        // Arrange — окружение; приёмник доставляет ≥1 сегмент
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = await OwnPostgres.StartAsync("wal-it-s3down", ct);
        await using var minio = await OwnMinio.StartAsync("wal-it-s3down", ct);
        var cluster = $"c{postgres.RunId[..8]}";
        var shard = "shard1";
        await using var s3 = HostS3(minio);

        var (cts, run) = await StartCoreAsync(postgres, minio, cluster, shard);
        try
        {
            await GenerateWalAsync(postgres.AdminDsn, Switches, ct);
            await WaitUntilAsync(async () =>
                (await ListSegmentsAsync(s3, cluster, shard, ct)).Names.Count >= 1,
                TimeSpan.FromSeconds(60));
            var (_, namesBefore) = await ListSegmentsAsync(s3, cluster, shard, ct);
            var chainBefore = WalChain.Check(
                WalFileName.TryParse(namesBefore.Min()!)!.Value, namesBefore);
            var tailBefore = PgWorker.WalReceiver.S3TailResolver.EndLsn(chainBefore.LastSegment!.Value);

            // Act AC6 — S3-обрыв: docker pause MinIO; бюджет спеки «≥1 мин»
            TestContext.Current.TestOutputHelper?.WriteLine("[PHASE] pause-wait 60s (S3 недоступен)");
            await minio.PauseAsync(ct);
            await Task.Delay(TimeSpan.FromSeconds(60), ct);

            // Assert — приёмник НЕ подтверждает за недоставленное (backpressure)
            var restartDuring = await postgres.ReadRestartLsnAsync(ct);
            restartDuring.Should().BeLessThanOrEqualTo(
                tailBefore, "restart_lsn не уходит за последний доставленный сегмент при паузе S3");

            // Act — восстановление S3: доставка доигрывается
            TestContext.Current.TestOutputHelper?.WriteLine("[PHASE] unpause, доставка доигрывается");
            await minio.UnpauseAsync(ct);
            await GenerateWalAsync(postgres.AdminDsn, 2, ct);
            await WaitUntilAsync(async () =>
            {
                var (_, names) = await ListSegmentsAsync(s3, cluster, shard, ct);
                return names.Count > namesBefore.Count;
            }, TimeSpan.FromSeconds(90));

            // Assert — цепочка непрерывна, AC1 после восстановления
            var (minName, namesAfter) = await ListSegmentsAsync(s3, cluster, shard, ct);
            var chain = WalChain.Check(WalFileName.TryParse(minName!)!.Value, namesAfter);
            chain.IsContinuous.Should().BeTrue(chain.GapError ?? "цепочка непрерывна");
            var restartLsn = await postgres.ReadRestartLsnAsync(ct);
            restartLsn.Should().BeLessThanOrEqualTo(
                PgWorker.WalReceiver.S3TailResolver.EndLsn(chain.LastSegment!.Value));
        }
        finally
        {
            // Контейнер мог остаться на паузе при падении ассертов — снимаем
            try
            {
                await minio.UnpauseAsync(CancellationToken.None);
            }
            catch
            {
                // teardown внизу всё равно стопает контейнер
            }

            cts.Cancel();
            (await run).Should().Be(0);
        }
    }

    [Fact]
    public async Task Permanent_segment_size_mismatch()
    {
        // Arrange — PG с несовместимым wal_segment_size=32MB
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = await OwnPostgres.StartAsync("wal-it-segsize", ct, initdbArgs: "--wal-segsize=32");
        await using var minio = await OwnMinio.StartAsync("wal-it-segsize", ct);
        var cluster = $"c{postgres.RunId[..8]}";
        var shard = "shard1";

        // Act — запуск приёмника: сверка wal_segment_size → permanent-выход
        var options = postgres.ReceiverOptions(
            minio.HostEndpoint, cluster, shard, OwnMinio.AccessKey, OwnMinio.SecretKey);
        await using var s3 = HostS3(minio);
        var stdout = new StringWriter();
        var code = await PgWorker.WalReceiver.WalReceiverCore.RunAsync(
            options,
            new PgWorker.WalReceiver.NpgsqlXLogSource(options),
            new PgWorker.WalReceiver.NpgsqlSlotPositionReader(options),
            s3,
            stdout,
            new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token);

        // Assert — код 3, result-маркер называет wal_segment_size
        code.Should().Be(3);
        stdout.ToString().Should().Contain("wal_segment_size");
        stdout.ToString().Should().Contain("\"ok\":false");
    }
}
