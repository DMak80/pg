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

// Интеграции WAL-приёмника против живого PG + ownS3-ноды (docker, динамические
// порты, guid-окружение OwnPostgres/OwnS3, полный teardown при любом исходе).
// AC1: restart_lsn ≤ доставленного в S3 хвоста; AC6: обрыв S3 → backpressure →
// восстановление без дыр; рестарт — дозасылка от хвоста; segment-size mismatch —
// исход FailedPermanent (состояние, не exit).
// ВЕРДИКТ TIMELINE_HISTORY: Npgsql 10.0.3 ЭКСПОНИРУЕТ публичный API
// ReplicationConnection.TimelineHistory(tli) (XML-doc + рефлексия пакета) —
// NpgsqlXLogSource реализует ReadTimelineHistoryAsync через него; условный
// fallback (docker-exec) НЕ нужен. TLI-переход в Ф2 не воспроизводится
// (single-node) — путь закрыт юнитом WalReceiverCoreTests и E2E (AC5, promote).
[Collection(NonE2eCollection.Name)]
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

    private static BackupS3 HostS3(OwnS3 s3Node) => new(s3Node.S3());

    /// <summary>Запуск ядра приёмника в фоне (in-process, host-клиенты). Сессия
    /// выход — только отмена (код 0, SIGTERM-семантика); внезапный исход
    /// сессии — код 1 (фейл ассерта .Be(0) — неожиданное завершение ядра).
    /// Диагностика ядра — в живой лог теста (TeeWriter).</summary>
    private async Task<(CancellationTokenSource Cts, Task<int> Run)> StartCoreAsync(
        OwnPostgres postgres, OwnS3 s3Node, string cluster, string shard)
    {
        var options = postgres.ReceiverOptions(
            s3Node.HostEndpoint, cluster, shard, OwnS3.AccessKey, OwnS3.SecretKey);
        var s3 = HostS3(s3Node);
        var source = new PgWorker.WalReceiver.NpgsqlXLogSource(options);
        var slots = new PgWorker.WalReceiver.NpgsqlSlotPositionReader(options);
        var cts = new CancellationTokenSource();
        var stdout = new TeeWriter(_output);
        var events = new PgWorker.WalReceiver.WalSessionEvents
        {
            Diagnostic = stdout.WriteLine,
        };
        var run = Task.Run<int>(async () =>
        {
            try
            {
                await PgWorker.WalReceiver.WalReceiverCore.RunSessionAsync(
                    options, source, slots, s3, events, cts.Token);
                return 1; // исход сессии до отмены — тесты трактуют как фейл
            }
            catch (OperationCanceledException)
            {
                return 0; // чистая отмена — прежний код 0
            }
        });
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

        var info = debug is null ? "none" : await debug();
        Assert.Fail($"the condition was not met within the {budget.TotalSeconds} s budget; debug: {info}");
    }

    [Fact]
    public async Task SegmentDeliveryAndContinuity()
    {
        // Arrange — своё окружение: PG (TLS+слот) + ownS3-нода, guid cluster/shard
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = await OwnPostgres.StartAsync("wal-it-delivery", ct);
        await using var s3Node = await OwnS3.StartAsync("wal-it-delivery", ct);
        var cluster = $"c{postgres.RunId[..8]}";
        var shard = "shard1";
        await using var s3 = HostS3(s3Node);

        // Act — приёмник стартует (пустой префикс → старт от restart_lsn слота)
        var (cts, run) = await StartCoreAsync(postgres, s3Node, cluster, shard);
        try
        {
            // Слепой sleep старта не нужен: готовность приёмника покрыта
            // WaitUntil ниже (бюджет 30 c, debug-дамп печатает сегменты и
            // restart_lsn — полную картину при недожатии).
            // WAL-нагрузка: 6 переключений ≈ 50 MiB → ≥2 полных сегмента 16 MiB
            await GenerateWalAsync(postgres.AdminDsn, Switches, ct);
            await WaitUntilAsync(async () =>
                (await ListSegmentsAsync(s3, cluster, shard, ct)).Names.Count >= 2,
                TimeSpan.FromSeconds(30),
                async () =>
                {
                    var (_, names) = await ListSegmentsAsync(s3, cluster, shard, ct);
                    var restart = await postgres.ReadRestartLsnAsync(ct);
                    return $"segments=[{string.Join(",", names)}] restart_lsn={restart}";
                });

            // Assert — сегменты в S3, без .partial, цепочка непрерывна
            var (minName, names) = await ListSegmentsAsync(s3, cluster, shard, ct);
            minName.Should().NotBeNull("the chain is anchored by the first full segment");
            names.Should().NotContain(n => n.EndsWith(".partial"), ".partial files do not appear");
            var chain = WalChain.Check(
                WalFileName.TryParse(minName!)!.Value, names);
            chain.IsContinuous.Should().BeTrue(chain.GapError ?? "the chain is continuous");

            // Assert AC1 — restart_lsn не ушёл за доставленный хвост: restart_lsn
            // читается ПЕРВЫМ, листинг ВТОРЫМ (доставка монотонна: хвост свежего
            // листинга ≥ хвоста в момент чтения restart_lsn; обратный порядок
            // ловит гонку роста между двумя чтениями при быстрой доставке).
            var restartLsn = await postgres.ReadRestartLsnAsync(ct);
            var (ac1Name, ac1Names) = await ListSegmentsAsync(s3, cluster, shard, ct);
            var ac1Chain = WalChain.Check(WalFileName.TryParse(ac1Name!)!.Value, ac1Names);
            var tail = PgWorker.WalReceiver.S3TailResolver.EndLsn(ac1Chain.LastSegment!.Value);
            restartLsn.Should().BeLessThanOrEqualTo(tail, "the confirm-on-delivery invariant");
        }
        finally
        {
            // Остановка core: чистая отмена → код 0
            cts.Cancel();
            (await run).Should().Be(0);
        }
    }

    [Fact]
    public async Task RestartCatchesUpFromTail()
    {
        // Arrange — окружение; первый запуск доставляет ≥2 сегмента и останавливается
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = await OwnPostgres.StartAsync("wal-it-restart", ct);
        await using var s3Node = await OwnS3.StartAsync("wal-it-restart", ct);
        var cluster = $"c{postgres.RunId[..8]}";
        var shard = "shard1";
        await using var s3 = HostS3(s3Node);

        var (cts1, run1) = await StartCoreAsync(postgres, s3Node, cluster, shard);
        await GenerateWalAsync(postgres.AdminDsn, Switches, ct);
        await WaitUntilAsync(async () =>
            (await ListSegmentsAsync(s3, cluster, shard, ct)).Names.Count >= 2,
            TimeSpan.FromSeconds(30));
        cts1.Cancel();
        (await run1).Should().Be(0);
        var (_, beforeNames) = await ListSegmentsAsync(s3, cluster, shard, ct);

        // Act — новая WAL-нагрузка и ПОВТОРНЫЙ запуск приёмника (дозасылка от хвоста)
        await GenerateWalAsync(postgres.AdminDsn, 2, ct);
        var (cts2, run2) = await StartCoreAsync(postgres, s3Node, cluster, shard);
        try
        {
            await WaitUntilAsync(async () =>
                (await ListSegmentsAsync(s3, cluster, shard, ct)).Names.Count > beforeNames.Count,
                TimeSpan.FromSeconds(30));

            // Assert — цепочка непрерывна (дыр нет), AC1 сохраняется: restart_lsn
            // читается ПЕРВЫМ, листинг ВТОРЫМ (доставка монотонна — устранение
            // гонки роста между чтениями, образец выше).
            var restartLsn = await postgres.ReadRestartLsnAsync(ct);
            var (minName, names) = await ListSegmentsAsync(s3, cluster, shard, ct);
            var chain = WalChain.Check(WalFileName.TryParse(minName!)!.Value, names);
            chain.IsContinuous.Should().BeTrue(chain.GapError ?? "the chain is continuous");
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
    public async Task S3OutageBackpressureRecovery()
    {
        // Arrange — окружение; приёмник доставляет ≥1 сегмент
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = await OwnPostgres.StartAsync("wal-it-s3down", ct);
        await using var s3Node = await OwnS3.StartAsync("wal-it-s3down", ct);
        var cluster = $"c{postgres.RunId[..8]}";
        var shard = "shard1";
        await using var s3 = HostS3(s3Node);

        var (cts, run) = await StartCoreAsync(postgres, s3Node, cluster, shard);
        try
        {
            await GenerateWalAsync(postgres.AdminDsn, Switches, ct);
            await WaitUntilAsync(async () =>
                (await ListSegmentsAsync(s3, cluster, shard, ct)).Names.Count >= 1,
                TimeSpan.FromSeconds(30));
            var (_, namesBefore) = await ListSegmentsAsync(s3, cluster, shard, ct);
            var chainBefore = WalChain.Check(
                WalFileName.TryParse(namesBefore.Min()!)!.Value, namesBefore);
            var tailBefore = PgWorker.WalReceiver.S3TailResolver.EndLsn(chainBefore.LastSegment!.Value);

            // Act AC6 — S3-обрыв: docker pause ownS3-ноды; бюджет спеки «≥1 мин»
            TestContext.Current.TestOutputHelper?.WriteLine("[PHASE] pause-wait 60s (S3 unavailable)");
            await s3Node.PauseAsync(ct);
            // Длительность сценария по AC6 (≥1 мин недоступности S3), не таймаут
            // ожидания — не увеличивать и не уменьшать.
            await Task.Delay(TimeSpan.FromSeconds(60), ct);

            // Assert — приёмник НЕ подтверждает за недоставленное (backpressure)
            var restartDuring = await postgres.ReadRestartLsnAsync(ct);
            restartDuring.Should().BeLessThanOrEqualTo(
                tailBefore, "restart_lsn does not go past the last delivered segment while S3 is paused");

            // Act — восстановление S3: доставка доигрывается
            TestContext.Current.TestOutputHelper?.WriteLine("[PHASE] unpause, the delivery finishes");
            await s3Node.UnpauseAsync(ct);
            await GenerateWalAsync(postgres.AdminDsn, 2, ct);
            await WaitUntilAsync(async () =>
            {
                var (_, names) = await ListSegmentsAsync(s3, cluster, shard, ct);
                return names.Count > namesBefore.Count;
            }, TimeSpan.FromSeconds(30));

            // Assert — цепочка непрерывна, AC1 после восстановления
            var (minName, namesAfter) = await ListSegmentsAsync(s3, cluster, shard, ct);
            var chain = WalChain.Check(WalFileName.TryParse(minName!)!.Value, namesAfter);
            chain.IsContinuous.Should().BeTrue(chain.GapError ?? "the chain is continuous");
            var restartLsn = await postgres.ReadRestartLsnAsync(ct);
            restartLsn.Should().BeLessThanOrEqualTo(
                PgWorker.WalReceiver.S3TailResolver.EndLsn(chain.LastSegment!.Value));
        }
        finally
        {
            // Контейнер мог остаться на паузе при падении ассертов — снимаем
            try
            {
                await s3Node.UnpauseAsync(CancellationToken.None);
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
    public async Task TliStart_IdentifyAndExplicitTimeline_StreamsFromTail()
    {
        // Arrange — своё окружение: PG (TLS+слот) + ownS3-нода; ядро приёмника
        // (старт без TIMELINE) накатывает ≥3 закрытых сегмента и останавливается
        // отменой — дальше проверяется прямой транспорт tli-механики.
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = await OwnPostgres.StartAsync("wal-it-tli", ct);
        await using var s3Node = await OwnS3.StartAsync("wal-it-tli", ct);
        var cluster = $"c{postgres.RunId[..8]}";
        var shard = "shard1";
        await using var s3 = HostS3(s3Node);

        var (ctsCore, runCore) = await StartCoreAsync(postgres, s3Node, cluster, shard);
        try
        {
            // Switches (6) переключений, а не 3: приёмник стартует от restart_lsn
            // (середина сегмента — слот уже отпустил начало) и доставляет только
            // ЗАКРЫТЫЕ сегменты выше него; 3 переключений дают < 3 доставленных.
            await GenerateWalAsync(postgres.AdminDsn, Switches, ct);
            await WaitUntilAsync(async () =>
                (await ListSegmentsAsync(s3, cluster, shard, ct)).Names.Count >= 3,
                TimeSpan.FromSeconds(30),
                async () =>
                {
                    var (_, names) = await ListSegmentsAsync(s3, cluster, shard, ct);
                    return $"segments=[{string.Join(",", names)}]";
                });
        }
        finally
        {
            ctsCore.Cancel();
            (await runCore).Should().Be(0);
        }

        // Act — прямой транспорт: IDENTIFY_SYSTEM по короткоживущему
        // replication-соединению, затем START_REPLICATION с явным TIMELINE 1
        // от хвоста S3 (перегрузка «streaming starts on timeline tli»).
        var options = postgres.ReceiverOptions(
            s3Node.HostEndpoint, cluster, shard, OwnS3.AccessKey, OwnS3.SecretKey);
        await using var source = new PgWorker.WalReceiver.NpgsqlXLogSource(options);
        (await source.OpenAsync(ct)).IsSuccess.Should().BeTrue();
        var identity = await source.IdentifySystemAsync(ct);
        identity.IsSuccess.Should().BeTrue(identity.Error?.Message);

        // Assert — живой PG без промоутов: текущий таймлайн 1, позиция записи > 0
        identity.Value!.Tli.Should().Be(1, "single-node PG: current timeline is 1");
        identity.Value!.XLogPos.Should().BeGreaterThan(0);

        // Хвост S3: конец последнего сегмента непрерывной цепочки
        var (minName, names) = await ListSegmentsAsync(s3, cluster, shard, ct);
        minName.Should().NotBeNull("the chain is anchored by the first full segment");
        var chain = WalChain.Check(WalFileName.TryParse(minName!)!.Value, names);
        chain.IsContinuous.Should().BeTrue(chain.GapError ?? "the chain is continuous");
        var tail = PgWorker.WalReceiver.S3TailResolver.EndLsn(chain.LastSegment!.Value);

        (await source.StartReplicationAsync(OwnPostgres.Slot, tail, 1, ct))
            .IsSuccess.Should().BeTrue("the explicit-timeline start succeeds on a live PG");

        // Новый WAL — гарантия байтов в стриме; первый чанк — с бюджетом ≤ 30 с
        await GenerateWalAsync(postgres.AdminDsn, 1, ct);
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(ct);
        budget.CancelAfter(TimeSpan.FromSeconds(30));
        var enumerator = source.Stream.GetAsyncEnumerator(budget.Token);
        var moved = await enumerator.MoveNextAsync().AsTask();

        // Assert — перегрузка с TIMELINE стартует ровно от LSN (поведение риса
        // §9 «не иначе»); данные чанка не пусты; утечки соединений нет —
        // DisposeAsync source гасит стрим-соединение (проверка — успешный
        // dispose ниже, фейков счётчиков у прод-транспорта нет).
        moved.Should().BeTrue("the explicit-timeline stream yields data from the tail");
        var chunk = enumerator.Current;
        chunk.WalStart.Should().Be(tail, "explicit TIMELINE start is exact at the tail LSN");
        chunk.Data.Length.Should().BeGreaterThan(0);
        await enumerator.DisposeAsync();
    }

    [Fact]
    public async Task Permanent_segment_size_mismatch()
    {
        // Arrange — PG с несовместимым wal_segment_size=32MB
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = await OwnPostgres.StartAsync("wal-it-segsize", ct, initdbArgs: "--wal-segsize=32");
        await using var s3Node = await OwnS3.StartAsync("wal-it-segsize", ct);
        var cluster = $"c{postgres.RunId[..8]}";
        var shard = "shard1";

        // Act — запуск приёмника: сверка wal_segment_size → исход FailedPermanent
        // (состояние, НЕ exit процесса)
        var options = postgres.ReceiverOptions(
            s3Node.HostEndpoint, cluster, shard, OwnS3.AccessKey, OwnS3.SecretKey);
        await using var s3 = HostS3(s3Node);
        var diagnostics = new StringWriter();
        var outcome = await PgWorker.WalReceiver.WalReceiverCore.RunSessionAsync(
            options,
            new PgWorker.WalReceiver.NpgsqlXLogSource(options),
            new PgWorker.WalReceiver.NpgsqlSlotPositionReader(options),
            s3,
            new PgWorker.WalReceiver.WalSessionEvents { Diagnostic = diagnostics.WriteLine },
            new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token);

        // Assert — FailedPermanent, текст ошибки называет wal_segment_size
        outcome.Kind.Should().Be(PgWorker.WalReceiver.WalSessionKind.FailedPermanent);
        outcome.Error.Should().Contain("wal_segment_size");
    }
}
