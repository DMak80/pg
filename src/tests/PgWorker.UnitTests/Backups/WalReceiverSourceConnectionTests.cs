using FluentAssertions;
using Npgsql;
using Npgsql.Replication;
using NpgsqlTypes;
using PgWorker.WalReceiver;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Жизненный цикл replication-соединений транспорта (ревью Фазы 7):
// transient-цикл приёмника (обрыв источника / рестарт / сбой START_REPLICATION)
// обязан гасить прежнее соединение ДО создания нового — иначе живые walsender'ы
// копятся и исчерпывают max_wal_senders источника, ломая репликацию кластера.
// Инвариант: закрытий ≥ открытий − 1 (живое — максимум одно, текущее).
public class WalReceiverSourceConnectionTests
{
    private static WalReceiverOptions Options() => new(
        "s1a", 5432, "backup_exec", "pw", "postgres",
        "pgw_bkp_c1_shard1", "c1", "shard1",
        ["http://s3a:9000"], ["http://s3a:9000"], "backups", "ak", "sk");

    private static NpgsqlXLogSource Source(
        FakeReplicationConnection fake,
        Action<FakeReplicationConnection>? configure = null)
    {
        configure?.Invoke(fake);
        return new NpgsqlXLogSource(
            Options(),
            connectionFactory: _ => fake,
            rawSegmentSizeProbe: _ => Task.FromResult("16MB"));
    }

    [Fact]
    public async Task RepeatedOpenAsync_KillsPreviousConnection()
    {
        // Arrange — источник с фейк-фабрикой; два цикла OpenAsync (модель
        // transient-переподключения: обрыв источника → новый цикл).
        var fake = new FakeReplicationConnection();
        await using var source = Source(fake);

        // Act — открытие, затем повторное открытие без Dispose первого.
        (await source.OpenAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
        (await source.OpenAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();

        // Assert — прежнее соединение закрыто: открытий 2, закрытий ≥ 1
        // (живое — только текущее; инвариант закрытий ≥ открытий − 1).
        fake.Opened.Should().Be(2);
        fake.Disposed.Should().BeGreaterThanOrEqualTo(1);
    }

    [Fact]
    public async Task StartReplicationFailure_KillsBrokenConnection()
    {
        // Arrange — фейк с запрограммированным сбоем START_REPLICATION.
        var fake = new FakeReplicationConnection();
        await using var source = Source(fake, f => f.FailStartReplication = true);

        // Act — OpenAsync ок, StartReplication падает (слот занят/протокол).
        (await source.OpenAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
        var start = await source.StartReplicationAsync("slot", 0x1000000, null, CancellationToken.None);

        // Assert — сбой отражён, соединение погашено (0 живых): следующий
        // цикл OpenAsync создаст свежее, а не переиспользует сломанное.
        start.IsSuccess.Should().BeFalse();
        fake.Disposed.Should().Be(fake.Opened, "the broken connection must die immediately");
    }

    [Fact]
    public async Task SeriesOfStartFailures_DoesNotAccumulateConnections()
    {
        // Arrange — модель худшего случая ревью: «Open ок → Start fail → 5 с →
        // повтор» многократно (мастер недоступен для стрима долгое время).
        var fake = new FakeReplicationConnection();
        await using var source = Source(fake, f => f.FailStartReplication = true);

        // Act — 5 полных transient-циклов (open → fail start) подряд.
        for (var i = 0; i < 5; i++)
        {
            (await source.OpenAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
            (await source.StartReplicationAsync("slot", 0x1000000, null, CancellationToken.None))
                .IsSuccess.Should().BeFalse();
        }

        // Assert — инвариант ревью: закрытий ≥ открытий − 1 (утечки нет —
        // каждый walsender жил ровно до своей замены).
        fake.Opened.Should().Be(5);
        fake.Disposed.Should().BeGreaterThanOrEqualTo(fake.Opened - 1);
    }

    [Fact]
    public async Task StartRefused_58P01_ReturnsTypedMarker()
    {
        // Arrange — отказ старта по 58P01 (сегмент срезан/не существует на таймлайне).
        var fake = new FakeReplicationConnection
        {
            StartReplicationException = new PostgresException(
                // Сигнатура Npgsql 10.0.3: (messageText, severity, invariantSeverity, sqlState).
                "requested WAL segment 000000020000000000000003 has already been removed",
                "ERROR", "ERROR", "58P01")
        };
        await using var source = Source(fake);
        (await source.OpenAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();

        // Act
        var start = await source.StartReplicationAsync("slot", 0x1000000, null, CancellationToken.None);

        // Assert — типизированный WalStartRefused наружу, текст исходной ошибки
        // сохранён в Message; соединение погашено (следующий старт требует OpenAsync).
        start.IsSuccess.Should().BeFalse();
        start.Error.Should().BeOfType<WalStartRefused>();
        start.Error!.Message.Should().Contain("has already been removed");
        fake.Disposed.Should().Be(fake.Opened, "the refused start must kill the connection");
    }

    [Fact]
    public async Task StartFailed_OtherSqlState_PlainApplicationException()
    {
        // Arrange — отказ старта с другим SqlState (не 58P01).
        var fake = new FakeReplicationConnection
        {
            StartReplicationException = new PostgresException(
                "replication slot is active", "ERROR", "ERROR", "42P01")
        };
        await using var source = Source(fake);
        (await source.OpenAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();

        // Act
        var start = await source.StartReplicationAsync("slot", 0x1000000, null, CancellationToken.None);

        // Assert — НЕ WalStartRefused (58P01-маркер только для 58P01).
        start.IsSuccess.Should().BeFalse();
        start.Error.Should().NotBeOfType<WalStartRefused>();
    }

    [Fact]
    public async Task IdentifySystem_ConnectionClosed()
    {
        // Arrange — короткоживущее Identify-соединение: без предварительного
        // OpenAsync источника (стрим-коннект после failed-старта уже погашен).
        var fake = new FakeReplicationConnection();
        await using var source = Source(fake);

        // Act
        var identity = await source.IdentifySystemAsync(CancellationToken.None);

        // Assert — попытка успешна и соединение погашено: открытий = закрытий
        // (инвариант «закрытий ≥ открытий − 1» — короткоживущее живёт ровно
        // до своего Dispose).
        identity.IsSuccess.Should().BeTrue();
        identity.Value!.Tli.Should().Be(1);
        fake.Opened.Should().Be(1);
        fake.Disposed.Should().Be(fake.Opened);
    }

    [Fact]
    public async Task IdentifySystem_Failed_ReturnsFailed()
    {
        // Arrange — отказ открытия короткоживущего Identify-соединения.
        var fake = new FakeReplicationConnection { FailOpen = true };
        await using var source = Source(fake);

        // Act
        var identity = await source.IdentifySystemAsync(CancellationToken.None);

        // Assert — Failed (transient-семантика ядра), соединение погашено.
        identity.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public async Task TliProcedure_DoesNotLeakConnections()
    {
        // Arrange — транспорт с отказом старта 58P01 на любой конфигурации:
        // серия tli-процедуры «open → refuse → identify → open →
        // backfill-refuse → open → jump-refuse» ×3 (каждый старт — свежее
        // соединение; короткоживущее Identify-соединение гасится сразу).
        var fake = new FakeReplicationConnection
        {
            StartReplicationException = new PostgresException(
                "requested WAL segment has already been removed", "ERROR", "ERROR", "58P01"),
            IdentifyResult = (2, 3UL * 16 * 1024 * 1024 + 5 * 1024 * 1024)
        };
        await using var source = Source(fake);

        // Act — три полные итерации процедуры.
        for (var i = 0; i < 3; i++)
        {
            (await source.OpenAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
            var refused = await source.StartReplicationAsync("slot", 0x1000000, null, CancellationToken.None);
            refused.Error.Should().BeOfType<WalStartRefused>();
            (await source.IdentifySystemAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
            (await source.OpenAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
            var backfill = await source.StartReplicationAsync("slot", 0x1000000, 1, CancellationToken.None);
            backfill.Error.Should().BeOfType<WalStartRefused>();
            (await source.OpenAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
            var jump = await source.StartReplicationAsync("slot", 0x1000000, 2, CancellationToken.None);
            jump.Error.Should().BeOfType<WalStartRefused>();
        }

        // Assert — инвариант утечки: закрытий ≥ открытий − 1 с учётом ВСЕХ
        // открытий (процедурных включительно; каждое соединение живёт ровно
        // до своей замены — 4 открытия на итерацию, 12 всего).
        fake.Opened.Should().Be(12);
        fake.Disposed.Should().BeGreaterThanOrEqualTo(fake.Opened - 1);
    }

    /// <summary>Фейк replication-соединения: счётчики открытий/закрытий;
    /// StartReplication программируемо падает (сценарии сбоя старта стрима).</summary>
    private sealed class FakeReplicationConnection : IReplicationConnection
    {
        internal int Opened;
        internal int Disposed;
        internal bool FailStartReplication;

        public Task Open(CancellationToken ct)
        {
            Opened++;
            if (FailOpen)
                throw new IOException("fake: open failed");
            return Task.CompletedTask;
        }

        // Точки программирования: флаг отказа Open (сбой открытия короткоживящего
        // Identify-соединения), исключение из StartReplication (например,
        // PostgresException с SqlState 58P01), результат IdentifySystem.
        public bool FailOpen;
        public Exception? StartReplicationException;
        public (uint Tli, ulong XLogPos) IdentifyResult = (1, 0);

        public Task<(uint Tli, ulong XLogPos)> IdentifySystem(CancellationToken ct)
            => Task.FromResult(IdentifyResult);

        public IAsyncEnumerable<XLogDataMessage> StartReplication(
            PhysicalReplicationSlot slot, NpgsqlLogSequenceNumber startLsn,
            CancellationToken ct, uint? timeline)
        {
            if (StartReplicationException is { } ex)
                throw ex;
            return FailStartReplication
                ? throw new InvalidOperationException("fake: start replication failed")
                : EmptyStream();
        }

        public void SetReplicationStatus(NpgsqlLogSequenceNumber lsn)
        {
        }

        public Task SendStatusUpdate(CancellationToken ct) => Task.CompletedTask;

        public Task<byte[]?> TimelineHistory(uint tli, CancellationToken ct)
            => throw new NotSupportedException("fake: history is not needed in these tests");

        public ValueTask DisposeAsync()
        {
            Disposed++;
            return ValueTask.CompletedTask;
        }

        private static async IAsyncEnumerable<XLogDataMessage> EmptyStream()
        {
            await Task.Yield();
            yield break;
        }
    }
}
