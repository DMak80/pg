using FluentAssertions;
using Npgsql.Replication;
using NpgsqlTypes;
using PgWorker.WalReceiver;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Жизненный цикл replication-соединений транспорта (t27, ревью Фазы 7):
// transient-цикл приёмника (обрыв источника / рестарт / сбой START_REPLICATION)
// обязан гасить прежнее соединение ДО создания нового — иначе живые walsender'ы
// копятся и исчерпывают max_wal_senders источника, ломая репликацию кластера.
// Инвариант: закрытий ≥ открытий − 1 (живое — максимум одно, текущее).
public class WalReceiverSourceConnectionTests
{
    private static WalReceiverOptions Options() => new(
        "s1a", 5432, "backup_exec", "pw", "postgres",
        "pgw_bkp_c1_shard1", "c1", "shard1",
        "http://minio:9000", null, "backups", "ak", "sk", true);

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
    public async Task Повторный_OpenAsync_ГаситПрежнееСоединение()
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
    public async Task Сбой_StartReplication_ГаситСломанноеСоединение()
    {
        // Arrange — фейк с запрограммированным сбоем START_REPLICATION.
        var fake = new FakeReplicationConnection();
        await using var source = Source(fake, f => f.FailStartReplication = true);

        // Act — OpenAsync ок, StartReplication падает (слот занят/протокол).
        (await source.OpenAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
        var start = await source.StartReplicationAsync("slot", 0x1000000, CancellationToken.None);

        // Assert — сбой отражён, соединение погашено (0 живых): следующий
        // цикл OpenAsync создаст свежее, а не переиспользует сломанное.
        start.IsSuccess.Should().BeFalse();
        fake.Disposed.Should().Be(fake.Opened, "сломанное соединение обязано умереть сразу");
    }

    [Fact]
    public async Task Серия_Сбоев_Старта_Не_КопитСоединения()
    {
        // Arrange — модель худшего случая ревью: «Open ок → Start fail → 5 с →
        // повтор» многократно (мастер недоступен для стрима долгое время).
        var fake = new FakeReplicationConnection();
        await using var source = Source(fake, f => f.FailStartReplication = true);

        // Act — 5 полных transient-циклов (open → fail start) подряд.
        for (var i = 0; i < 5; i++)
        {
            (await source.OpenAsync(CancellationToken.None)).IsSuccess.Should().BeTrue();
            (await source.StartReplicationAsync("slot", 0x1000000, CancellationToken.None))
                .IsSuccess.Should().BeFalse();
        }

        // Assert — инвариант ревью: закрытий ≥ открытий − 1 (утечки нет —
        // каждый walsender жил ровно до своей замены).
        fake.Opened.Should().Be(5);
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
            return Task.CompletedTask;
        }

        public IAsyncEnumerable<XLogDataMessage> StartReplication(
            PhysicalReplicationSlot slot, NpgsqlLogSequenceNumber startLsn, CancellationToken ct)
            => FailStartReplication
                ? throw new InvalidOperationException("fake: start replication failed")
                : EmptyStream();

        public void SetReplicationStatus(NpgsqlLogSequenceNumber lsn)
        {
        }

        public Task SendStatusUpdate(CancellationToken ct) => Task.CompletedTask;

        public Task<TimelineHistoryFile> TimelineHistory(uint tli, CancellationToken ct)
            => throw new NotSupportedException("fake: history не нужен в этих тестах");

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
