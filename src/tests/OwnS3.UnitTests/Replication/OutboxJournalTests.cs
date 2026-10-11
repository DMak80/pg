using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Replication;
using OwnS3.Replication.Outbox;

namespace OwnS3.UnitTests.Replication;

// Журнал доставки (t35, глава 07 §4): seqNo сквозной/чтение батчами/тримминг
// по watermark приёма/bounded-бюджет с принудительным триммом/ленивое создание.
public sealed class OutboxJournalFixture : IDisposable
{
    public string Dir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        // Teardown: temp-каталог удаляется при любом исходе
        if (Directory.Exists(Dir))
            Directory.Delete(Dir, recursive: true);
    }
}

public class OutboxJournalTests : IClassFixture<OutboxJournalFixture>
{
    private readonly OutboxJournalFixture _fixture = new();

    // Пособытийная ротация: лимит 1 байт — каждый Append после первого ротирует сегмент.
    private static OutboxJournal SmallSegments(string dir, long budget) =>
        new(dir, budget, NullLogger<OutboxJournal>.Instance, maxSegmentBytes: 1, maxSegmentAge: TimeSpan.FromMinutes(5));

    private static ReplicationEvent Append(OutboxJournal journal, long modTime = 1) =>
        journal.Append(ReplicationEventType.ObjectPut, "b", "k", modTime, "etag", 5, "sha", "origin");

    public OutboxJournalTests()
    {
        // Изоляция кейсов: temp-каталог на кейс
        if (Directory.Exists(_fixture.Dir))
            Directory.Delete(_fixture.Dir, recursive: true);
    }

    private string Dir => _fixture.Dir;

    [Fact]
    public void Append_YieldsMonotonicSeqNo_ReadBatchInOrder()
    {
        // Arrange — журнал на temp-каталоге
        using var journal = SmallSegments(Dir, long.MaxValue);

        // Act — три события
        var e1 = Append(journal);
        var e2 = Append(journal);
        var e3 = Append(journal);

        // Assert — seqNo монотонны 1,2,3; чтение с начала в порядке
        e1.SeqNo.Should().Be(1);
        e2.SeqNo.Should().Be(2);
        e3.SeqNo.Should().Be(3);
        var batch = journal.ReadBatch(0, 10);
        batch.Select(e => e.SeqNo).Should().ContainInOrder(1, 2, 3);
        journal.LastSeqNo.Should().Be(3);
    }

    [Fact]
    public void Restart_ContinuesSeqNo_ReadBatchAfterWatermark()
    {
        // Arrange — журнал с тремя событиями,Dispose (имитация рестарта)
        var journal = SmallSegments(Dir, long.MaxValue);
        Append(journal);
        Append(journal);
        Append(journal);
        journal.Dispose();

        // Act — новый экземпляр того же каталога
        using var restarted = SmallSegments(Dir, long.MaxValue);
        var e4 = Append(restarted);

        // Assert — seqNo продолжается (4); чтение от watermark=2 — события >2
        e4.SeqNo.Should().Be(4);
        restarted.ReadBatch(2, 10).Select(e => e.SeqNo).Should().ContainInOrder(3, 4);
        restarted.ReadBatch(4, 10).Should().BeEmpty();
    }

    [Fact]
    public void Trim_RemovesSegmentsBelowWatermark_KeepsTail()
    {
        // Arrange — четыре события в четырёх сегментах (пособытийная ротация)
        using var journal = SmallSegments(Dir, long.MaxValue);
        foreach (var modTime in new[] { 1L, 2L, 3L, 4L })
            Append(journal, modTime);

        // Act — штатный тримминг: min watermark приёма активных соседей = 3
        var removed = journal.Trim(3);

        // Assert — сегменты с концом < 3 удалены (события 1,2 недоступны),
        // хвост жив (ReadBatch(2,...) отдаёт 3,4), активный не тронут
        removed.Should().BeTrue();
        journal.ReadBatch(0, 10).Select(e => e.SeqNo).Should().ContainInOrder(3, 4);
        journal.ReadBatch(2, 10).Select(e => e.SeqNo).Should().ContainInOrder(3, 4);
        journal.LastSeqNo.Should().Be(4);
        journal.TotalBytes.Should().BeGreaterThan(0);
    }

    [Fact]
    public void Trim_NothingBelowWatermark_ReturnsFalse()
    {
        // Arrange — два события
        using var journal = SmallSegments(Dir, long.MaxValue);
        Append(journal);
        Append(journal);

        // Act — watermark ниже минимума
        var removed = journal.Trim(1);

        // Assert — удалять нечего
        removed.Should().BeFalse();
        journal.ReadBatch(0, 10).Should().HaveCount(2);
    }

    [Fact]
    public void Budget_Exceeded_ForceTrimToBudget_AppendStillSucceeds()
    {
        // Arrange — малый бюджет: 6 событий в 6 сегментах его превышают
        var budget = 4 * EstimateEntrySize() + 8;
        using var journal = SmallSegments(Dir, budget);
        for (var i = 0; i < 6; i++)
            Append(journal, i + 1);
        journal.BudgetExceeded.Should().BeTrue();

        // Act — принудительный тримминг
        var trimmed = journal.ForceTrimToBudget();

        // Assert — журнал удержан бюджетом (или остался только активный сегмент),
        // запись клиента продолжает работать (переполнение — алерт, не отказ, R6)
        trimmed.Should().BeTrue();
        var activeStart = journal.LastSeqNo; // активный сегмент содержит последнее событие
        journal.ReadBatch(activeStart - 1, 10).Select(e => e.SeqNo).Should().Contain(activeStart);
        var appended = Append(journal, 99);
        appended.SeqNo.Should().Be(7);
        journal.ReadBatch(6, 10).Select(e => e.SeqNo).Should().ContainInOrder(6, 7);
    }

    [Fact]
    public void LazyDirectory_CreatedOnFirstAppendOnly()
    {
        // Arrange — журнал без событий (неактивная репликация)
        // Act
        using var journal = SmallSegments(Dir, long.MaxValue);

        // Assert — каталог не создан до первого Append (глава 07 §4)
        Directory.Exists(Dir).Should().BeFalse();

        // Act — первый Append
        Append(journal);

        // Assert — журнал создан лениво и живёт
        Directory.Exists(Dir).Should().BeTrue();
        journal.ReadBatch(0, 10).Should().HaveCount(1);
    }

    // Приблизительный размер одной записи (бюджет-кейс: больше 2 записей, меньше 6).
    private static int EstimateEntrySize() =>
        OutboxEntryCodec.Encode(new ReplicationEvent(1, ReplicationEventType.ObjectPut, "b", "k", 1,
            "etag", 5, "sha", "origin")).Length;
}
