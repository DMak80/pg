using KafkaWorker.Provisioning.Kafka;
using KafkaWorker.UnitTests.Provisioning;

namespace KafkaWorker.UnitTests.Kafka;

// Backoff недоступного кластера (t05, spec §3.2): 15 → 60 → 300 с, сброс при
// успехе, чистка исчезнувших; лежащий кластер не долбится каждый тик
// (порт BackoffAfter KafkaProbeLoop t11).
public class KafkaClusterBackoffTests
{
    // AAA: шкала после N-й подряд неудачи.
    [Theory]
    [InlineData(1, 15)]
    [InlineData(2, 60)]
    [InlineData(3, 300)]
    [InlineData(7, 300)]
    public void BackoffAfter_Scale(int failures, int expectedSec)
    {
        // Act + Assert: окно по номеру подряд-неудачи.
        KafkaClusterBackoff.BackoffAfter(failures).Should().Be(TimeSpan.FromSeconds(expectedSec));
    }

    // AAA: окно блокирует IsBlocked, истечение — разблокирует.
    [Fact]
    public void RecordFailure_BlocksUntilWindowExpires()
    {
        // Arrange: трекер на управляемых часах.
        var clock = new FixedTimeProvider();
        var backoff = new KafkaClusterBackoff(clock);

        // Act: первая неудача.
        backoff.RecordFailure("events", "down");

        // Assert: окно активно сейчас, истекает через 15 c.
        backoff.IsBlocked("events").Should().BeTrue();
        clock.Utc += TimeSpan.FromSeconds(15);
        backoff.IsBlocked("events").Should().BeFalse();
    }

    // AAA: рост окна со 2-й неудачи (60 с) и 3-й (300 с).
    [Fact]
    public void RecordFailure_WindowGrows()
    {
        // Arrange: трекер.
        var clock = new FixedTimeProvider();
        var backoff = new KafkaClusterBackoff(clock);

        // Act: лестница подряд-неудач с частичным истечением окон.
        backoff.RecordFailure("events", "down");       // окно 15 c
        clock.Utc += TimeSpan.FromSeconds(15);
        backoff.RecordFailure("events", "down");       // окно 60 c
        clock.Utc += TimeSpan.FromSeconds(59);

        // Assert: 2-я ступень — 60 c (на 59-й секунде блокирует).
        backoff.IsBlocked("events").Should().BeTrue();
        clock.Utc += TimeSpan.FromSeconds(1);
        backoff.IsBlocked("events").Should().BeFalse();

        // Act: третья неудача.
        backoff.RecordFailure("events", "down");       // окно 300 c
        clock.Utc += TimeSpan.FromSeconds(299);

        // Assert: 3-я ступень — 300 c.
        backoff.IsBlocked("events").Should().BeTrue();
    }

    // AAA: успех сбрасывает счётчик (следующая неудача — снова 15 c).
    [Fact]
    public void RecordSuccess_Resets()
    {
        // Arrange: две подряд-неудачи.
        var clock = new FixedTimeProvider();
        var backoff = new KafkaClusterBackoff(clock);
        backoff.RecordFailure("events", "down");
        backoff.RecordFailure("events", "down");
        clock.Utc += TimeSpan.FromSeconds(60);

        // Act: успех, затем новая неудача.
        backoff.RecordSuccess("events");

        // Assert: счётчик сброшен — запись удалена.
        backoff.IsBlocked("events").Should().BeFalse();
        backoff.RecordFailure("events", "down");
        clock.Utc += TimeSpan.FromSeconds(15);
        backoff.IsBlocked("events").Should().BeFalse(); // снова первая ступень
    }

    // AAA: чистка исчезнувших И сброс счётчика: после ForgetMissing новая
    // неудача gone-кластера даёт окно 15 с (не 60/300 — счётчик не пережил
    // исчезновение из снапшота).
    [Fact]
    public void ForgetMissing_RemovesAndResetsGoneClusters()
    {
        // Arrange: gone с двумя неудачами (окно 60 c), events — с одной.
        var clock = new FixedTimeProvider();
        var backoff = new KafkaClusterBackoff(clock);
        backoff.RecordFailure("gone", "down");
        backoff.RecordFailure("gone", "down"); // счётчик 2 — окно 60 c
        backoff.RecordFailure("events", "down");

        // Act: gone исчез из живых — чистка.
        backoff.ForgetMissing(new HashSet<string> { "events" });

        // Assert: gone исчез из живых и «вернулся»: первая неудача заново — 15 c.
        clock.Utc += TimeSpan.FromHours(1);
        backoff.IsBlocked("gone").Should().BeFalse("запись стёрта чисткой");
        backoff.RecordFailure("gone", "down");
        backoff.IsBlocked("gone").Should().BeTrue();
        clock.Utc += TimeSpan.FromSeconds(15);
        backoff.IsBlocked("gone").Should().BeFalse("окно первой ступени, не 60/300 — счётчик сброшен");

        // Assert: events (живой) untouched: окно от своей записи; истекло к этому времени.
        backoff.IsBlocked("events").Should().BeFalse();
    }
}
