namespace AdminPanel.UnitTests;

// Управляемый TimeProvider для тестов фиксированных окон rate-limiter'а (spec t02 §9.4).
public sealed class FixedTimeProvider : TimeProvider
{
    // Текущее «время»; старт — фиксированная дата, двигается из тестов.
    public DateTimeOffset Utc { get; set; } = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow() => Utc;
}
