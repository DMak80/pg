namespace S3Worker.UnitTests;

/// <summary>
/// Управляемый TimeProvider для тестов процессов: время двигает тест явно,
/// троттлы/дедупы проверяются без реальных задержек.
/// </summary>
public sealed class FixedTimeProvider : TimeProvider
{
    public DateTimeOffset Utc { get; set; } = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow() => Utc;
}
