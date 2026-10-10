using OwnS3.Storage;

namespace OwnS3.UnitTests.Storage;

// Таблица conditional-исходов канона 02 §1 (P8: кавычки нормализуются;
// мс-обрезка modTime при сравнении с HTTP-датами по секундам).
public class ConditionalEvaluatorTests
{
    private static readonly DateTimeOffset ModTime = new(2026, 1, 1, 12, 0, 0, 500, TimeSpan.Zero); // мс-хвост .500

    private const string Etag = "5d41402abc4b2a76b9719d911017c592";
    private const string EtagQuoted = "\"5d41402abc4b2a76b9719d911017c592\"";

    // — If-Match —

    [Fact]
    public void IfMatch_Match_Proceeds()
    {
        // Arrange / Act / Assert
        ConditionalEvaluator.Evaluate(new ObjectConditions(EtagQuoted, null, null, null), Etag, ModTime, true)
            .Should().Be(ConditionalOutcome.Proceed);
    }

    [Fact]
    public void IfMatch_Mismatch_PreconditionFailed()
    {
        // Arrange / Act / Assert
        ConditionalEvaluator.Evaluate(new ObjectConditions("\"other\"", null, null, null), Etag, ModTime, true)
            .Should().Be(ConditionalOutcome.PreconditionFailed);
    }

    [Fact]
    public void IfMatch_Asterisk_MatchesAny()
    {
        // Arrange / Act / Assert
        ConditionalEvaluator.Evaluate(new ObjectConditions("*", null, null, null), Etag, ModTime, true)
            .Should().Be(ConditionalOutcome.Proceed);
    }

    [Fact]
    public void IfMatch_ListWithMatch_Proceeds()
    {
        // Arrange: список через запятую — совпадение любого элемента
        // Act / Assert
        ConditionalEvaluator.Evaluate(new ObjectConditions($"\"other\", {EtagQuoted}", null, null, null), Etag, ModTime, true)
            .Should().Be(ConditionalOutcome.Proceed);
    }

    [Fact]
    public void IfMatch_PriorityOverIfNoneMatch()
    {
        // Arrange: оба заданы, оба совпали — If-Match решает единолично
        // Act / Assert: исход NotModified от If-None-Match не меняет решение If-Match
        ConditionalEvaluator.Evaluate(new ObjectConditions(EtagQuoted, EtagQuoted, null, null), Etag, ModTime, true)
            .Should().Be(ConditionalOutcome.Proceed);
    }

    // — If-None-Match —

    [Fact]
    public void IfNoneMatch_Match_NotModified()
    {
        // Arrange / Act / Assert
        ConditionalEvaluator.Evaluate(new ObjectConditions(null, EtagQuoted, null, null), Etag, ModTime, true)
            .Should().Be(ConditionalOutcome.NotModified);
    }

    [Fact]
    public void IfNoneMatch_Mismatch_Proceeds()
    {
        // Arrange / Act / Assert
        ConditionalEvaluator.Evaluate(new ObjectConditions(null, "\"other\"", null, null), Etag, ModTime, true)
            .Should().Be(ConditionalOutcome.Proceed);
    }

    // — If-Modified-Since —

    [Fact]
    public void IfModifiedSince_ObjectModifiedAfter_Proceeds()
    {
        // Arrange: объект менялся ПОСЛЕ даты клиента
        var since = ModTime.AddSeconds(-10);

        // Act / Assert
        ConditionalEvaluator.Evaluate(new ObjectConditions(null, null, since, null), Etag, ModTime, true)
            .Should().Be(ConditionalOutcome.Proceed);
    }

    [Fact]
    public void IfModifiedSince_NotModifiedSince_NotModified()
    {
        // Arrange: клиент видел объект (дата клиента = обрезанное modTime)
        var since = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

        // Act / Assert: modTime 12:00:00.500 обрезается до секунд — равно If-Modified-Since
        ConditionalEvaluator.Evaluate(new ObjectConditions(null, null, since, null), Etag, ModTime, true)
            .Should().Be(ConditionalOutcome.NotModified);
    }

    [Fact]
    public void IfModifiedSince_IgnoredWhenNotApplicable()
    {
        // Arrange: copy-семейства — If-Modified-Since не применяется
        var since = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

        // Act / Assert
        ConditionalEvaluator.Evaluate(new ObjectConditions(null, null, since, null), Etag, ModTime, false)
            .Should().Be(ConditionalOutcome.Proceed);
    }

    // — If-Unmodified-Since —

    [Fact]
    public void IfUnmodifiedSince_ObjectModifiedAfter_PreconditionFailed()
    {
        // Arrange: объект менялся после границы клиента
        var since = ModTime.AddSeconds(-10);

        // Act / Assert
        ConditionalEvaluator.Evaluate(new ObjectConditions(null, null, null, since), Etag, ModTime, true)
            .Should().Be(ConditionalOutcome.PreconditionFailed);
    }

    [Fact]
    public void NoConditions_Proceeds()
    {
        // Arrange / Act / Assert
        ConditionalEvaluator.Evaluate(null, Etag, ModTime, true).Should().Be(ConditionalOutcome.Proceed);
        ConditionalEvaluator.Evaluate(new ObjectConditions(null, null, null, null), Etag, ModTime, true)
            .Should().Be(ConditionalOutcome.Proceed);
    }

    // — ETagMatches —

    [Theory]
    [InlineData("\"5d41402abc4b2a76b9719d911017c592\"", true)]   // кавычки снимаются
    [InlineData("5d41402abc4b2a76b9719d911017c592", true)]        // и без кавычек
    [InlineData("\"other\"", false)]
    [InlineData("*", true)]                                        // «*» — любой
    [InlineData("\"a\", \"5d41402abc4b2a76b9719d911017c592\"", true)] // список
    [InlineData("\"a\", \"b\"", false)]
    public void ETagMatches_QuotesAndLists(string headerValue, bool expected)
    {
        // Arrange / Act / Assert: сравнение Ordinal с нормализацией кавычек
        ConditionalEvaluator.ETagMatches(headerValue, Etag).Should().Be(expected);
    }
}
