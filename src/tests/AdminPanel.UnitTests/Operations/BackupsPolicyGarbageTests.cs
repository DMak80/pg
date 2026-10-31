using System.Text.Json;
using AdminPanel.Api.Operations;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests.Operations;

// u16 (arch/02 §9.12): команда policy с GarbageMinAgeSec=600 → тело POST
// воркеру содержит ключ garbage_min_age_sec; null → ключа нет (замещение
// целиком снимает пер-кластерный гейт — кластер живёт на дефолте воркера
// 600 с). Проверка через публичный шов: стаб шлюза захватывает body команды
// (BuildBody internal — наружу не отдаётся). AAA.
public class BackupsPolicyGarbageTests
{
    // Стаб шлюза (образец WorkerProxyCommandTests.StubWorkerApi).
    private sealed class StubWorkerApi : IWorkerApiGateway
    {
        public object? LastBody { get; private set; }

        public Task<WorkerApiResult> SendAsync(
            string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)
        {
            LastBody = body;
            return Task.FromResult(new WorkerApiResult(200, "\"{}\"")); // Results.Ok(string) воркера — кавыченный JSON
        }

        public Task<IReadOnlyList<WorkerApiInstanceResult>> SendAllAsync(
            string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)
            => throw new NotSupportedException("broadcast не участвует в policy-прокси");
    }

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    [Fact]
    public async Task Handle_Garbage600_BodyContainsKey()
    {
        // Arrange
        var api = new StubWorkerApi();
        var handler = new UpdateBackupsPolicyCommandHandler(api);
        var command = new UpdateBackupsPolicyCommand("demo", 7, 4, 6, 86400, true, 1,
            S3: null, RequestedBy: "ops", GarbageMinAgeSec: 600);

        // Act
        var result = await handler.Handle(command, CancellationToken.None);
        var json = JsonSerializer.Serialize(api.LastBody, Json);

        // Assert — ключ канона policy в теле воркера
        result.IsSuccess.Should().BeTrue();
        json.Should().Contain("\"garbage_min_age_sec\":600");
    }

    [Fact]
    public async Task Handle_GarbageNull_KeyAbsent()
    {
        // Arrange — поле не задано формой (панель шлёт policy БЕЗ ключа)
        var api = new StubWorkerApi();
        var handler = new UpdateBackupsPolicyCommandHandler(api);
        var command = new UpdateBackupsPolicyCommand("demo", 7, 4, 6, 86400, true, 1,
            S3: null, RequestedBy: "ops");

        // Act
        var result = await handler.Handle(command, CancellationToken.None);
        var json = JsonSerializer.Serialize(api.LastBody, Json);

        // Assert
        result.IsSuccess.Should().BeTrue();
        json.Should().NotContain("garbage_min_age_sec");
    }
}

// Панельная валидация policy-тела (400-ветка модуля, u16/arch/02 §9.12):
// границы гварда панели — дубль для UX (истина — воркер). Имя кластера —
// канон; garbageMinAgeSec < 600 — 400 (A1: граница канона arch/19 §4). AAA.
public class PolicyBodyValidationTests
{
    private static UpdateBackupsPolicyRequest ValidBody(long? garbage = 600)
        => new(7, 4, 6, 86400, true, 1, S3: null, GarbageMinAgeSec: garbage);

    [Theory]
    [InlineData(600L)]
    [InlineData(601L)]
    [InlineData(null)]
    public void GarbageMinAgeSec_AtOrAboveMin_BodyValid(long? garbage)
    {
        // Arrange/Act — граница включена (600 — дефолт канона)
        var invalid = PolicyBodyValidation.IsInvalid(ValidBody(garbage));

        // Assert
        invalid.Should().BeFalse();
    }

    [Theory]
    [InlineData(599L)]
    [InlineData(0L)]
    [InlineData(-1L)]
    public void GarbageMinAgeSec_BelowMin_BodyInvalid(long? garbage)
    {
        // Arrange/Act
        var invalid = PolicyBodyValidation.IsInvalid(ValidBody(garbage));

        // Assert
        invalid.Should().BeTrue();
    }

    [Fact]
    public void RetentionDaysZero_BodyInvalid()
    {
        // Arrange — граница retention (первичное поле политики)
        var body = ValidBody() with { RetentionDays = 0 };

        // Act/Assert
        PolicyBodyValidation.IsInvalid(body).Should().BeTrue();
    }

    [Fact]
    public void FullMaxAgeSec599_BodyInvalid()
    {
        // Arrange — соседняя граница сек-полей (единый порог 600)
        var body = ValidBody() with { FullMaxAgeSec = 599 };

        // Act/Assert
        PolicyBodyValidation.IsInvalid(body).Should().BeTrue();
    }

    [Fact]
    public void DrillIntervalDaysOutOfRange_BodyInvalid()
    {
        // Arrange — граница дрилов [0..3650]
        var body = ValidBody() with { DrillIntervalDays = 3651 };

        // Act/Assert
        PolicyBodyValidation.IsInvalid(body).Should().BeTrue();
    }

    [Fact]
    public void Hint_MentionsGarbageBoundary()
    {
        // Arrange/Act/Assert — подсказка формы называет границу garbage
        PolicyBodyValidation.Hint.Should().Contain("garbageMinAgeSec ≥ 600");
    }
}
