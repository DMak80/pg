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
