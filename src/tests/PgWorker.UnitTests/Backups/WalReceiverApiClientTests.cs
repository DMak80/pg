using System.Net;
using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Парсинг ответов клиента API приёмника (WalReceiverApiClient): тело 409 —
// снимок АКТИВНОЙ операции с её id (корреляция «своя/чужая» в 409-ветке
// запуска воркера — фантомный COMPLETED чужого снимка исключён); 404 —
// IsNotFound без статуса; транспортный отказ — StatusCode 0.
public class WalReceiverApiClientTests
{
    // Стаб-обработчик: фиксированный ответ на любой запрос.
    private sealed class StubHandler(HttpResponseMessage response) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(response);
    }

    private static WalReceiverApiClient ClientWith(HttpResponseMessage response)
        => new(new HttpClient(new StubHandler(response)));

    // AAA: 409 с телом-снимком (id активной операции) — IsConflict + id распарсен
    [Fact]
    public async Task Conflict409_WithBody_ParsesActiveOperationId()
    {
        // Arrange — приёмник отвечает 409 со снимком активной (чужой) операции
        var ct = TestContext.Current.CancellationToken;
        var client = ClientWith(new HttpResponseMessage(HttpStatusCode.Conflict)
        {
            Content = new StringContent(
                """{"state":"running","phase":"basebackup","id":"20250101000000Z","error":"operation already active"}"""),
        });

        // Act
        var reply = await client.StartFullAsync("http://node:8080", "20260908030000Z", ct);

        // Assert
        reply.IsConflict.Should().BeTrue();
        reply.Status.Should().NotBeNull();
        reply.Status!.Id.Should().Be("20250101000000Z", "the 409 body carries the active operation id");
        reply.Status.Error.Should().Contain("already active");
    }

    // AAA: 409 без тела (приёмник без корреляции) — IsConflict, статус null:
    // воркер трактует как «как раньше» (своя операция)
    [Fact]
    public async Task Conflict409_WithoutBody_NoStatus()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        var client = ClientWith(new HttpResponseMessage(HttpStatusCode.Conflict));

        // Act
        var reply = await client.GetFullAsync("http://node:8080", ct);

        // Assert
        reply.IsConflict.Should().BeTrue();
        reply.Status.Should().BeNull("no correlation id — the legacy path applies");
    }

    // AAA: 404 — IsNotFound, статуса нет
    [Fact]
    public async Task NotFound404_NoStatus()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        var client = ClientWith(new HttpResponseMessage(HttpStatusCode.NotFound));

        // Act
        var reply = await client.GetFullAsync("http://node:8080", ct);

        // Assert
        reply.IsNotFound.Should().BeTrue();
        reply.IsConflict.Should().BeFalse();
        reply.Status.Should().BeNull();
    }
}
