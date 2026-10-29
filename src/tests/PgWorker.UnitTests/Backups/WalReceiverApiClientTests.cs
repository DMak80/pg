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

    // AAA: node-операция (arch/25 №22) — 200 несёт тело итога
    [Fact]
    public async Task EnsureHbaAsync_200_ParsesBody()
    {
        // Arrange — приёмник подтвердил hba-строку (arch/19 §7)
        var ct = TestContext.Current.CancellationToken;
        var client = ClientWith(new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("""{"applied":true,"bootstrap":false}"""),
        });

        // Act
        var reply = await client.EnsureHbaAsync("http://node:8080", ct);

        // Assert
        reply.IsSuccess.Should().BeTrue();
        reply.Body!.Applied.Should().BeTrue();
        reply.Body.Bootstrap.Should().BeFalse();
    }

    // AAA: pgdump — тело запроса и текст дампа в ответе 200
    [Fact]
    public async Task PgDumpAsync_200_ParsesDdl()
    {
        // Arrange — приёмник вернул дамп схемы (arch/14 §Move M1)
        var ct = TestContext.Current.CancellationToken;
        var handler = new CapturingHandler(new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("""{"ddl":"-- ddl"}"""),
        });
        var client = new WalReceiverApiClient(new HttpClient(handler));

        // Act
        var reply = await client.PgDumpAsync(
            "http://pgw-shop-shard1-shard1a:8080", new StartPgDumpRequest("bucket_42", "shop"), ct);

        // Assert
        reply.IsSuccess.Should().BeTrue();
        reply.Body!.Ddl.Should().Be("-- ddl");
        handler.CapturedUri.Should().Be("http://pgw-shop-shard1-shard1a:8080/api/node/pgdump");
        handler.CapturedBody.Should().Contain("\"schema\":\"bucket_42\"");
        handler.CapturedBody.Should().Contain("\"dbname\":\"shop\"");
    }

    // AAA: pgdata — 200 present/absent; транспортный отказ — StatusCode 0
    [Fact]
    public async Task GetPgDataAsync_Present_AndTransportFailure()
    {
        // Arrange — приёмник подтвердил факт PG_VERSION
        var ct = TestContext.Current.CancellationToken;
        var client = ClientWith(new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("""{"pg_version":"present"}"""),
        });

        // Act / Assert
        var reply = await client.GetPgDataAsync("http://node:8080", ct);
        reply.IsSuccess.Should().BeTrue();
        reply.Body!.PgVersion.Should().Be("present");

        // Arrange — транспорт недоступен: Unknown для Д3 (утрата не доказана)
        var dead = new WalReceiverApiClient(new HttpClient(new ThrowingHandler()));

        // Act / Assert
        var failed = await dead.GetPgDataAsync("http://node:8080", ct);
        failed.StatusCode.Should().Be(0);
        failed.IsSuccess.Should().BeFalse();
    }

    // 5xx node-операции — IsTransient с причиной приёмника (поле error)
    [Fact]
    public async Task EnsureHbaAsync_500_TransientWithError()
    {
        // Arrange — приёмник не смог обеспечить строку (hba не применилась)
        var ct = TestContext.Current.CancellationToken;
        var client = ClientWith(new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = new StringContent("""{"error":"hba line did not appear in 8s"}"""),
        });

        // Act
        var reply = await client.EnsureHbaAsync("http://node:8080", ct);

        // Assert
        reply.IsTransient.Should().BeTrue();
        reply.Error.Should().Be("hba line did not appear in 8s");
    }

    // Обработчик, отдающий фикс-ответ и запоминающий запрос (URL/тело — в поля).
    private sealed class CapturingHandler(HttpResponseMessage response) : HttpMessageHandler
    {
        public string? CapturedUri { get; private set; }
        public string? CapturedBody { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken ct)
        {
            CapturedUri = request.RequestUri?.ToString();
            CapturedBody = request.Content is null
                ? null
                : await request.Content.ReadAsStringAsync(ct);
            return response;
        }
    }

    // Обработчик транспортного отказа (соединение не установлено).
    private sealed class ThrowingHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken ct)
            => throw new HttpRequestException("connection refused");
    }
}
