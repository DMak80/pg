using System.Net;
using PgWorker.Core.Templates;
using PgWorker.Provisioning.Probes;
using Xunit;

// AAA (П8.4): контракт NodeChannelClient — тела команд (su_password/
// standby_password/is_tool_start), разбор ответов, transient-коды; контракт
// PgNodeStatusFact — Parse/StatusKey.
public class NodeChannelClientTests
{
    // Fake-обработчик: захват запроса + заготовленный ответ.
    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
    {
        public HttpRequestMessage? LastRequest;
        public string? LastBody;

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken ct)
        {
            LastRequest = request;
            if (request.Content is not null)
                LastBody = await request.Content.ReadAsStringAsync(ct);
            return respond(request);
        }
    }

    private static NodeChannelClient Client(FakeHandler handler)
        => new(new HttpClient(handler) { BaseAddress = new Uri("https://agent:8081") });

    [Fact]
    public async Task Handshake_SendsBodyWithPasswords_ParsesOk()
    {
        // Arrange: обработчик возвращает 200 {"ok":true}.
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("""{"ok":true}"""),
        });
        var client = Client(handler);

        // Act: команда доставки.
        var reply = await client.HandshakeAsync("https://agent:8081", "su", "sb",
            isToolStart: false, CancellationToken.None);

        // Assert: тело несёт пары (JSON-поля контракта), ответ Ok без ревизии.
        reply.IsSuccess.Should().BeTrue();
        reply.AppliedRevision.Should().BeNull();
        handler.LastBody.Should().NotBeNull();
        handler.LastBody.Should().Contain("\"su_password\":\"su\"");
        handler.LastBody.Should().Contain("\"standby_password\":\"sb\"");
        handler.LastBody.Should().Contain("\"is_tool_start\":false");
        handler.LastRequest!.RequestUri!.PathAndQuery.Should().Be("/api/bootstrap");
    }

    [Fact]
    public async Task Apply_ParsesAppliedRevision()
    {
        // Arrange: обработчик возвращает 200 с applied_revision.
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("""{"applied_revision":42}"""),
        });
        var client = Client(handler);

        // Act: команда применения.
        var reply = await client.ApplyAsync("https://agent:8081", CancellationToken.None);

        // Assert: applied_revision разобран.
        reply.IsSuccess.Should().BeTrue();
        reply.AppliedRevision.Should().Be(42);
        handler.LastRequest!.RequestUri!.PathAndQuery.Should().Be("/api/bootstrap/apply");
    }

    [Theory]
    [InlineData(HttpStatusCode.BadRequest)]
    [InlineData(HttpStatusCode.Conflict)]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    [InlineData(HttpStatusCode.InternalServerError)]
    public async Task Non2xx_IsTransientNotSuccess(HttpStatusCode status)
    {
        // Arrange: обработчик возвращает не-2xx.
        var handler = new FakeHandler(_ => new HttpResponseMessage(status));
        var client = Client(handler);

        // Act: команда доставки.
        var reply = await client.HandshakeAsync("https://agent:8081", "su", "sb", false, CancellationToken.None);

        // Assert: transient (воркер ретраит тиком), не успех.
        reply.IsTransient.Should().BeTrue();
        reply.IsSuccess.Should().BeFalse();
        reply.StatusCode.Should().Be((int)status);
    }

    [Fact]
    public async Task TransportFailure_StatusCodeZero()
    {
        // Arrange: обработчик бросает (транспорт).
        var handler = new FakeHandler(_ => throw new HttpRequestException("connection refused (fake)"));
        var client = Client(handler);

        // Act: команда применения.
        var reply = await client.ApplyAsync("https://agent:8081", CancellationToken.None);

        // Assert: StatusCode 0 — транспорт, transient.
        reply.StatusCode.Should().Be(0);
        reply.IsTransient.Should().BeTrue();
        reply.Error.Should().Contain("connection refused");
    }

    [Fact]
    public async Task Handshake_DoesNotPutPasswordsInUrl()
    {
        // Arrange/Act: команда доставки.
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("""{"ok":true}"""),
        });
        var client = Client(handler);
        await client.HandshakeAsync("https://agent:8081", "secret-su", "secret-sb", false, CancellationToken.None);

        // Assert: пароли в URL не попадают (только тело TLS-канала).
        handler.LastRequest!.RequestUri!.Query.Should().NotContain("secret");
    }
}

// AAA: PgNodeStatusFact — Parse полного JSON / битого / StatusKey-формат.
public class PgNodeStatusFactTests
{
    [Fact]
    public void Parse_FullJson_AllFields()
    {
        // Arrange: JSON статусного ключа (контракт агента).
        const string json = """{"state":"ready","applied_revision":9,"components":{"pg":"ready","doorman":"ready"},"updated_unix":7}""";

        // Act
        var fact = PgNodeStatusFact.Parse(json);

        // Assert
        fact.Should().NotBeNull();
        fact!.State.Should().Be("ready");
        fact.AppliedRevision.Should().Be(9);
        fact.Components!["pg"].Should().Be("ready");
        fact.UpdatedUnix.Should().Be(7);
    }

    [Fact]
    public void Parse_MalformedJson_ReturnsNull()
    {
        // Arrange/Act/Assert: битый JSON = статуса нет.
        PgNodeStatusFact.Parse("no json {").Should().BeNull();
    }

    [Fact]
    public void StatusKey_HasCanonicalFormat()
    {
        // Arrange/Act/Assert: /pgworker/nodes/<C>/<X>/<N>/status.
        PgNodeStatusFact.StatusKey("c1", "s1", "n1")
            .Should().Be("/pgworker/nodes/c1/s1/n1/status");
    }
}
