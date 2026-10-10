using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using OwnS3.App.Pipeline;

namespace OwnS3.UnitTests;

// S3ModelFactory (impl-фикс код-ревью): RawTarget — приоритетный источник
// пути/query для подписи; при его отсутствии (TestServer) — откат на
// escaped Path/QueryString (PathString хранит percent-кодированную форму).
public sealed class S3ModelFactoryTests
{
    [Fact]
    public void Create_RawTargetPresent_SplitByFirstQuestionMark()
    {
        // Arrange: фича с сырой request-line (кодированный ключ + query)
        var http = new DefaultHttpContext();
        http.Features.Get<IHttpRequestFeature>()!.RawTarget = "/b/caf%C3%A9?prefix=a%20b&marker=z";
        http.Request.Method = "GET";
        http.Request.Host = new HostString("localhost:9000");

        // Act
        var model = S3ModelFactory.Create(http);

        // Assert: путь/query — из RawTarget как прислано, без декодирования
        model.RawPath.Should().Be("/b/caf%C3%A9");
        model.RawQuery.Should().Be("prefix=a%20b&marker=z");
        model.Method.Should().Be("GET");
        model.Host.Should().Be("localhost:9000");
    }

    [Fact]
    public void Create_RawTargetMissing_FallsBackToPathAndQuery()
    {
        // Arrange: фича без RawTarget (TestServer не заполняет) — фолбэк на
        // Request.Path/QueryString; PathString может нормализовать
        // percent-тройки, канонизация EncodePath выравнивает формы (поэтому
        // кодированные ключи интеграционных кейсов проходят и через фолбэк)
        var http = new DefaultHttpContext();
        http.Features.Get<IHttpRequestFeature>()!.RawTarget = null!;
        http.Request.Method = "GET";
        http.Request.Path = "/b/key";
        http.Request.QueryString = new QueryString("?prefix=x");
        http.Request.Host = new HostString("localhost:9000");

        // Act
        var model = S3ModelFactory.Create(http);

        // Assert: фолбэк отдаёт путь/query запроса
        model.RawPath.Should().Be("/b/key");
        model.RawQuery.Should().Be("prefix=x");
    }
}
