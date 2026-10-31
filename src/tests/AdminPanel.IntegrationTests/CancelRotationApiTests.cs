using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// DELETE-отмены живых заявок ротаций kafka/valkey (u17, arch/02 §10.2-18/19/20,
// §11.2-7/8) — HTTP-уровень панели: 204 (успех), 409/404 ProblemDetails воркера
// телом как есть, неканоническое имя → 404 БЕЗ вызова воркера (валидация
// path-интерполяции), оператор уходит воркеру заголовком X-Requested-By
// (аудит отмен — host-лог воркера). Docker не нужен: стаб IWorkerApiGateway.
[Collection("api")]
public class CancelRotationApiTests(AuthWebFactory factory)
{
    private readonly AuthWebFactory _factory = factory;

    [Fact]
    public async Task DeleteKafkaRotation_WithoutCookie_Returns401()
    {
        // Arrange
        _factory.WorkerApi.Reset();
        using var client = _factory.CreateClient();

        // Act
        using var response = await client.DeleteAsync(
            "/api/kafka/clusters/shop/app-password/rotate", TestContext.Current.CancellationToken);

        // Assert: default-deny закрывает DELETE как все /api/*
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        _factory.WorkerApi.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task DeleteKafkaRotation_CanonicalCluster_Returns204WithOperator()
    {
        // Arrange
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(204, null);
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.DeleteAsync(
            "/api/kafka/clusters/shop/app-password/rotate", TestContext.Current.CancellationToken);

        // Assert: 204; прокси DELETE несёт оператора сессии (аудит воркера)
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        _factory.WorkerApi.Calls.Should().ContainSingle().Which.Should().Match<TestWorkerApi.Call>(c =>
            c.Worker == "kafkaworker" && c.Method == HttpMethod.Delete
            && c.Path == "/api/kafka/clusters/shop/app-password/rotate"
            && c.RequestedBy == "admin");
        _factory.EtcdStub.WriteCalls.Should().Be(0); // панель не пишет в etcd
    }

    [Theory]
    [InlineData("app-password/rotate")]
    [InlineData("admin-password/rotate")]
    [InlineData("ca/rotate")]
    public async Task DeleteKafkaRotation_Worker409_MirroredAsIs(string tail)
    {
        // Arrange — воркер отвечает 409 ProblemDetails (начатая ротация)
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(409,
            """{"title":"Rotation rejected","status":409,"detail":"rotation is already executing"}""");
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.DeleteAsync(
            $"/api/kafka/clusters/shop/{tail}", TestContext.Current.CancellationToken);

        // Assert — ProblemDetails воркера телом как есть (UI-контракт §10.2)
        response.StatusCode.Should().Be(HttpStatusCode.Conflict);
        response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        body.Should().Contain("already executing");
    }

    [Fact]
    public async Task DeleteKafkaRotation_Worker404_MirroredAsIs()
    {
        // Arrange — заявки нет: воркер 404
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(404,
            """{"title":"Not found","status":404,"detail":"rotate rotation request for shop not found"}""");
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.DeleteAsync(
            "/api/kafka/clusters/shop/admin-password/rotate", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Theory]
    [InlineData("/api/kafka/clusters/Bad_Name/app-password/rotate")]
    [InlineData("/api/kafka/clusters/9x/ca/rotate")]
    [InlineData("/api/valkey/clusters/Bad_Name/password/rotate")]
    public async Task DeleteRotation_NonCanonicalName_Returns404_NoWorkerCall(string url)
    {
        // Arrange — неканоническое имя отсекается панелью до интерполяции path
        _factory.WorkerApi.Reset();
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.DeleteAsync(url, TestContext.Current.CancellationToken);

        // Assert — 404 ProblemDetails панели; воркер НЕ вызывался
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        body.Should().Contain("canonical name required");
        _factory.WorkerApi.Calls.Should().BeEmpty();
    }

    [Theory]
    [InlineData("password/rotate")]
    [InlineData("ca/rotate")]
    public async Task DeleteValkeyRotation_CanonicalCluster_Returns204WithOperator(string tail)
    {
        // Arrange
        _factory.WorkerApi.Reset();
        _factory.WorkerApi.Respond = _ => new WorkerApiResult(204, null);
        using var client = await ApiTestLogin.LoginAsync(_factory);

        // Act
        using var response = await client.DeleteAsync(
            $"/api/valkey/clusters/shop/{tail}", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        _factory.WorkerApi.Calls.Should().ContainSingle().Which.Should().Match<TestWorkerApi.Call>(c =>
            c.Worker == "valkeyworker" && c.Method == HttpMethod.Delete
            && c.Path == $"/api/valkey/clusters/shop/{tail}"
            && c.RequestedBy == "admin");
        _factory.EtcdStub.WriteCalls.Should().Be(0);
    }
}
