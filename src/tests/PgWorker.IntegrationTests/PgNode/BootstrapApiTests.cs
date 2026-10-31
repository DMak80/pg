using System.Net.Http.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit;

namespace PgWorker.IntegrationTests.PgNode;

// API-контракт команд канала доставки (spec §3.4) через WebApplicationFactory
// над internal-Program (образец BackupApiOperationTests): POST /api/bootstrap —
// 400 (валидация аргументов/ключа), 200 ok-пинок/Invalid при отсутствии ключа;
// POST /api/bootstrap/apply — 409 (не ready) / 503 (etcd недоступен); GET
// /api/bootstrap — 405 (маршрута нет: статус — etcd, канал только команды).
// hosted-цикл стрима выключен env PGW_WALRECEIVER_NO_STREAM=1; под TestServer
// ConfigureKestrel-листенеры (8080/8081) не bind'ятся — тесты ходят через
// TestServer-клиент. etcd-ключей нет (фейк-контур пуст) — handshake → Invalid.
// AAA-комментарии.
[Collection("pgnode-api")]
public class BootstrapApiTests : IDisposable
{
    private static readonly Dictionary<string, string?> Saved = [];

    private static void Save(string key)
        => Saved.TryAdd(key, Environment.GetEnvironmentVariable(key));

    private static void SetEnv()
    {
        Save("PGW_BACKUP_CLUSTER");
        Save("PGW_BACKUP_SHARD");
        Save("PGW_BACKUP_NODE");
        Save("PGW_ETCD");
        Save("PGW_WALRECEIVER_NO_STREAM");
        Save("PGW_AGENT_CLIENT_CA");
        Save("DOORMAN_CONFIG_TEMPLATE");
        Environment.SetEnvironmentVariable("PGW_BACKUP_CLUSTER", "c1");
        Environment.SetEnvironmentVariable("PGW_BACKUP_SHARD", "s1");
        Environment.SetEnvironmentVariable("PGW_BACKUP_NODE", "n1");
        Environment.SetEnvironmentVariable("PGW_ETCD", "http://etcd-fake:2379");
        Environment.SetEnvironmentVariable("PGW_WALRECEIVER_NO_STREAM", "1");
        Environment.SetEnvironmentVariable("PGW_AGENT_CLIENT_CA", "");
        Environment.SetEnvironmentVariable("DOORMAN_CONFIG_TEMPLATE", "pooler ${PGW_SU_PASSWORD}");
    }

    private readonly WebApplicationFactory<global::PgWorker.PgNode.Program> _factory;
    private readonly HttpClient _client;

    public BootstrapApiTests()
    {
        // Arrange: Program с пустым etcd-контуром (bootstrap-ключа нет —
        // машина не кеширует данные, handshake → Invalid; apply → 503/409).
        SetEnv();
        _factory = new WebApplicationFactory<global::PgWorker.PgNode.Program>();
        _client = _factory.CreateClient();
    }

    // AAA: POST без паролей — 400 (валидация аргументов хендшейка).
    [Fact]
    public async Task Bootstrap_Post_EmptyPassword_Returns400()
    {
        // Act: команда доставки без паролей.
        var response = await _client.PostAsJsonAsync("/api/bootstrap",
            new { su_password = "", standby_password = "" }, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(System.Net.HttpStatusCode.BadRequest);
    }

    // AAA: POST с паролями, но без bootstrap-ключа в etcd — Invalid (400).
    [Fact]
    public async Task Bootstrap_Post_NoEtcdKey_Returns400Or503()
    {
        // Act: команда доставки при пустом контуре.
        var response = await _client.PostAsJsonAsync("/api/bootstrap",
            new { su_password = "su", standby_password = "sb", is_tool_start = false },
            TestContext.Current.CancellationToken);

        // Assert: ключа нет — Invalid (400); etcd-транспортный сбой — 503.
        response.StatusCode.Should().BeOneOf(
            System.Net.HttpStatusCode.BadRequest, System.Net.HttpStatusCode.ServiceUnavailable);
    }

    // AAA: apply без ready-машины — 409 (NotReady) либо 503 (etcd-сбой чтения).
    [Fact]
    public async Task Apply_Post_Returns409Or503()
    {
        // Act: команда применения без ready-ноды.
        var response = await _client.PostAsync("/api/bootstrap/apply", content: null, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().BeOneOf(
            System.Net.HttpStatusCode.Conflict, System.Net.HttpStatusCode.ServiceUnavailable);
    }

    // AAA: GET /api/bootstrap — 405 (маршрута нет: статус — etcd, канал —
    // только команды, spec §3.4/§8.20).
    [Fact]
    public async Task Bootstrap_Get_Returns405()
    {
        // Act
        var response = await _client.GetAsync("/api/bootstrap", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(System.Net.HttpStatusCode.MethodNotAllowed);
    }

    public void Dispose()
    {
        _client.Dispose();
        _factory.Dispose();
        foreach (var (key, value) in Saved)
            Environment.SetEnvironmentVariable(key, value);
        Saved.Clear();
    }
}
