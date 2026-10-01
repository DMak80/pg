using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Workers;
using AdminPanel.Probes.S3;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Shared.Core.DI;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Panel policy-API бэкапов (reliability t02, 02 §9.12): GET деталей шарда
// несёт drill-бейдж, GET грани — policy кластера; PUT policy — команда-прокси
// в POST policy-API воркера ПОЛНЫМ телом (панель в etcd не пишет).
[Collection("backups")]
public class BackupsPolicyPanelApiTests : IClassFixture<EtcdContainerFixture>,
    IClassFixture<MinioContainerFixture>, IAsyncDisposable
{
    private readonly TestWorkerApi _workerApi = new();

    private readonly BackupsWebFactory _factory;

    private readonly string _etcdEndpoint;

    private readonly MinioContainerFixture _minio;

    public BackupsPolicyPanelApiTests(EtcdContainerFixture etcd, MinioContainerFixture minio)
    {
        _etcdEndpoint = etcd.Endpoint;
        _minio = minio;
        _factory = new BackupsWebFactory
        {
            EtcdEndpoint = etcd.Endpoint,
            MinioEndpoint = minio.HostEndpoint,
            MinioBucket = minio.Bucket,
            WorkerApi = _workerApi,
        };
    }

    private TestWorkerApi WorkerApi => _workerApi;

    // AAA: GET деталей шарда отдаёт drill-бейдж из ключа <X>/drill.
    [Fact]
    public async Task ShardDetails_ReturnsDrillBadge()
    {
        _factory.EnsureBuilt();
        // Arrange — сид бэкапов demo/s1 + ключ дрилла; снапшот — ручным тиком.
        var ct = TestContext.Current.CancellationToken;
        await EtcdSeed.SeedBackupsAsync(_etcdEndpoint, ct);
        await EtcdSeed.PutAsync(_etcdEndpoint, "/pgworker/backups/demo/s1/drill",
            """{"state":"SUCCEEDED","id":"20261001120000Z","backup_id":"20261001090000Z","started_unix":100,"finished_unix":400,"restored_to_lsn":"0/42"}""",
            ct);
        await _factory.Services.GetRequiredService<SnapshotRefresher>()
            .RefreshOnceAsync(ct);
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act
        using var response = await client.GetAsync(
            "/api/backups/storage/demo/s1", ct);

        // Assert — бейдж с state/фазой/LSN/временами.
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(ct);
        var drill = dto.GetProperty("drill");
        drill.GetProperty("state").GetString().Should().Be("SUCCEEDED");
        drill.GetProperty("restoredToLsn").GetString().Should().Be("0/42");
        drill.GetProperty("startedUnix").GetInt64().Should().Be(100);
        drill.GetProperty("finishedUnix").GetInt64().Should().Be(400);
        drill.GetProperty("phase").ValueKind.Should().Be(JsonValueKind.Null);
    }

    // AAA: GET грани отдаёт policy кластера (retention/verify/drill) из policy-ключа.
    [Fact]
    public async Task StorageCluster_ReturnsPolicy()
    {
        _factory.EnsureBuilt();
        // Arrange — объект S3 (кластер в дереве инвентаря) + полная policy demo.
        var ct = TestContext.Current.CancellationToken;
        await EtcdSeed.PutAsync(_etcdEndpoint, "/pgworker/backups/demo/policy",
            """{"retention":{"days":7,"weeks":4,"months":6},"full_max_age_sec":86400,"verify":{"on_create":true},"drill":{"interval_days":2}}""",
            ct);
        // Объект demo/s1 в S3 — кластер появляется в дереве инвентаря.
        await _minio.SeedObjectsAsync([("demo/s1/full/b1/base.tar", new string('a', 64))], ct);
        var minio = _factory.Services.GetRequiredService<MinioInventoryLoop>();
        await minio.RunOnceAsync(ct);
        (await _factory.Services.GetRequiredService<SnapshotRefresher>().RefreshOnceAsync(ct))
            .IsSuccess.Should().BeTrue();
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act
        using var response = await client.GetAsync("/api/backups/storage", ct);

        // Assert — policy в строке кластера demo; drill-поле на месте.
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var dto = await response.Content.ReadFromJsonAsync<JsonElement>(ct);
        var demo = dto.GetProperty("clusters").EnumerateArray()
            .Single(c => c.GetProperty("cluster").GetString() == "demo");
        var policy = demo.GetProperty("policy");
        policy.GetProperty("retentionDays").GetInt32().Should().Be(7);
        policy.GetProperty("fullMaxAgeSec").GetInt64().Should().Be(86400);
        policy.GetProperty("verifyOnCreate").GetBoolean().Should().BeTrue();
        policy.GetProperty("drillIntervalDays").GetInt32().Should().Be(2);
    }

    // AAA: PUT policy — панель проксирует в API воркера ПОЛНОЕ тело (drill в
    // теле), 200.
    [Fact]
    public async Task UpdatePolicy_ProxiesFullBodyToWorker()
    {
        _factory.EnsureBuilt();
        // Arrange — дефолтный 204-стаб воркера.
        var ct = TestContext.Current.CancellationToken;
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PutAsJsonAsync(
            "/api/clusters/demo/backups/policy", new
            {
                retentionDays = 7,
                retentionWeeks = 4,
                retentionMonths = 6,
                fullMaxAgeSec = 86400L,
                verifyOnCreate = true,
                drillIntervalDays = 3,
            }, ct);

        // Assert — 200; захват: POST policy-API воркера с секциями
        // retention + full_max_age_sec + verify + drill.
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var call = WorkerApi.Calls.Should().ContainSingle().Subject;
        call.Worker.Should().Be("pgworker");
        call.Method.Should().Be(HttpMethod.Post);
        call.Path.Should().Be("/api/clusters/demo/backups/policy");
        var body = JsonSerializer.Serialize(call.Body,
            new JsonSerializerOptions(JsonSerializerDefaults.Web));
        body.Should().Contain("\"drill\":{\"interval_days\":3}")
            .And.Contain("\"retention\":{\"days\":7,\"weeks\":4,\"months\":6}")
            .And.Contain("\"full_max_age_sec\":86400")
            .And.Contain("\"verify\":{\"on_create\":true}");
    }

    // AAA: ошибка валидации воркера (400) пробрасывается панелью.
    [Fact]
    public async Task UpdatePolicy_Worker400_Mapped()
    {
        _factory.EnsureBuilt();
        // Arrange — воркер отвечает 400 ProblemDetails.
        var ct = TestContext.Current.CancellationToken;
        WorkerApi.Respond = _ => new WorkerApiResult(400,
            """{"title":"Validation failed","errors":{"drill.interval_days":["период дрилов — целое в [0..3650]"]}}""");
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PutAsJsonAsync(
            "/api/clusters/demo/backups/policy", new
            {
                retentionDays = 7,
                retentionWeeks = 4,
                retentionMonths = 6,
                fullMaxAgeSec = 86400L,
                verifyOnCreate = true,
                drillIntervalDays = 3,
            }, ct);

        // Assert — 400 и тело воркера как есть (проксирование без изменений).
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        var body = await response.Content.ReadAsStringAsync(ct);
        body.Should().Contain("drill.interval_days");
    }

    // AAA: живых инстансов воркера нет → 503 (образец прочих мутаций).
    [Fact]
    public async Task UpdatePolicy_NoWorkers_503()
    {
        _factory.EnsureBuilt();
        // Arrange — гейтвей кидает WorkerApiUnavailableException.
        var ct = TestContext.Current.CancellationToken;
        WorkerApi.Throw = new WorkerApiUnavailableException("нет живых инстансов pgworker");
        using var client = await BackupsLogin.LoginAsync(_factory);

        // Act
        using var response = await client.PutAsJsonAsync(
            "/api/clusters/demo/backups/policy", new
            {
                retentionDays = 7,
                retentionWeeks = 4,
                retentionMonths = 6,
                fullMaxAgeSec = 86400L,
                verifyOnCreate = true,
                drillIntervalDays = 1,
            }, ct);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    public async ValueTask DisposeAsync() => await _factory.DisposeAsync();
}
